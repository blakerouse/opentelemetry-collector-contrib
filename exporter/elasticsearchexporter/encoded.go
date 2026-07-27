// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/pool"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
)

// sessionTarget selects which bulk indexer a bulk item is written to. Logs,
// traces and metrics always use targetDefault (the mapping-mode indexer);
// profiles fan out across the dedicated profiling indexers as well.
type sessionTarget uint8

// The numeric values are persisted in the early-encoded persistent-queue wire
// format (see marshalEncodedRequest): append new targets before
// numSessionTargets only, and never reorder or remove existing ones, or items
// drained from disk after an upgrade will be routed to the wrong indexer.
const (
	targetDefault sessionTarget = iota
	targetProfilingEvents
	targetProfilingStackTraces
	targetProfilingStackFrames
	targetProfilingExecutables
	numSessionTargets
)

// itemKind distinguishes how an encodedItem's doc bytes are consumed.
//
// The numeric values are persisted in the early-encoded persistent-queue wire
// format (see marshalEncodedRequest): append new kinds before numItemKinds
// only, and never reorder or remove existing ones.
type itemKind uint8

const (
	// itemKindDoc is a final document sent as-is (logs, traces, profiles, and
	// unmergeable metric docs).
	itemKindDoc itemKind = iota
	// itemKindMergeableMetrics is a final OTel-mode metrics document that can
	// be merged with same-group documents by byte splicing at consume time.
	itemKindMergeableMetrics
	// itemKindDeferredMetrics is a proto-marshaled pmetric.Metrics holding
	// ECS-mode scopes; grouped and encoded at consume time over the whole
	// batch, like the legacy pdata path.
	itemKindDeferredMetrics
	numItemKinds
)

// encodedItem is a single record that has already been serialized to its final
// bulk item form (index routing, document id, pipeline, dynamic templates,
// action, target indexer and the encoded document bytes). Serializing at ingest
// time (in the request converter, which runs on the ConsumeX caller goroutines)
// moves the per-record JSON encoding off the sending-queue consumer's critical
// path: the consumer only has to assemble these pre-encoded bytes into a bulk
// request and send it. The shape is signal-agnostic: it matches the arguments of
// every signal's bulkIndexerSession.Add call.
type encodedItem struct {
	index            string
	docID            string
	pipeline         string
	action           string
	dynamicTemplates map[string]string
	mappingMode      MappingMode
	target           sessionTarget
	kind             itemKind
	// doc is the final serialized bulk-item body (or, for
	// itemKindDeferredMetrics, the proto-marshaled pdata). It is populated only
	// on the early/ingest path (by itemSink, from the separate encodedDoc
	// handed to docSink.add); the streaming/legacy path never sets it.
	doc []byte

	// Merge metadata, set only for itemKindMergeableMetrics: the data point
	// group identity, the offsets of the inner "metrics" object fields within
	// doc, the sorted metric names, and the _doc_count. Same-group documents
	// are merged at consume time; see consumeEncodedItems.
	groupKey    metricgroup.HashKey
	fragStart   int
	fragEnd     int
	metricNames []string
	docCount    uint64

	// deferredMetrics, set only for itemKindDeferredMetrics created at ingest,
	// holds the original payload by reference — no copy, no serialization (the
	// legacy pdata path's memory profile). doc stays nil until the item is
	// persisted (see marshalEncodedRequest); items read back from disk have doc
	// set instead. For deferred items, mappingMode records the request-default
	// mapping mode so the consumer can re-resolve scope modes deterministically,
	// and deferredSize is the deferred data's proto size for the byte sizers.
	// deferredScopes, set only for a mixed-mode payload, lists the scopes that
	// were actually deferred (nil = all of them): sizing and persistence are
	// restricted to those, since the payload's other scopes already exist as
	// encoded doc items and must not be counted or stored twice. It is never
	// serialized — persisted payloads are pre-filtered.
	deferredMetrics pmetric.Metrics
	deferredScopes  []scopeRef
	deferredSize    int
}

// size returns the item's contribution to request byte sizing: the encoded doc
// length, or the payload proto size for a not-yet-marshaled deferred item.
func (it *encodedItem) size() int {
	if it.doc == nil {
		return it.deferredSize
	}
	return len(it.doc)
}

// encodedRequest is an exporterhelper request.Request carrying a slice of
// already-encoded bulk items. Merging is a slice append; splitting cuts the
// slice while respecting the configured sizer.
type encodedRequest struct {
	items     []encodedItem
	bytesSize int
}

var _ xexporterhelper.Request = (*encodedRequest)(nil)

func newEncodedRequest(items []encodedItem) *encodedRequest {
	var bytesSize int
	for i := range items {
		bytesSize += items[i].size()
	}
	return &encodedRequest{items: items, bytesSize: bytesSize}
}

// ItemsCount returns the number of bulk items (records) in the request.
func (r *encodedRequest) ItemsCount() int { return len(r.items) }

// BytesSize returns the total size of the encoded documents in bytes. This is
// used by the batcher/queue when configured with the bytes sizer.
func (r *encodedRequest) BytesSize() int { return r.bytesSize }

// MergeSplit merges r with req (if non-nil) and splits the result so each
// returned Request fits maxSize according to the given sizer type. Per the
// interface contract, the last returned Request is guaranteed to be the
// smallest.
func (r *encodedRequest) MergeSplit(
	ctx context.Context,
	maxSize int,
	sizerType exporterhelper.RequestSizerType,
	req xexporterhelper.Request,
) ([]xexporterhelper.Request, error) {
	merged := r.items
	if req != nil {
		other, ok := req.(*encodedRequest)
		if !ok {
			// A pdataRequest can reach the batcher unconverted when the
			// persistent queue's marshal round-trip is bypassed; convert it here.
			conv, isEncodable := req.(encodableRequest)
			if !isEncodable {
				return nil, errors.New("elasticsearchexporter: MergeSplit got incompatible Request type")
			}
			var err error
			if other, err = conv.toEncoded(ctx); err != nil {
				return nil, err
			}
		}
		merged = append(merged, other.items...)
	}

	if maxSize == 0 || len(merged) == 0 {
		return []xexporterhelper.Request{newEncodedRequest(merged)}, nil
	}

	switch sizerType {
	case exporterhelper.RequestSizerTypeBytes:
		return splitByBytes(merged, maxSize), nil
	case exporterhelper.RequestSizerTypeItems:
		return splitByCount(merged, maxSize), nil
	default:
		return nil, fmt.Errorf("elasticsearchexporter: unsupported sizer type %q", sizerType.String())
	}
}

// splitByBytes places items into bins of at most maxSize bytes (measured by
// encoded document size). Items that individually equal or exceed maxSize are
// emitted as their own single-item Request, deliberately exceeding maxSize:
// the upstream pdata requests drop such records with an error instead, but the
// batcher flushes oversize results immediately (never retaining them), and the
// bulk session's maxFlushBytes force-flush bounds the actual request body — so
// sending the record is strictly friendlier than dropping it. Pinned by
// TestSplitByBytes_OversizedItemSentNotDropped. Only the last Request is
// guaranteed to be the smallest, so we track the running minimum and swap it
// to the end once rather than sorting.
func splitByBytes(items []encodedItem, maxSize int) []xexporterhelper.Request {
	var out []xexporterhelper.Request
	var curSize, minIdx, minSize int

	emit := func(slice []encodedItem) {
		// Clamp cap to len (three-index slice). The sub-requests returned here
		// alias one backing array; the batcher keeps one as its pending batch and
		// flushes the others on worker goroutines. A later MergeSplit appends into
		// the retained request's items, so without cap==len that append would write
		// into a sibling's region of the shared array — corrupting a request that is
		// (or is about to be) read by a flush goroutine. cap==len forces the append
		// to reallocate instead.
		req := newEncodedRequest(slice[:len(slice):len(slice)])
		out = append(out, req)
		if size := req.BytesSize(); len(out) == 1 || size < minSize {
			minIdx = len(out) - 1
			minSize = size
		}
	}

	start := 0
	for i := range items {
		size := items[i].size()
		if size >= maxSize {
			if i > start {
				emit(items[start:i])
			}
			emit(items[i : i+1])
			start = i + 1
			curSize = 0
			continue
		}
		if curSize+size > maxSize && i > start {
			emit(items[start:i])
			start = i
			curSize = 0
		}
		curSize += size
	}
	if start < len(items) {
		emit(items[start:])
	}

	if len(out) > 1 && minIdx != len(out)-1 {
		out[minIdx], out[len(out)-1] = out[len(out)-1], out[minIdx]
	}
	return out
}

func splitByCount(items []encodedItem, maxCount int) []xexporterhelper.Request {
	if maxCount <= 0 {
		return []xexporterhelper.Request{newEncodedRequest(items)}
	}
	out := make([]xexporterhelper.Request, 0, (len(items)+maxCount-1)/maxCount)
	for i := 0; i < len(items); i += maxCount {
		end := min(i+maxCount, len(items))
		// Clamp cap to len (three-index slice) so a later MergeSplit append on a
		// retained sub-request reallocates instead of overwriting a sibling that
		// shares this backing array; see splitByBytes.
		out = append(out, newEncodedRequest(items[i:end:end]))
	}
	return out
}

// recordEncoder serializes one pdata payload (plog.Logs, pmetric.Metrics, ...)
// into encoded bulk items at ingest time. perRecordErrs are deterministic
// per-record encoding errors: the legacy push path returns them to the caller
// after flushing, while the converter path drops only the failed records
// (logging and counting them) and sends the rest — unless every record failed,
// in which case the conversion fails so a total drop is not reported as
// success. A non-nil err aborts the whole batch (e.g. mapping-mode resolution
// failure).
type recordEncoder[T any] func(ctx context.Context, data T) (items []encodedItem, perRecordErrs []error, err error)

// convertToEncoded encodes data into an *encodedRequest, dropping (and logging
// and counting) records that fail to encode. A non-nil error means the whole
// payload failed (e.g. mapping-mode resolution); any consumererror permanent
// wrapper is preserved.
func convertToEncoded[T any](ctx context.Context, e *elasticsearchExporter, encode recordEncoder[T], data T) (*encodedRequest, error) {
	items, perRecordErrs, err := encode(ctx, data)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		return nil, err
	}
	if len(perRecordErrs) > 0 {
		// Every record failed deterministically (e.g. the resolved mapping
		// mode does not support this signal at all). There is nothing to send,
		// and returning success would hide a total drop from the pipeline;
		// fail the conversion so the caller sees the error, as the legacy
		// push path did.
		if len(items) == 0 {
			return nil, errors.Join(perRecordErrs...)
		}
		// Partial failure: per-record encoding failures are deterministic, so
		// retrying cannot fix them, and returning an error here would make
		// exporterhelper drop the whole request, including the successfully
		// encoded records. Send what we can instead: drop only the failed
		// records, logging them and counting them as failed_client documents.
		e.set.Logger.Warn("dropping records that failed to encode",
			zap.Int("dropped_records", len(perRecordErrs)),
			zap.Error(errors.Join(perRecordErrs...)))
		e.telemetryBuilder.ElasticsearchDocsProcessed.Add(ctx, int64(len(perRecordErrs)),
			metric.WithAttributeSet(attribute.NewSet(append(
				getAttributesFromMetadataKeys(ctx, e.config.MetadataKeys),
				withOutcome("failed_client"),
			)...)))
	}
	return newEncodedRequest(items), nil
}

// newEncodedConverter adapts a per-signal recordEncoder into the exporterhelper
// RequestConverterFunc used by the request-based API. It runs on the ConsumeX
// caller goroutines.
func newEncodedConverter[T any](e *elasticsearchExporter, encode recordEncoder[T]) xexporterhelper.RequestConverterFunc[T] {
	return func(ctx context.Context, data T) (xexporterhelper.Request, error) {
		r, err := convertToEncoded(ctx, e, encode, data)
		if err != nil {
			// The request-converter path re-wraps any error as permanent; unwrap
			// so the surfaced message matches the legacy push path.
			return nil, unwrapPermanent(err)
		}
		return r, nil
	}
}

// pdataRequest wraps a not-yet-encoded pdata payload so the persistent queue
// can keep the legacy pdata on-disk format (feature gate off, or metadata_keys
// configured). It normally lives only between ConsumeX and the queue's Marshal;
// Unmarshal returns an *encodedRequest, encoding on the consumer goroutine like
// the legacy path. MergeSplit and pushEncodedRequest still accept it, converting
// on the spot, for configurations that bypass the marshal round-trip.
type pdataRequest[T any] struct {
	data    T
	e       *elasticsearchExporter
	encode  recordEncoder[T]
	items   int
	size    func() int
	marshal func(context.Context, T) ([]byte, error)
}

var (
	_ xexporterhelper.Request = (*pdataRequest[any])(nil)
	_ encodableRequest        = (*pdataRequest[any])(nil)
)

// encodableRequest is implemented by requests that can convert themselves into
// an *encodedRequest on demand.
type encodableRequest interface {
	toEncoded(ctx context.Context) (*encodedRequest, error)
}

// newPdataConverter builds the ingest-time converter for the legacy-format
// path: it wraps the pdata payload without encoding it. itemsCount and
// bytesSize report the payload's record count and proto size for the queue
// sizers; marshal writes the legacy pdatareq on-disk format.
func newPdataConverter[T any](
	e *elasticsearchExporter,
	encode recordEncoder[T],
	itemsCount func(T) int,
	bytesSize func(T) int,
	marshal func(context.Context, T) ([]byte, error),
) xexporterhelper.RequestConverterFunc[T] {
	return func(_ context.Context, data T) (xexporterhelper.Request, error) {
		return &pdataRequest[T]{
			data:    data,
			e:       e,
			encode:  encode,
			items:   itemsCount(data),
			size:    sync.OnceValue(func() int { return bytesSize(data) }),
			marshal: marshal,
		}, nil
	}
}

func (r *pdataRequest[T]) ItemsCount() int { return r.items }

func (r *pdataRequest[T]) BytesSize() int { return r.size() }

func (r *pdataRequest[T]) toEncoded(ctx context.Context) (*encodedRequest, error) {
	return convertToEncoded(ctx, r.e, r.encode, r.data)
}

// MergeSplit converts the payload to an *encodedRequest (and other too, if it
// is also a pdataRequest) and delegates. It is only reached when a pdataRequest
// bypasses the persistent queue's marshal round-trip.
func (r *pdataRequest[T]) MergeSplit(
	ctx context.Context,
	maxSize int,
	sizerType exporterhelper.RequestSizerType,
	req xexporterhelper.Request,
) ([]xexporterhelper.Request, error) {
	enc, err := r.toEncoded(ctx)
	if err != nil {
		return nil, err
	}
	if other, ok := req.(encodableRequest); ok {
		req, err = other.toEncoded(ctx)
		if err != nil {
			return nil, err
		}
	}
	return enc.MergeSplit(ctx, maxSize, sizerType, req)
}

// pushEncodedRequest is the request consumer (runs on the sending-queue consumer
// goroutines). It assembles the pre-encoded bulk items into bulk indexer
// sessions and flushes them. Normally no serialization happens here; a
// pdataRequest that bypassed the persistent queue's marshal round-trip is
// converted on the spot (matching the legacy encode-on-consumer profile).
func (e *elasticsearchExporter) pushEncodedRequest(ctx context.Context, req xexporterhelper.Request) error {
	r, ok := req.(*encodedRequest)
	if !ok {
		conv, isEncodable := req.(encodableRequest)
		if !isEncodable {
			return fmt.Errorf("elasticsearchexporter: pushEncodedRequest got %T, expected *encodedRequest", req)
		}
		var err error
		if r, err = conv.toEncoded(ctx); err != nil {
			return err
		}
	}
	return e.consumeEncodedItems(ctx, r.items)
}

// consumeEncodedItems assembles already-encoded items into the appropriate bulk
// indexer sessions and flushes them. Plain items are streamed as-is; mergeable
// metric documents with the same group identity are merged by byte splicing,
// and deferred ECS-mode metrics are grouped and encoded here over the whole
// batch — both restoring the batch-wide document grouping of the legacy pdata
// path.
func (e *elasticsearchExporter) consumeEncodedItems(ctx context.Context, items []encodedItem) error {
	var sessions encodedSessionSet
	sessions.init(e)
	defer sessions.end()

	// A single reader is reused across items: the bulk indexer reads the body
	// synchronously in Add (it copies into its own buffer), so the reader can be
	// reset for the next item. This avoids a *bytes.Reader allocation per item on
	// the consumer critical path.
	var reader bytes.Reader
	var errs []error
	var merger metricsDocMerger
	var deferred []*encodedItem
	for i := range items {
		item := &items[i]
		switch item.kind {
		case itemKindMergeableMetrics:
			merger.add(item)
			continue
		case itemKindDeferredMetrics:
			deferred = append(deferred, item)
			continue
		}
		session := sessions.get(ctx, item)
		reader.Reset(item.doc)
		if err := session.Add(
			ctx,
			item.index,
			item.docID,
			item.pipeline,
			&reader,
			item.dynamicTemplates,
			item.action,
		); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			errs = append(errs, err)
		}
	}

	for _, group := range merger.groups {
		item := group.items[0]
		body, dynamicTemplates := group.assemble(&reader)
		session := sessions.get(ctx, item)
		if err := session.Add(
			ctx,
			item.index,
			item.docID,
			item.pipeline,
			body,
			dynamicTemplates,
			item.action,
		); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			errs = append(errs, err)
		}
	}

	if len(deferred) > 0 {
		if err := e.consumeDeferredMetrics(ctx, &sessions, deferred); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			errs = append(errs, err)
		}
	}

	if err := sessions.flush(ctx); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// consumeDeferredMetrics groups the deferred (ECS-mode) payloads over the
// whole batch and encodes the resulting documents into sessions. Each item's
// payload — held as pdata from ingest, or unmarshaled from its persisted proto
// bytes — is re-walked with the item's recorded default mapping mode, and only
// the scopes resolving to ECS are collected (the others were already encoded
// at ingest). The walk only reads the payloads, so held pdata is never copied
// or mutated and the work is safely repeatable on retry.
func (e *elasticsearchExporter) consumeDeferredMetrics(ctx context.Context, sessions *encodedSessionSet, deferred []*encodedItem) error {
	var errs []error
	groups := newMetricsGroups()
	unmarshaler := pmetric.ProtoUnmarshaler{}
	ecsOnly := func(m MappingMode) bool { return m == MappingECS }
	for _, item := range deferred {
		m := item.deferredMetrics
		if item.doc != nil {
			var err error
			m, err = unmarshaler.UnmarshalMetrics(item.doc)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to unmarshal deferred metrics payload: %w", err))
				continue
			}
		}
		defaultMode := item.mappingMode
		if _, _, err := e.collectMetricsGroups(ctx, groups, m, &defaultMode, ecsOnly); err != nil {
			errs = append(errs, err)
		}
	}
	if len(groups.byIndex) > 0 || len(groups.validationErrs) > 0 {
		perRecordErrs, err := e.emitMetricsGroups(ctx, sessionSink{sessions: sessions}, groups)
		if err != nil {
			errs = append(errs, err)
		}
		if len(perRecordErrs) > 0 {
			// Deterministic per-record failures: retrying cannot fix them, so
			// log and count them instead of returning (matching the converter).
			e.set.Logger.Warn("dropping records that failed to encode",
				zap.Int("dropped_records", len(perRecordErrs)),
				zap.Error(errors.Join(perRecordErrs...)))
			e.telemetryBuilder.ElasticsearchDocsProcessed.Add(ctx, int64(len(perRecordErrs)),
				metric.WithAttributeSet(attribute.NewSet(append(
					getAttributesFromMetadataKeys(ctx, e.config.MetadataKeys),
					withOutcome("failed_client"),
				)...)))
		}
	}
	return errors.Join(errs...)
}

// mergeGroupKey identifies a mergeable metric document group within a batch.
type mergeGroupKey struct {
	mappingMode MappingMode
	index       string
	key         metricgroup.HashKey
}

// metricsDocGroup accumulates mergeable metric items that share a group key and
// have pairwise-disjoint metric names.
type metricsDocGroup struct {
	items    []*encodedItem
	names    map[string]struct{}
	docCount uint64
}

func (g *metricsDocGroup) disjoint(names []string) bool {
	for _, n := range names {
		if _, ok := g.names[n]; ok {
			return false
		}
	}
	return true
}

func (g *metricsDocGroup) append(item *encodedItem) {
	g.items = append(g.items, item)
	for _, n := range item.metricNames {
		g.names[n] = struct{}{}
	}
	if item.docCount != 0 {
		g.docCount = item.docCount
	}
}

// assemble returns the document body for the group and its dynamic templates.
// A single-item group streams its doc untouched via reader. A merged group is
// spliced: the first doc up to the end of its "metrics" fields, the other
// items' fragments joined with commas, and a freshly written tail whose
// _metric_names_hash covers the sorted union of names — byte-identical to the
// tail of a document serialized from the merged data points, so the merged
// document keeps the same TSDB identity the legacy whole-batch encoding
// produced.
func (g *metricsDocGroup) assemble(reader *bytes.Reader) (io.WriterTo, map[string]string) {
	first := g.items[0]
	if len(g.items) == 1 {
		reader.Reset(first.doc)
		return reader, first.dynamicTemplates
	}

	segments := multiSliceWriterTo{first.doc[:first.fragEnd]}
	nonEmpty := first.fragEnd > first.fragStart
	dynamicTemplates := make(map[string]string, len(first.dynamicTemplates)*len(g.items))
	names := make([]string, 0, len(g.names))
	for _, item := range g.items {
		for k, v := range item.dynamicTemplates {
			dynamicTemplates[k] = v
		}
		names = append(names, item.metricNames...)
	}
	for _, item := range g.items[1:] {
		frag := item.doc[item.fragStart:item.fragEnd]
		if len(frag) == 0 {
			continue
		}
		if nonEmpty {
			segments = append(segments, jsonComma)
		}
		segments = append(segments, frag)
		nonEmpty = true
	}
	sort.Strings(names)
	var tail bytes.Buffer
	otelserializer.AppendMergedMetricsTail(&tail, g.docCount, names)
	segments = append(segments, tail.Bytes())
	return segments, dynamicTemplates
}

var jsonComma = []byte{','}

// metricsDocMerger buckets mergeable metric items by group key. Items whose
// metric names overlap an existing bucket start a new one (identical name sets
// are true duplicates that Elasticsearch TSDB deduplicates, as before).
type metricsDocMerger struct {
	byKey  map[mergeGroupKey][]*metricsDocGroup
	groups []*metricsDocGroup // in insertion order
}

func (m *metricsDocMerger) add(item *encodedItem) {
	key := mergeGroupKey{mappingMode: item.mappingMode, index: item.index, key: item.groupKey}
	if m.byKey == nil {
		m.byKey = make(map[mergeGroupKey][]*metricsDocGroup)
	}
	for _, g := range m.byKey[key] {
		if g.disjoint(item.metricNames) {
			g.append(item)
			return
		}
	}
	g := &metricsDocGroup{names: make(map[string]struct{}, len(item.metricNames))}
	g.append(item)
	m.byKey[key] = append(m.byKey[key], g)
	m.groups = append(m.groups, g)
}

// multiSliceWriterTo streams a sequence of byte slices, used to assemble merged
// documents without copying.
type multiSliceWriterTo [][]byte

func (m multiSliceWriterTo) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for _, s := range m {
		k, err := w.Write(s)
		n += int64(k)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// encodedSessionSet lazily starts the bulk indexer sessions needed to consume a
// batch of encodedItems: one per mapping mode for targetDefault items, plus one
// per profiling indexer for profiling items.
type encodedSessionSet struct {
	e         *elasticsearchExporter
	modes     mappingModeSessions
	profiling [numSessionTargets]bulkIndexerSession
	extra     sessionList
}

func (s *encodedSessionSet) init(e *elasticsearchExporter) {
	s.e = e
	s.modes = mappingModeSessions{indexers: &e.bulkIndexers.modes}
}

func (s *encodedSessionSet) get(ctx context.Context, item *encodedItem) bulkIndexerSession {
	if item.target == targetDefault {
		return s.modes.StartSession(ctx, item.mappingMode)
	}
	if s.profiling[item.target] == nil {
		s.profiling[item.target] = s.e.profilingIndexer(item.target).StartSession(ctx)
		s.extra = append(s.extra, s.profiling[item.target])
	}
	return s.profiling[item.target]
}

func (s *encodedSessionSet) flush(ctx context.Context) error {
	return errors.Join(s.modes.Flush(ctx), s.extra.Flush(ctx))
}

func (s *encodedSessionSet) end() {
	s.modes.End()
	s.extra.End()
}

func (e *elasticsearchExporter) profilingIndexer(t sessionTarget) bulkIndexer {
	switch t {
	case targetProfilingEvents:
		return e.bulkIndexers.profilingEvents
	case targetProfilingStackTraces:
		return e.bulkIndexers.profilingStackTraces
	case targetProfilingStackFrames:
		return e.bulkIndexers.profilingStackFrames
	case targetProfilingExecutables:
		return e.bulkIndexers.profilingExecutables
	default:
		return nil // unreachable: targetDefault is handled by the caller
	}
}

// docSink consumes one freshly-encoded document together with its bulk-item
// metadata. It is the seam that lets a single per-record encode path serve both
// exporter paths: the streaming sink hands the document straight to a bulk
// indexer session (legacy/consumer path), while the buffering sink copies it into
// an encodedItem for the request queue (early/ingest path). The sink takes
// ownership of doc.
type docSink interface {
	add(ctx context.Context, item encodedItem, doc encodedDoc) error
}

// encodedDoc abstracts the freshly-encoded bytes over pooled buffers (logs,
// traces and metrics, which own their buffer) and caller-owned buffers
// (profiles, whose buffer is reused by the encoder after the callback returns).
type encodedDoc struct {
	// writerTo is streamed to bulkIndexerSession.Add. For a pooled buffer its
	// WriteTo recycles the buffer once the bulk indexer has read it.
	writerTo io.WriterTo
	// buf is the underlying buffer, read when copying the document out.
	buf *bytes.Buffer
	// recycle releases a pooled buffer after it has been copied. It is nil for
	// caller-owned buffers, which must not be recycled here.
	recycle func()
}

func pooledDoc(buf pool.PooledBuffer) encodedDoc {
	return encodedDoc{writerTo: buf, buf: buf.Buffer, recycle: buf.Recycle}
}

func rawDoc(buf *bytes.Buffer) encodedDoc {
	return encodedDoc{writerTo: buf, buf: buf}
}

// sessionSink streams encoded documents directly to bulk indexer sessions. It is
// used by the legacy push path (encoding on the consumer goroutine) and keeps the
// original streaming memory profile: one pooled buffer in flight at a time, freed
// by the bulk indexer as it reads it.
type sessionSink struct {
	sessions *encodedSessionSet
}

func (s sessionSink) add(ctx context.Context, item encodedItem, doc encodedDoc) error {
	// Not recycling on Add error: the buffer may already have been read (and thus
	// recycled) by the bulk indexer.
	return s.sessions.get(ctx, &item).Add(
		ctx, item.index, item.docID, item.pipeline, doc.writerTo, item.dynamicTemplates, item.action,
	)
}

// itemSink copies encoded documents into encodedItems for the request queue. It
// is used by the early path (encoding at ingest), materializing each document so
// it can outlive the pooled buffer and travel through the sending queue.
type itemSink struct {
	items []encodedItem
}

func (s *itemSink) add(_ context.Context, item encodedItem, doc encodedDoc) error {
	item.doc = make([]byte, doc.buf.Len())
	copy(item.doc, doc.buf.Bytes())
	if doc.recycle != nil {
		doc.recycle()
	}
	s.items = append(s.items, item)
	return nil
}

// useEarlyEncoding reports whether the exporter should serialize each record at
// ingest time. It only controls the ingest side — what the request carries and,
// with a persistent queue, which format is written to disk (early-encoded vs
// legacy pdata). Draining is unaffected: encodedEncoding.Unmarshal always reads
// both formats. Early encoding is always used unless a persistent sending queue
// (sending_queue.storage) is configured, in which case it additionally requires
// the feature gate and the absence of metadata_keys partitioning (the
// early-encoded on-disk format does not carry the request context that
// partitioning needs on drain).
func useEarlyEncoding(cf *Config) bool {
	persistentQueue := cf.QueueBatchConfig.HasValue() && cf.QueueBatchConfig.Get().StorageID != nil
	if !persistentQueue {
		return true
	}
	return metadata.ExporterElasticsearchEarlyEncodingWithPersistentQueueFeatureGate.IsEnabled() &&
		len(cf.MetadataKeys) == 0
}

const (
	// earlyEncodedMagic is the first byte of an early-encoded persistent-queue
	// payload. It cannot collide with the legacy pdata formats: pdatareq payloads
	// start with 0x0D and plain pdata protobuf starts with 0x0A, whereas 0xFF is
	// not a valid protobuf leading tag byte. This lets the decoder tell the two
	// apart and support both on drain.
	earlyEncodedMagic byte = 0xFF
	// earlyEncodedVersion is the format version of the bytes following the magic
	// byte. Bump it if the wire format below changes.
	earlyEncodedVersion byte = 1
)

// encodedEncoding is the generic queue Encoding for *encodedRequest. It marshals
// early-encoded requests to the compact wire format below, and on Unmarshal
// transparently handles both that format and legacy pdata payloads written by a
// previous (pdata-based) persistent queue. Legacy payloads are decoded and
// re-encoded through the same ingest converter so the consumer only ever sees an
// *encodedRequest. The type parameter T is the signal's pdata type; it only
// appears on the legacy-decode path, so encodedEncoding[T] still satisfies the
// non-generic queue.Encoding[Request] the queue expects.
type encodedEncoding[T any] struct {
	convert        xexporterhelper.RequestConverterFunc[T]
	unmarshalCtx   func([]byte) (context.Context, T, error) // e.g. pdatareq.UnmarshalLogs
	unmarshalPlain func([]byte) (T, error)                  // e.g. (&plog.ProtoUnmarshaler{}).UnmarshalLogs
}

func (encodedEncoding[T]) Marshal(ctx context.Context, req xexporterhelper.Request) ([]byte, error) {
	switch r := req.(type) {
	case *encodedRequest:
		return marshalEncodedRequest(r)
	case *pdataRequest[T]:
		// Legacy pdata on-disk format (gate off or metadata_keys configured):
		// keeps the queue readable by older collector versions.
		return r.marshal(ctx, r.data)
	default:
		return nil, fmt.Errorf("elasticsearchexporter: encodedEncoding.Marshal got %T, expected *encodedRequest or *pdataRequest", req)
	}
}

func (enc encodedEncoding[T]) Unmarshal(b []byte) (context.Context, xexporterhelper.Request, error) {
	if len(b) >= 2 && b[0] == earlyEncodedMagic {
		if b[1] != earlyEncodedVersion {
			return context.Background(), nil, fmt.Errorf("elasticsearchexporter: unsupported early-encoded payload version %d", b[1])
		}
		r, err := unmarshalEncodedRequest(b)
		if err != nil {
			return context.Background(), nil, err
		}
		return context.Background(), r, nil
	}

	// Legacy pdata payload written by a previous version's pdata-based persistent
	// queue. Decode it (preserving the request context so mapping-mode-from-
	// metadata still resolves) and re-encode it into an *encodedRequest.
	ctx, data, err := enc.unmarshalCtx(b)
	if errors.Is(err, pdatareq.ErrInvalidFormat) {
		// Even older payload without the request-context wrapper.
		ctx = context.Background()
		data, err = enc.unmarshalPlain(b)
	}
	if err != nil {
		return context.Background(), nil, fmt.Errorf("elasticsearchexporter: failed to unmarshal legacy payload: %w", err)
	}
	req, err := enc.convert(ctx, data)
	if err != nil {
		return context.Background(), nil, err
	}
	return ctx, req, nil
}

// Wire format (after the magic + version bytes):
//
//	uvarint itemCount
//	repeated itemCount times:
//	  uvarint mappingMode
//	  uvarint target
//	  uvarint kind
//	  if kind == itemKindMergeableMetrics:
//	    uvarint groupKey.resource, groupKey.scope, groupKey.dataPoint
//	    uvarint fragStart, fragEnd
//	    uvarint metricNamesCount
//	    repeated: string name
//	    uvarint docCount
//	  string  index, docID, pipeline, action        (uvarint length + bytes)
//	  uvarint dynamicTemplatesCount
//	  repeated: string key, string value
//	  bytes   doc                                    (uvarint length + bytes)
//
// A deferred-metrics item created at ingest holds its payload as pdata
// (deferredMetrics) and is proto-marshaled here, on persist; its doc bytes are
// the full original payload, which the consumer re-walks and filters.
func marshalEncodedRequest(r *encodedRequest) ([]byte, error) {
	buf := make([]byte, 0, r.bytesSize+16*len(r.items)+8)
	buf = append(buf, earlyEncodedMagic, earlyEncodedVersion)
	buf = binary.AppendUvarint(buf, uint64(len(r.items)))
	for i := range r.items {
		it := &r.items[i]
		buf = binary.AppendUvarint(buf, uint64(it.mappingMode))
		buf = binary.AppendUvarint(buf, uint64(it.target))
		buf = binary.AppendUvarint(buf, uint64(it.kind))
		if it.kind == itemKindMergeableMetrics {
			kr, ks, kd := it.groupKey.Uint64s()
			buf = binary.AppendUvarint(buf, kr)
			buf = binary.AppendUvarint(buf, ks)
			buf = binary.AppendUvarint(buf, kd)
			buf = binary.AppendUvarint(buf, uint64(it.fragStart))
			buf = binary.AppendUvarint(buf, uint64(it.fragEnd))
			buf = binary.AppendUvarint(buf, uint64(len(it.metricNames)))
			for _, name := range it.metricNames {
				buf = appendLenPrefixedString(buf, name)
			}
			buf = binary.AppendUvarint(buf, it.docCount)
		}
		buf = appendLenPrefixedString(buf, it.index)
		buf = appendLenPrefixedString(buf, it.docID)
		buf = appendLenPrefixedString(buf, it.pipeline)
		buf = appendLenPrefixedString(buf, it.action)
		buf = binary.AppendUvarint(buf, uint64(len(it.dynamicTemplates)))
		for k, v := range it.dynamicTemplates {
			buf = appendLenPrefixedString(buf, k)
			buf = appendLenPrefixedString(buf, v)
		}
		doc := it.doc
		if it.kind == itemKindDeferredMetrics && doc == nil {
			var err error
			doc, err = (&pmetric.ProtoMarshaler{}).MarshalMetrics(filteredDeferredMetrics(it.deferredMetrics, it.deferredScopes))
			if err != nil {
				return nil, fmt.Errorf("elasticsearchexporter: failed to marshal deferred metrics payload: %w", err)
			}
		}
		buf = appendLenPrefixed(buf, doc)
	}
	return buf, nil
}

// filteredDeferredMetrics returns the payload restricted to the given scopes
// (in walk order), so a mixed-mode payload persists only its deferred scopes.
// A nil scopes slice means the whole payload is deferred, which is returned
// as-is with no copying — the common, uniform-mode case.
func filteredDeferredMetrics(m pmetric.Metrics, scopes []scopeRef) pmetric.Metrics {
	if len(scopes) == 0 {
		return m
	}
	out := pmetric.NewMetrics()
	lastRM := -1
	var rmOut pmetric.ResourceMetrics
	for _, ref := range scopes {
		rmIn := m.ResourceMetrics().At(ref.rm)
		if ref.rm != lastRM {
			rmOut = out.ResourceMetrics().AppendEmpty()
			rmIn.Resource().CopyTo(rmOut.Resource())
			rmOut.SetSchemaUrl(rmIn.SchemaUrl())
			lastRM = ref.rm
		}
		rmIn.ScopeMetrics().At(ref.sm).CopyTo(rmOut.ScopeMetrics().AppendEmpty())
	}
	return out
}

// deferredMetricsSize approximates the proto size of the payload restricted to
// the given scopes: each parent ResourceMetrics' full size minus its
// non-deferred scopes. It slightly overcounts (the removed scopes' framing
// bytes remain included), which is the safe direction for byte thresholds.
// scopes must be in walk order (as returned by collectMetricsGroups).
func deferredMetricsSize(m pmetric.Metrics, scopes []scopeRef) int {
	sizer := pmetric.ProtoMarshaler{}
	var size int
	for i := 0; i < len(scopes); {
		rmIdx := scopes[i].rm
		rm := m.ResourceMetrics().At(rmIdx)
		size += sizer.ResourceMetricsSize(rm)
		deferred := make(map[int]struct{})
		for ; i < len(scopes) && scopes[i].rm == rmIdx; i++ {
			deferred[scopes[i].sm] = struct{}{}
		}
		for smIdx, sm := range rm.ScopeMetrics().All() {
			if _, ok := deferred[smIdx]; !ok {
				size -= sizer.ScopeMetricsSize(sm)
			}
		}
	}
	return size
}

func unmarshalEncodedRequest(b []byte) (*encodedRequest, error) {
	r := byteReader{b: b, pos: 2} // skip magic + version, validated by the caller
	count, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	// Each item encodes to at least a few bytes (two uvarints plus five
	// length prefixes), so a count larger than len(b) is corrupt. Reject it, and
	// cap the preallocation hint so a corrupt-but-in-range count can't drive a
	// huge up-front allocation before the per-item decode fails.
	if count > uint64(len(b)) {
		return nil, errors.New("elasticsearchexporter: corrupt early-encoded payload: item count exceeds payload size")
	}
	const minBytesPerItem = 8
	items := make([]encodedItem, 0, min(count, uint64(len(b))/minBytesPerItem+1))
	for range count {
		mm, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if mm >= uint64(NumMappingModes) {
			return nil, fmt.Errorf("elasticsearchexporter: corrupt early-encoded payload: invalid mapping mode %d", mm)
		}
		target, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if target >= uint64(numSessionTargets) {
			return nil, fmt.Errorf("elasticsearchexporter: corrupt early-encoded payload: invalid session target %d", target)
		}
		kind, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if kind >= uint64(numItemKinds) {
			return nil, fmt.Errorf("elasticsearchexporter: corrupt early-encoded payload: invalid item kind %d", kind)
		}
		var groupKey metricgroup.HashKey
		var fragStart, fragEnd uint64
		var metricNames []string
		var docCount uint64
		if itemKind(kind) == itemKindMergeableMetrics {
			var kr, ks, kd uint64
			if kr, err = r.uvarint(); err != nil {
				return nil, err
			}
			if ks, err = r.uvarint(); err != nil {
				return nil, err
			}
			if kd, err = r.uvarint(); err != nil {
				return nil, err
			}
			groupKey = metricgroup.NewHashKey(kr, ks, kd)
			if fragStart, err = r.uvarint(); err != nil {
				return nil, err
			}
			if fragEnd, err = r.uvarint(); err != nil {
				return nil, err
			}
			nameCount, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			// Each name takes at least one length-prefix byte; cap the
			// preallocation hint so a corrupt count can't drive a huge
			// allocation before the per-name decode fails.
			if nameCount > uint64(len(b)) {
				return nil, errors.New("elasticsearchexporter: corrupt early-encoded payload: metric name count exceeds payload size")
			}
			metricNames = make([]string, 0, min(nameCount, uint64(len(b)-r.pos)+1))
			for range nameCount {
				name, err := r.string()
				if err != nil {
					return nil, err
				}
				metricNames = append(metricNames, name)
			}
			if docCount, err = r.uvarint(); err != nil {
				return nil, err
			}
		}
		index, err := r.string()
		if err != nil {
			return nil, err
		}
		docID, err := r.string()
		if err != nil {
			return nil, err
		}
		pipeline, err := r.string()
		if err != nil {
			return nil, err
		}
		action, err := r.string()
		if err != nil {
			return nil, err
		}
		dtCount, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		var dynamicTemplates map[string]string
		if dtCount > 0 {
			dynamicTemplates = make(map[string]string, dtCount)
			for range dtCount {
				k, err := r.string()
				if err != nil {
					return nil, err
				}
				v, err := r.string()
				if err != nil {
					return nil, err
				}
				dynamicTemplates[k] = v
			}
		}
		doc, err := r.copyBytes()
		if err != nil {
			return nil, err
		}
		if itemKind(kind) == itemKindMergeableMetrics &&
			(fragStart == 0 || fragStart > fragEnd || fragEnd > uint64(len(doc))) {
			return nil, errors.New("elasticsearchexporter: corrupt early-encoded payload: invalid metric fragment offsets")
		}
		items = append(items, encodedItem{
			index:            index,
			docID:            docID,
			pipeline:         pipeline,
			action:           action,
			dynamicTemplates: dynamicTemplates,
			mappingMode:      MappingMode(mm),
			target:           sessionTarget(target),
			kind:             itemKind(kind),
			doc:              doc,
			groupKey:         groupKey,
			fragStart:        int(fragStart),
			fragEnd:          int(fragEnd),
			metricNames:      metricNames,
			docCount:         docCount,
		})
	}
	return newEncodedRequest(items), nil
}

func appendLenPrefixed(dst, p []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(p)))
	return append(dst, p...)
}

// appendLenPrefixedString is appendLenPrefixed for a string, avoiding the
// intermediate []byte(s) allocation (append accepts a string operand directly).
func appendLenPrefixedString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// byteReader is a minimal cursor over a byte slice used to decode the wire
// format. Every read is bounds-checked so malformed on-disk data returns an
// error rather than panicking.
type byteReader struct {
	b   []byte
	pos int
}

var errShortEarlyEncodedPayload = errors.New("elasticsearchexporter: corrupt early-encoded payload: unexpected end of data")

func (r *byteReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		return 0, errShortEarlyEncodedPayload
	}
	r.pos += n
	return v, nil
}

// slice reads a uvarint length prefix and returns the following n bytes as a
// sub-slice aliasing the source buffer. Callers whose result must outlive the
// (possibly reused) source use copyBytes instead.
func (r *byteReader) slice() ([]byte, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	// Compare against the remaining bytes rather than r.pos+n, which would
	// overflow for an attacker/corruption-controlled n near 2^64 and defeat the
	// bounds check. r.pos <= len(r.b) is an invariant maintained by every read.
	if n > uint64(len(r.b)-r.pos) {
		return nil, errShortEarlyEncodedPayload
	}
	p := r.b[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return p, nil
}

func (r *byteReader) string() (string, error) {
	p, err := r.slice()
	if err != nil {
		return "", err
	}
	return string(p), nil
}

// copyBytes returns a copy of the next length-prefixed slice so it does not
// alias the (potentially reused) source buffer owned by the queue.
func (r *byteReader) copyBytes() ([]byte, error) {
	p, err := r.slice()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), p...), nil
}
