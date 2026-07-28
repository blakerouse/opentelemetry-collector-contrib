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
	// itemKindDeferredMetrics is a pmetric.Metrics payload holding ECS-mode
	// scopes, grouped and encoded at consume time over the whole batch.
	itemKindDeferredMetrics
	numItemKinds
)

// encodedItem is a single record serialized to its final bulk item form.
// Serializing at ingest (in the request converter, on the ConsumeX caller
// goroutines) keeps per-record JSON encoding off the queue consumer's critical
// path. The shape is signal-agnostic, matching bulkIndexerSession.Add.
type encodedItem struct {
	index            string
	docID            string
	pipeline         string
	action           string
	dynamicTemplates map[string]string
	mappingMode      MappingMode
	target           sessionTarget
	kind             itemKind
	// doc is the final serialized bulk-item body — or, for a deferred-metrics
	// item read back from the persistent queue, the proto-marshaled pdata.
	doc []byte

	// Merge metadata, set only for itemKindMergeableMetrics: the data point
	// group identity, the offsets of the inner "metrics" object fields within
	// doc, the sorted metric names, and the _doc_count. Same-group documents
	// are merged at consume time; see consumeEncodedItems.
	groupKey       metricgroup.HashKey
	fragStart      int
	fragEnd        int
	metricNames    []string
	docCount       uint64
	docCountHinted bool

	// deferredMetrics, set only for itemKindDeferredMetrics created at ingest,
	// holds the original payload by reference until the item is persisted (see
	// marshalEncodedRequest) or consumed; items read back from disk carry doc
	// instead. For deferred items, mappingMode records the request-default
	// mapping mode used to re-resolve scope modes at consume. The payload is
	// ref-counted across the memory queue by pdataRefCounter; as with the
	// standard pdata requests, that protection ends once the batcher retains a
	// pending batch (relevant only if pdata.useProtoPooling is enabled).
	deferredMetrics pmetric.Metrics
	// deferredScopes lists the deferred scopes of a mixed-mode payload (nil =
	// all of them); sizing and persistence cover only those, since the other
	// scopes already exist as encoded doc items. Never serialized.
	deferredScopes []scopeRef
	// deferredSize is the deferred data's proto size, used by the byte sizers.
	deferredSize int
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
	// holdsPdata is true when any item holds pdata by reference (a deferred
	// metrics item not yet marshaled), so the reference counter can skip
	// scanning items on requests that cannot hold any.
	holdsPdata bool
}

var _ xexporterhelper.Request = (*encodedRequest)(nil)

func newEncodedRequest(items []encodedItem) *encodedRequest {
	var bytesSize int
	var holdsPdata bool
	for i := range items {
		bytesSize += items[i].size()
		holdsPdata = holdsPdata || (items[i].kind == itemKindDeferredMetrics && items[i].doc == nil)
	}
	return &encodedRequest{items: items, bytesSize: bytesSize, holdsPdata: holdsPdata}
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
			// A pdataRequest (offered directly or read back from a persistent
			// queue) reaches the batcher unconverted; convert it here.
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
// encoded document size). An item that alone reaches maxSize is emitted as its
// own over-limit single-item Request rather than dropped; that is safe because
// the batcher flushes oversize requests immediately and the bulk session's
// maxFlushBytes force-flush bounds the actual request body. Only the last
// Request must be the smallest, so the running minimum is swapped to the end
// instead of sorting.
func splitByBytes(items []encodedItem, maxSize int) []xexporterhelper.Request {
	var out []xexporterhelper.Request
	var curSize, minIdx, minSize int

	emit := func(slice []encodedItem) {
		// cap==len so a later MergeSplit append on the retained sub-request
		// reallocates instead of writing into a sibling's shared backing array.
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
		// cap==len so a later append reallocates instead of overwriting a
		// sibling's shared backing array; see splitByBytes.
		out = append(out, newEncodedRequest(items[i:end:end]))
	}
	return out
}

// recordEncoder serializes one pdata payload (plog.Logs, pmetric.Metrics, ...)
// into encoded bulk items at ingest time. perRecordErrs are deterministic
// per-record encoding failures; a non-nil err aborts the whole payload (e.g.
// mapping-mode resolution failure).
type recordEncoder[T any] func(ctx context.Context, data T) (items []encodedItem, perRecordErrs []error, err error)

// convertToEncoded encodes data into an *encodedRequest, dropping (and logging
// and counting) records that fail to encode. A non-nil error means the whole
// payload failed (e.g. mapping-mode resolution); any consumererror permanent
// wrapper is preserved.
func convertToEncoded[T any](ctx context.Context, e *elasticsearchExporter, encode recordEncoder[T], data T) (*encodedRequest, error) {
	items, perRecordErrs, err := encode(ctx, data)
	if err != nil {
		// No ctx.Err() substitution: conversion is pure CPU and every converter
		// error is treated as permanent; substituting would mask the real cause.
		return nil, err
	}
	if len(perRecordErrs) > 0 {
		// Nothing encoded: returning success would hide a total drop from the
		// pipeline, so surface the joined per-record errors instead.
		if len(items) == 0 {
			return nil, errors.Join(perRecordErrs...)
		}
		// Deterministic per-record failures: send what encoded, drop the rest
		// with a log and a failed_client count; an error would drop everything.
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
			// The request-converter path re-wraps errors as permanent; unwrap
			// to avoid a doubled "Permanent error:" prefix.
			return nil, unwrapPermanent(err)
		}
		return r, nil
	}
}

// pdataRequest wraps a not-yet-encoded pdata payload so the persistent queue
// keeps the pdata on-disk format (feature gate off, or metadata_keys
// configured). Unmarshal also returns it for pdata payloads, mirroring the
// offered request so queue size accounting stays symmetric. MergeSplit and
// pushEncodedRequest convert it on first use, on the consumer goroutine.
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

// pdataRefCounter implements the queue's reference counting: the memory queue
// refs a request on Offer and unrefs it after consume, so pdata held by a
// queued request is not reclaimed by proto pooling. ref/unref cover a
// pdataRequest's payload; refMetrics/unrefMetrics cover the deferred metric
// payloads an encodedRequest can hold.
type pdataRefCounter[T any] struct {
	ref, unref               func(T)
	refMetrics, unrefMetrics func(pmetric.Metrics)
}

func (c pdataRefCounter[T]) Ref(req xexporterhelper.Request) {
	c.count(req, c.ref, c.refMetrics)
}

func (c pdataRefCounter[T]) Unref(req xexporterhelper.Request) {
	c.count(req, c.unref, c.unrefMetrics)
}

func (pdataRefCounter[T]) count(req xexporterhelper.Request, f func(T), fMetrics func(pmetric.Metrics)) {
	switch r := req.(type) {
	case *pdataRequest[T]:
		f(r.data)
	case *encodedRequest:
		if !r.holdsPdata {
			return
		}
		for i := range r.items {
			it := &r.items[i]
			if it.kind == itemKindDeferredMetrics && it.doc == nil {
				fMetrics(it.deferredMetrics)
			}
		}
	}
}

// newPdataConverter builds the ingest-time converter for the pdata-format
// path: it wraps the payload without encoding it. itemsCount and bytesSize
// report record count and proto size for the queue sizers; marshal writes the
// pdatareq on-disk format.
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
// is also a pdataRequest) and delegates.
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

// pushEncodedRequest is the request consumer (runs on the sending-queue
// consumer goroutines). It assembles the pre-encoded bulk items into bulk
// indexer sessions and flushes them; a pdataRequest that reached here
// unconverted is converted on the spot.
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

// consumeEncodedItems assembles already-encoded items into the appropriate
// bulk indexer sessions and flushes them. Plain items stream as-is; mergeable
// metric documents with the same group identity merge by byte splicing, and
// deferred ECS-mode metrics are grouped and encoded over the whole batch.
func (e *elasticsearchExporter) consumeEncodedItems(ctx context.Context, items []encodedItem) error {
	var sessions encodedSessionSet
	sessions.init(e)
	defer sessions.end()

	// One reader reused for all items: the bulk indexer copies the body
	// synchronously in Add, so no per-item reader allocation is needed.
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
		if group.conflicted {
			// Overlapping names in the group: emit the original documents
			// unmerged so no two synthesized documents share a TSDB identity.
			for _, item := range group.items {
				reader.Reset(item.doc)
				if err := sessions.get(ctx, item).Add(
					ctx, item.index, item.docID, item.pipeline, &reader, item.dynamicTemplates, item.action,
				); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					errs = append(errs, err)
				}
			}
			continue
		}
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
		encodeErrs, addErrs, err := e.emitMetricsGroups(ctx, sessionSink{sessions: sessions}, groups)
		if err != nil {
			errs = append(errs, err)
		}
		// Add errors can be transient bulk-indexer failures; return them so
		// the request stays retryable.
		errs = append(errs, addErrs...)
		if len(encodeErrs) > 0 {
			// Deterministic encoding failures: log and count instead of
			// returning, since retrying cannot fix them.
			e.set.Logger.Warn("dropping records that failed to encode",
				zap.Int("dropped_records", len(encodeErrs)),
				zap.Error(errors.Join(encodeErrs...)))
			e.telemetryBuilder.ElasticsearchDocsProcessed.Add(ctx, int64(len(encodeErrs)),
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

// metricsDocGroup accumulates the mergeable metric items sharing one group
// key. Items merge while their metric names stay pairwise disjoint; the first
// overlap marks the group conflicted, and a conflicted group emits every item
// unmerged. Emitted name sets are then either the merged union or exactly as
// ingested, so the merger never synthesizes two documents with the same TSDB
// identity (which Elasticsearch would deduplicate).
type metricsDocGroup struct {
	items          []*encodedItem
	names          map[string]struct{}
	docCount       uint64
	docCountHinted bool
	conflicted     bool
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
	if !g.conflicted && (len(g.items) == 0 || g.disjoint(item.metricNames)) {
		for _, n := range item.metricNames {
			g.names[n] = struct{}{}
		}
		// Last-hinted-wins, as the serializer does per document: a hinted
		// zero overrides an earlier value and omits the field.
		if item.docCountHinted {
			g.docCount = item.docCount
			g.docCountHinted = true
		}
	} else {
		g.conflicted = true
	}
	g.items = append(g.items, item)
}

// assemble returns the group's document body and dynamic templates. A
// single-item group streams its doc untouched; a merged group splices the
// first doc's prefix, the other fragments comma-joined, and a fresh tail whose
// _metric_names_hash covers the sorted name union — byte-identical to
// serializing the merged data points, preserving the TSDB identity.
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

// metricsDocMerger buckets mergeable metric items by group key, one group per
// key; see metricsDocGroup for the conflict semantics.
type metricsDocMerger struct {
	byKey  map[mergeGroupKey]*metricsDocGroup
	groups []*metricsDocGroup // in insertion order
}

func (m *metricsDocMerger) add(item *encodedItem) {
	key := mergeGroupKey{mappingMode: item.mappingMode, index: item.index, key: item.groupKey}
	if m.byKey == nil {
		m.byKey = make(map[mergeGroupKey]*metricsDocGroup)
	}
	g, ok := m.byKey[key]
	if !ok {
		g = &metricsDocGroup{names: make(map[string]struct{}, len(item.metricNames))}
		m.byKey[key] = g
		m.groups = append(m.groups, g)
	}
	g.append(item)
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
// metadata. It is the seam that lets one per-record encode path serve both
// sinks: sessionSink streams the document straight to a bulk indexer session,
// itemSink copies it into an encodedItem for the request queue. The sink takes
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

// sessionSink streams encoded documents directly to bulk indexer sessions,
// keeping one pooled buffer in flight at a time, freed by the bulk indexer as
// it reads it.
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

// itemSink copies encoded documents into encodedItems for the request queue,
// materializing each document so it can outlive the pooled buffer and travel
// through the sending queue.
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

// encodedEncoding is the queue Encoding for the request pipeline: it marshals
// encodedRequests to the wire format below and pdataRequests to the pdata
// format, and Unmarshal reads both. Pdata payloads are wrapped, not eagerly
// converted, so the read-back request reports the same sizes it was offered
// with, keeping queue size accounting symmetric. The type parameter T is the
// signal's pdata type, used only on the pdata paths.
type encodedEncoding[T any] struct {
	wrapPdata      xexporterhelper.RequestConverterFunc[T]  // from newPdataConverter; never fails
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

	// Pdata payload: decode it, preserving the request context, and wrap it
	// so the read-back request reports the same sizes it was offered with.
	ctx, data, err := enc.unmarshalCtx(b)
	if errors.Is(err, pdatareq.ErrInvalidFormat) {
		// Even older payload without the request-context wrapper.
		ctx = context.Background()
		data, err = enc.unmarshalPlain(b)
	}
	if err != nil {
		return context.Background(), nil, fmt.Errorf("elasticsearchexporter: failed to unmarshal legacy payload: %w", err)
	}
	req, err := enc.wrapPdata(ctx, data)
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
//	    uvarint docCountHinted (0 or 1)
//	  string  index, docID, pipeline, action        (uvarint length + bytes)
//	  uvarint dynamicTemplatesCount
//	  repeated: string key, string value
//	  bytes   doc                                    (uvarint length + bytes)
//
// A deferred-metrics item created at ingest holds its payload as pdata and is
// proto-marshaled here, on persist, restricted to its deferred scopes.
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
			var hinted uint64
			if it.docCountHinted {
				hinted = 1
			}
			buf = binary.AppendUvarint(buf, hinted)
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

// deferredMetricsSize approximates the proto size of the payload restricted
// to the given scopes (which must be in walk order): each parent
// ResourceMetrics' size minus its non-deferred scopes, plus the outer
// per-ResourceMetrics field framing. The estimate is guaranteed >= the
// persisted filtered payload's size, the safe direction for byte thresholds.
func deferredMetricsSize(m pmetric.Metrics, scopes []scopeRef) int {
	sizer := pmetric.ProtoMarshaler{}
	var size int
	for i := 0; i < len(scopes); {
		rmIdx := scopes[i].rm
		rm := m.ResourceMetrics().At(rmIdx)
		rmSize := sizer.ResourceMetricsSize(rm)
		deferred := make(map[int]struct{})
		for ; i < len(scopes) && scopes[i].rm == rmIdx; i++ {
			deferred[scopes[i].sm] = struct{}{}
		}
		for smIdx, sm := range rm.ScopeMetrics().All() {
			if _, ok := deferred[smIdx]; !ok {
				rmSize -= sizer.ScopeMetricsSize(sm)
			}
		}
		size += rmSize + 1 + uvarintLen(uint64(rmSize))
	}
	return size
}

// uvarintLen returns the encoded length of v as a protobuf varint.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func unmarshalEncodedRequest(b []byte) (*encodedRequest, error) {
	r := byteReader{b: b, pos: 2} // skip magic + version, validated by the caller
	count, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	// A count larger than len(b) is corrupt; also cap the preallocation hint
	// so an in-range corrupt count cannot drive a huge up-front allocation.
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
		var docCount, docCountHinted uint64
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
			// Cap the preallocation hint so a corrupt count cannot drive a
			// huge allocation before the per-name decode fails.
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
			if docCountHinted, err = r.uvarint(); err != nil {
				return nil, err
			}
			if docCountHinted > 1 {
				return nil, errors.New("elasticsearchexporter: corrupt early-encoded payload: invalid doc count hint flag")
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
			docCountHinted:   docCountHinted == 1,
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
	// Compare against the remaining bytes: r.pos+n would overflow for a
	// corruption-controlled n near 2^64 and defeat the bounds check.
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
