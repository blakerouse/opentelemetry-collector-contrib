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

	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/pool"
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
	// doc is the final serialized bulk-item body. It is populated only on the
	// early/ingest path (by itemSink, from the separate encodedDoc handed to
	// docSink.add); the streaming/legacy path never sets it.
	doc []byte
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
		bytesSize += len(items[i].doc)
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
	_ context.Context,
	maxSize int,
	sizerType exporterhelper.RequestSizerType,
	req xexporterhelper.Request,
) ([]xexporterhelper.Request, error) {
	merged := r.items
	if req != nil {
		other, ok := req.(*encodedRequest)
		if !ok {
			return nil, errors.New("elasticsearchexporter: MergeSplit got incompatible Request type")
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
	case exporterhelper.RequestSizerTypeRequests:
		return []xexporterhelper.Request{newEncodedRequest(merged)}, nil
	default:
		return nil, fmt.Errorf("elasticsearchexporter: unsupported sizer type %q", sizerType.String())
	}
}

// splitByBytes places items into bins of at most maxSize bytes (measured by
// encoded document size). Items that individually equal or exceed maxSize are
// emitted as their own single-item Request. Only the last Request is guaranteed
// to be the smallest, so we track the running minimum and swap it to the end
// once rather than sorting.
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
		size := len(items[i].doc)
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
// (logging and counting them) and sends the rest. A non-nil err aborts the
// whole batch (e.g. mapping-mode resolution failure).
type recordEncoder[T any] func(ctx context.Context, data T) (items []encodedItem, perRecordErrs []error, err error)

// newEncodedConverter adapts a per-signal recordEncoder into the exporterhelper
// RequestConverterFunc used by the request-based API. It runs on the ConsumeX
// caller goroutines.
func newEncodedConverter[T any](e *elasticsearchExporter, encode recordEncoder[T]) xexporterhelper.RequestConverterFunc[T] {
	return func(ctx context.Context, data T) (xexporterhelper.Request, error) {
		items, perRecordErrs, err := encode(ctx, data)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return nil, cerr
			}
			// The request-converter path re-wraps any error as permanent; unwrap
			// so the surfaced message matches the legacy push path.
			return nil, unwrapPermanent(err)
		}
		if len(perRecordErrs) > 0 {
			// Per-record encoding failures are deterministic, so retrying cannot
			// fix them, and returning an error here would make exporterhelper
			// drop the whole request, including the successfully encoded records.
			// Send what we can instead: drop only the failed records, logging
			// them and counting them as failed_client documents.
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
}

// pushEncodedRequest is the request consumer (runs on the sending-queue consumer
// goroutines). It assembles the pre-encoded bulk items into bulk indexer
// sessions and flushes them. No serialization happens here.
func (e *elasticsearchExporter) pushEncodedRequest(ctx context.Context, req xexporterhelper.Request) error {
	r, ok := req.(*encodedRequest)
	if !ok {
		return fmt.Errorf("elasticsearchexporter: pushEncodedRequest got %T, expected *encodedRequest", req)
	}
	return e.consumeEncodedItems(ctx, r.items)
}

// consumeEncodedItems assembles already-encoded items into the appropriate bulk
// indexer sessions and flushes them.
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
	for i := range items {
		item := &items[i]
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

	if err := sessions.flush(ctx); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
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
// ingest time (the request path) rather than on the sending-queue consumer (the
// legacy pdata path). Early encoding is always used unless a persistent sending
// queue (sending_queue.storage) is configured, in which case it additionally
// requires the feature gate and the absence of metadata_keys partitioning (the
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

func (encodedEncoding[T]) Marshal(_ context.Context, req xexporterhelper.Request) ([]byte, error) {
	r, ok := req.(*encodedRequest)
	if !ok {
		return nil, fmt.Errorf("elasticsearchexporter: encodedEncoding.Marshal got %T, expected *encodedRequest", req)
	}
	return marshalEncodedRequest(r), nil
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
//	  string  index, docID, pipeline, action        (uvarint length + bytes)
//	  uvarint dynamicTemplatesCount
//	  repeated: string key, string value
//	  bytes   doc                                    (uvarint length + bytes)
func marshalEncodedRequest(r *encodedRequest) []byte {
	buf := make([]byte, 0, r.bytesSize+16*len(r.items)+8)
	buf = append(buf, earlyEncodedMagic, earlyEncodedVersion)
	buf = binary.AppendUvarint(buf, uint64(len(r.items)))
	for i := range r.items {
		it := &r.items[i]
		buf = binary.AppendUvarint(buf, uint64(it.mappingMode))
		buf = binary.AppendUvarint(buf, uint64(it.target))
		buf = appendLenPrefixedString(buf, it.index)
		buf = appendLenPrefixedString(buf, it.docID)
		buf = appendLenPrefixedString(buf, it.pipeline)
		buf = appendLenPrefixedString(buf, it.action)
		buf = binary.AppendUvarint(buf, uint64(len(it.dynamicTemplates)))
		for k, v := range it.dynamicTemplates {
			buf = appendLenPrefixedString(buf, k)
			buf = appendLenPrefixedString(buf, v)
		}
		buf = appendLenPrefixed(buf, it.doc)
	}
	return buf
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
		items = append(items, encodedItem{
			index:            index,
			docID:            docID,
			pipeline:         pipeline,
			action:           action,
			dynamicTemplates: dynamicTemplates,
			mappingMode:      MappingMode(mm),
			target:           sessionTarget(target),
			doc:              doc,
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
