// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
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

// useEarlyEncoding reports whether the exporter should serialize each record at
// ingest time. It only controls the ingest side — what the request carries and,
// with a persistent queue, which format is written to disk (early-encoded vs
// legacy pdata). Draining is unaffected: queueEncoding.Unmarshal always reads
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
