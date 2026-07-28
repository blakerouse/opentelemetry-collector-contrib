// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
)

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

// queueEncoding is the queue Encoding for the request pipeline: it marshals
// encodedRequests to the wire format below and pdataRequests to the pdata
// format, and Unmarshal reads both. Pdata payloads are wrapped, not eagerly
// converted, so the read-back request reports the same sizes it was offered
// with, keeping queue size accounting symmetric. The type parameter T is the
// signal's pdata type, used only on the pdata paths.
type queueEncoding[T any] struct {
	wrapPdata      xexporterhelper.RequestConverterFunc[T]  // from newPdataConverter; never fails
	unmarshalCtx   func([]byte) (context.Context, T, error) // e.g. pdatareq.UnmarshalLogs
	unmarshalPlain func([]byte) (T, error)                  // e.g. (&plog.ProtoUnmarshaler{}).UnmarshalLogs
}

func (queueEncoding[T]) Marshal(ctx context.Context, req xexporterhelper.Request) ([]byte, error) {
	switch r := req.(type) {
	case *encodedRequest:
		return marshalEncodedRequest(r)
	case *pdataRequest[T]:
		// Legacy pdata on-disk format (gate off or metadata_keys configured):
		// keeps the queue readable by older collector versions.
		return r.marshal(ctx, r.data)
	default:
		return nil, fmt.Errorf("elasticsearchexporter: queueEncoding.Marshal got %T, expected *encodedRequest or *pdataRequest", req)
	}
}

func (enc queueEncoding[T]) Unmarshal(b []byte) (context.Context, xexporterhelper.Request, error) {
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
