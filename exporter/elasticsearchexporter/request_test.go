// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
)

// makeItems builds a slice of encodedItem whose docs have the given byte
// sizes. Each doc is filled with a distinct marker so tests can assert that no
// item is lost or duplicated across a split.
func makeItems(sizes ...int) []encodedItem {
	items := make([]encodedItem, len(sizes))
	for i, size := range sizes {
		doc := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		items[i] = encodedItem{
			index:       fmt.Sprintf("idx-%d", i),
			doc:         doc,
			mappingMode: MappingOTel,
		}
	}
	return items
}

// itemKeys returns the index field of every item across all requests, used to
// assert that a split preserves exactly the input set (ignoring ordering).
func itemKeys(reqs []xexporterhelper.Request) []string {
	var keys []string
	for _, req := range reqs {
		for _, item := range req.(*encodedRequest).items {
			keys = append(keys, item.index)
		}
	}
	return keys
}

func TestEncodedRequest_ItemsCountAndBytesSize(t *testing.T) {
	req := newEncodedRequest(makeItems(3, 5, 2))
	require.Equal(t, 3, req.ItemsCount())
	require.Equal(t, 10, req.BytesSize())

	empty := newEncodedRequest(nil)
	require.Equal(t, 0, empty.ItemsCount())
	require.Equal(t, 0, empty.BytesSize())
}

func TestEncodedRequest_MergeSplit(t *testing.T) {
	t.Run("merge with nil, no split", func(t *testing.T) {
		r := newEncodedRequest(makeItems(1, 2))
		out, err := r.MergeSplit(context.Background(), 0, exporterhelper.RequestSizerTypeBytes, nil)
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Equal(t, 2, out[0].ItemsCount())
	})

	t.Run("merge with other request", func(t *testing.T) {
		r := newEncodedRequest(makeItems(1, 2))
		other := newEncodedRequest(makeItems(3))
		out, err := r.MergeSplit(context.Background(), 0, exporterhelper.RequestSizerTypeBytes, other)
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Equal(t, 3, out[0].ItemsCount())
	})

	t.Run("incompatible request type", func(t *testing.T) {
		r := newEncodedRequest(makeItems(1))
		_, err := r.MergeSplit(context.Background(), 0, exporterhelper.RequestSizerTypeBytes, stubRequest{})
		require.ErrorContains(t, err, "incompatible Request type")
	})

	t.Run("sizer requests is unsupported", func(t *testing.T) {
		r := newEncodedRequest(makeItems(1, 1, 1))
		_, err := r.MergeSplit(context.Background(), 2, exporterhelper.RequestSizerTypeRequests, nil)
		require.ErrorContains(t, err, "unsupported sizer type")
	})

	t.Run("sizer bytes splits", func(t *testing.T) {
		r := newEncodedRequest(makeItems(3, 3, 3))
		out, err := r.MergeSplit(context.Background(), 6, exporterhelper.RequestSizerTypeBytes, nil)
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.ElementsMatch(t, []string{"idx-0", "idx-1", "idx-2"}, itemKeys(out))
	})

	t.Run("sizer items splits", func(t *testing.T) {
		r := newEncodedRequest(makeItems(1, 1, 1, 1, 1))
		out, err := r.MergeSplit(context.Background(), 2, exporterhelper.RequestSizerTypeItems, nil)
		require.NoError(t, err)
		require.Len(t, out, 3)
		require.Equal(t, 2, out[0].ItemsCount())
		require.Equal(t, 2, out[1].ItemsCount())
		require.Equal(t, 1, out[2].ItemsCount())
	})

	t.Run("empty merged result", func(t *testing.T) {
		r := newEncodedRequest(nil)
		out, err := r.MergeSplit(context.Background(), 10, exporterhelper.RequestSizerTypeBytes, nil)
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Equal(t, 0, out[0].ItemsCount())
	})

	t.Run("unsupported sizer type", func(t *testing.T) {
		r := newEncodedRequest(makeItems(1))
		_, err := r.MergeSplit(context.Background(), 10, exporterhelper.RequestSizerType{}, nil)
		require.ErrorContains(t, err, "unsupported sizer type")
	})
}

func TestSplitByBytes(t *testing.T) {
	tests := []struct {
		name      string
		sizes     []int
		maxSize   int
		wantReqs  int
		wantItems int
	}{
		{name: "all fit in one bin", sizes: []int{1, 1, 1}, maxSize: 100, wantReqs: 1, wantItems: 3},
		{name: "greedy binning", sizes: []int{3, 3, 3}, maxSize: 6, wantReqs: 2, wantItems: 3},
		{name: "oversized item at start", sizes: []int{10, 2}, maxSize: 5, wantReqs: 2, wantItems: 2},
		{name: "oversized item in middle", sizes: []int{2, 10, 2}, maxSize: 5, wantReqs: 3, wantItems: 3},
		{name: "oversized triggers min swap", sizes: []int{1, 6, 2}, maxSize: 5, wantReqs: 3, wantItems: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := splitByBytes(makeItems(tc.sizes...), tc.maxSize)
			require.Len(t, out, tc.wantReqs)

			// Every input item appears exactly once across the output.
			var gotItems int
			for _, req := range out {
				gotItems += req.ItemsCount()
			}
			require.Equal(t, tc.wantItems, gotItems)

			// Interface contract: the last request must be the smallest.
			last := out[len(out)-1].BytesSize()
			for _, req := range out {
				require.LessOrEqual(t, last, req.BytesSize())
			}
		})
	}
}

// TestSplitByBytes_OversizedItemSentNotDropped pins a deliberate deviation
// from the upstream pdata requests: an item that individually exceeds maxSize
// is emitted as its own over-limit single-item request rather than dropped
// with an error. This relies on the batcher flushing oversize results
// immediately (never retaining them as the pending batch) and on the bulk
// session's maxFlushBytes force-flush bounding the actual request body. If
// this test breaks because the batcher contract changed, revisit that
// reliance rather than silently dropping records.
func TestSplitByBytes_OversizedItemSentNotDropped(t *testing.T) {
	const maxSize = 50
	out := splitByBytes(makeItems(10, 100, 5), maxSize)
	require.Len(t, out, 3)

	// No item is lost: the oversized one is sent, alone, exceeding maxSize.
	require.ElementsMatch(t, []string{"idx-0", "idx-1", "idx-2"}, itemKeys(out))
	var oversize []xexporterhelper.Request
	for _, req := range out {
		if req.BytesSize() > maxSize {
			oversize = append(oversize, req)
		}
	}
	require.Len(t, oversize, 1)
	require.Equal(t, 1, oversize[0].ItemsCount())
	require.Equal(t, 100, oversize[0].BytesSize())

	// Everything else stays within the limit, and the last request is the
	// smallest per the MergeSplit contract, so the retained pending batch can
	// never be the oversized request.
	require.LessOrEqual(t, out[len(out)-1].BytesSize(), maxSize)
}

func TestSplitByCount(t *testing.T) {
	t.Run("non-positive maxCount keeps single request", func(t *testing.T) {
		out := splitByCount(makeItems(1, 1, 1), 0)
		require.Len(t, out, 1)
		require.Equal(t, 3, out[0].ItemsCount())
	})
	t.Run("exact multiple", func(t *testing.T) {
		out := splitByCount(makeItems(1, 1, 1, 1), 2)
		require.Len(t, out, 2)
		require.Equal(t, 2, out[0].ItemsCount())
		require.Equal(t, 2, out[1].ItemsCount())
	})
	t.Run("with remainder", func(t *testing.T) {
		out := splitByCount(makeItems(1, 1, 1, 1, 1), 2)
		require.Len(t, out, 3)
		require.Equal(t, 1, out[2].ItemsCount())
		require.ElementsMatch(t,
			[]string{"idx-0", "idx-1", "idx-2", "idx-3", "idx-4"}, itemKeys(out))
	})
}

// TestSplitDoesNotAliasSiblings guards against the split functions returning
// sub-requests that share a backing array such that a later MergeSplit append on
// the retained (last) request overwrites already-emitted sibling requests. The
// batcher keeps one split result as its pending batch and flushes the others
// concurrently, then merges new data into the retained one, so aliasing here
// corrupts in-flight requests and races the flush goroutines.
func TestSplitDoesNotAliasSiblings(t *testing.T) {
	check := func(t *testing.T, out []xexporterhelper.Request) {
		require.GreaterOrEqual(t, len(out), 2)
		siblings := out[:len(out)-1]
		before := itemKeys(siblings)

		// Merge new items into the retained (last) request, as the batcher does.
		_, err := out[len(out)-1].MergeSplit(context.Background(), 0, exporterhelper.RequestSizerTypeBytes,
			newEncodedRequest([]encodedItem{
				{index: "NEW-X", doc: []byte("x")},
				{index: "NEW-Y", doc: []byte("y")},
			}))
		require.NoError(t, err)

		// The already-emitted siblings must be untouched.
		require.Equal(t, before, itemKeys(siblings), "MergeSplit corrupted sibling requests via shared backing array")
	}

	t.Run("splitByBytes with min-swap", func(t *testing.T) {
		// sizes {1,6,2}/max 5 emits 3 requests and min-swaps the smallest to the
		// end, so the retained request is a non-tail subslice of the array.
		check(t, splitByBytes(makeItems(1, 6, 2), 5))
	})
	t.Run("splitByCount", func(t *testing.T) {
		check(t, splitByCount(makeItems(1, 1, 1, 1, 1), 2))
	})
}

func TestNewLogsRequestConverter_AllRecordsFailedReturnsError(t *testing.T) {
	// An invalid Logstash date format makes routing fail for every record.
	// When nothing at all encodes, the conversion must fail: returning an
	// empty request would report success upstream while silently dropping the
	// whole payload.
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{"http://localhost:9200"}
		cfg.Mapping.Mode = "otel"
		cfg.LogstashFormat = LogstashFormatSettings{
			Enabled:         true,
			PrefixSeparator: "-",
			DateFormat:      "%q", // invalid strftime directive
		}
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(t, err)

	req, err := newEncodedConverter(exp, exp.encodeLogRecords)(context.Background(), benchLogs(3))
	require.Error(t, err)
	require.Nil(t, req)
}

func TestNewLogsRequestConverter_SendsGoodRecordsDespitePerRecordErrors(t *testing.T) {
	// When only some records fail to encode, the converter must return a
	// request containing the records that encoded successfully rather than
	// sacrificing them for the failed ones.
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{"http://localhost:9200"}
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(t, err)

	stub := func(context.Context, plog.Logs) ([]encodedItem, []error, error) {
		items := []encodedItem{
			{index: "logs-generic", action: "create", doc: []byte(`{"a":1}`)},
			{index: "logs-generic", action: "create", doc: []byte(`{"b":2}`)},
		}
		return items, []error{errors.New("record 3 failed to encode")}, nil
	}
	req, err := newEncodedConverter(exp, stub)(context.Background(), plog.NewLogs())
	require.NoError(t, err)
	require.Equal(t, 2, req.(*encodedRequest).ItemsCount())
}

// newPdataLogsConverter mirrors the factory's gate-off wiring for logs.
func newPdataLogsConverter(e *elasticsearchExporter) xexporterhelper.RequestConverterFunc[plog.Logs] {
	return newPdataConverter(e, e.encodeLogRecords,
		plog.Logs.LogRecordCount, (&plog.ProtoMarshaler{}).LogsSize, pdatareq.MarshalLogs)
}

func TestPdataRequest_SizesAndPush(t *testing.T) {
	var flushed atomic.Int64
	server := newBenchESServer(t, &flushed)
	exp := newBenchLogsExporter(t, server.URL)

	ld := benchLogs(3)
	req, err := newPdataLogsConverter(exp)(context.Background(), ld)
	require.NoError(t, err)

	require.Equal(t, 3, req.ItemsCount())
	require.Equal(t, (&plog.ProtoMarshaler{}).LogsSize(ld), req.BytesSize())

	// pushEncodedRequest must accept an unconverted pdataRequest (it can reach
	// the consumer when the persistent queue's marshal round-trip is bypassed)
	// and deliver its documents.
	require.NoError(t, exp.pushEncodedRequest(context.Background(), req))
	require.Equal(t, int64(3), flushed.Load())
}

func TestPdataRequest_MergeSplitConverts(t *testing.T) {
	server := newBenchESServer(t, nil)
	exp := newBenchLogsExporter(t, server.URL)
	conv := newPdataLogsConverter(exp)

	a, err := conv(context.Background(), benchLogs(2))
	require.NoError(t, err)
	b, err := conv(context.Background(), benchLogs(3))
	require.NoError(t, err)

	// pdata + pdata: both sides are converted and merged into an encodedRequest.
	reqs, err := a.MergeSplit(context.Background(), 0, exporterhelper.RequestSizerTypeItems, b)
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	require.IsType(t, &encodedRequest{}, reqs[0])
	require.Equal(t, 5, reqs[0].ItemsCount())

	// encoded + pdata: an encodedRequest retained by the batcher must accept a
	// pdataRequest as the incoming request.
	encReq, err := newEncodedConverter(exp, exp.encodeLogRecords)(context.Background(), benchLogs(1))
	require.NoError(t, err)
	c, err := conv(context.Background(), benchLogs(2))
	require.NoError(t, err)
	reqs, err = encReq.MergeSplit(context.Background(), 0, exporterhelper.RequestSizerTypeItems, c)
	require.NoError(t, err)
	require.Len(t, reqs, 1)
	require.Equal(t, 3, reqs[0].ItemsCount())
}

// stubRequest is a minimal xexporterhelper.Request used to exercise the
// wrong-type branches; its methods are never meaningfully called.
type stubRequest struct{}

func (stubRequest) ItemsCount() int { return 0 }

func (stubRequest) BytesSize() int { return 0 }

func (stubRequest) MergeSplit(context.Context, int, exporterhelper.RequestSizerType, xexporterhelper.Request) ([]xexporterhelper.Request, error) {
	return nil, nil
}

var _ xexporterhelper.Request = stubRequest{}

func TestUseEarlyEncoding(t *testing.T) {
	storageID := component.MustNewID("file_storage")
	withStorage := func(cfg *Config) { cfg.QueueBatchConfig.Get().StorageID = &storageID }
	withMetadataKeys := func(cfg *Config) { cfg.MetadataKeys = []string{"x-tenant"} }

	tests := []struct {
		name      string
		gateOn    bool
		mutators  []func(*Config)
		wantEarly bool
	}{
		{name: "in-memory queue always early", gateOn: false, wantEarly: true},
		{name: "in-memory queue with metadata_keys still early", gateOn: false, mutators: []func(*Config){withMetadataKeys}, wantEarly: true},
		{name: "persistent queue, gate off -> legacy", gateOn: false, mutators: []func(*Config){withStorage}, wantEarly: false},
		{name: "persistent queue, gate on -> early", gateOn: true, mutators: []func(*Config){withStorage}, wantEarly: true},
		{name: "persistent queue, gate on, metadata_keys -> legacy", gateOn: true, mutators: []func(*Config){withStorage, withMetadataKeys}, wantEarly: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, featuregate.GlobalRegistry().Set(metadata.ExporterElasticsearchEarlyEncodingWithPersistentQueueFeatureGate.ID(), tc.gateOn))
			t.Cleanup(func() {
				require.NoError(t, featuregate.GlobalRegistry().Set(metadata.ExporterElasticsearchEarlyEncodingWithPersistentQueueFeatureGate.ID(), false))
			})
			cfg := withDefaultConfig(tc.mutators...)
			require.Equal(t, tc.wantEarly, useEarlyEncoding(cfg))
		})
	}
}

// TestPersistedEnumValuesAreStable pins the numeric values of the enums that are
// persisted in the early-encoded persistent-queue wire format. If this test
// fails, an enum was reordered or a value was inserted mid-enum: items already
// on disk would decode with the wrong mapping mode or session target. Append new
// values at the end (before the Num* sentinel) instead.
func TestPersistedEnumValuesAreStable(t *testing.T) {
	require.Equal(t, MappingMode(0), MappingNone)
	require.Equal(t, MappingMode(1), MappingECS)
	require.Equal(t, MappingMode(2), MappingOTel)
	require.Equal(t, MappingMode(3), MappingRaw)
	require.Equal(t, MappingMode(4), MappingBodyMap)
	require.Equal(t, MappingMode(5), NumMappingModes)

	require.Equal(t, sessionTarget(0), targetDefault)
	require.Equal(t, sessionTarget(1), targetProfilingEvents)
	require.Equal(t, sessionTarget(2), targetProfilingStackTraces)
	require.Equal(t, sessionTarget(3), targetProfilingStackFrames)
	require.Equal(t, sessionTarget(4), targetProfilingExecutables)
	require.Equal(t, sessionTarget(5), numSessionTargets)

	require.Equal(t, itemKind(0), itemKindDoc)
	require.Equal(t, itemKind(1), itemKindMergeableMetrics)
	require.Equal(t, itemKind(2), itemKindDeferredMetrics)
	require.Equal(t, itemKind(3), numItemKinds)
}

// TestConverter_CanceledContextDoesNotAbortConversion: conversion is pure CPU,
// so a canceled context on its own must not abort it — the payload converts
// and is enqueued, matching main where enqueueing preceded any encoding.
func TestConverter_CanceledContextDoesNotAbortConversion(t *testing.T) {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{"http://localhost:9200"}
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, err := newEncodedConverter(exp, exp.encodeLogRecords)(ctx, benchLogs(3))
	require.NoError(t, err)
	require.Equal(t, 3, req.ItemsCount())
}

// TestConverter_CanceledContextDoesNotMaskRealError: when conversion fails
// while the context is also canceled, the surfaced error (which exporterhelper
// logs in its "Dropping data" message) must be the real cause, not
// "context canceled".
func TestConverter_CanceledContextDoesNotMaskRealError(t *testing.T) {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{"http://localhost:9200"}
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(t, err)

	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"X-Elastic-Mapping-Mode": {"no-such-mode"}}),
	})
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = newEncodedConverter(exp, exp.encodeLogRecords)(ctx, benchLogs(1))
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "invalid context mapping mode")
}

// TestPdataRefCounter: the queue's Ref/Unref must reach every pdata payload a
// request holds by reference — a pdataRequest's data and an encodedRequest's
// not-yet-marshaled deferred metric payloads — and skip disk-read deferred
// items (doc set, no held pdata) and plain doc items.
func TestPdataRefCounter(t *testing.T) {
	var refs, unrefs, metricRefs, metricUnrefs int
	c := pdataRefCounter[pmetric.Metrics]{
		ref:          func(pmetric.Metrics) { refs++ },
		unref:        func(pmetric.Metrics) { unrefs++ },
		refMetrics:   func(pmetric.Metrics) { metricRefs++ },
		unrefMetrics: func(pmetric.Metrics) { metricUnrefs++ },
	}

	exp, _ := newMetricsMergeExporter(t)
	ts := time.Unix(1719000000, 0).UTC()

	// pdataRequest: counted via the signal funcs.
	pReq, err := newPdataConverter(exp, exp.encodeMetricRecords,
		pmetric.Metrics.DataPointCount, (&pmetric.ProtoMarshaler{}).MetricsSize, pdatareq.MarshalMetrics,
	)(context.Background(), buildGauges("ecs", ts, []string{"m.a"}, []float64{1}))
	require.NoError(t, err)
	c.Ref(pReq)
	c.Unref(pReq)
	require.Equal(t, 1, refs)
	require.Equal(t, 1, unrefs)

	// encodedRequest with a held deferred payload: counted via the metrics funcs.
	items := encodeMetricsPayload(t, exp, buildGauges("ecs", ts, []string{"m.a"}, []float64{1}))
	encReq := newEncodedRequest(items)
	require.True(t, encReq.holdsPdata)
	c.Ref(encReq)
	c.Unref(encReq)
	require.Equal(t, 1, metricRefs)
	require.Equal(t, 1, metricUnrefs)

	// Plain doc items and disk-read deferred items hold no pdata: not counted.
	docItems := encodeMetricsPayload(t, exp, buildGauges("", ts, []string{"m.b"}, []float64{2}))
	diskDeferred := items[0]
	diskDeferred.doc = []byte{1, 2, 3}
	diskDeferred.deferredMetrics = pmetric.Metrics{}
	noPdata := newEncodedRequest(append(docItems, diskDeferred))
	require.False(t, noPdata.holdsPdata)
	c.Ref(noPdata)
	c.Unref(noPdata)
	require.Equal(t, 1, metricRefs)
	require.Equal(t, 1, metricUnrefs)
	require.Equal(t, 1, refs)
	require.Equal(t, 1, unrefs)
}
