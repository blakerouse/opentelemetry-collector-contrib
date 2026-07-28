// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
)

// newPersistentQueueFallbackTest builds a config with a persistent sending queue
// (sending_queue.storage) and the feature gate off, plus a bulk-recording server
// and a host exposing the storage extension. In this mode every signal's exporter
// must wrap raw pdata at ingest (pdataRequest) so the persistent queue keeps the
// legacy pdata on-disk format, then encode on drain — end to end, documents must
// still be delivered.
func newPersistentQueueFallbackTest(t *testing.T) (*Config, component.Host, *bulkRecorder) {
	rec := newBulkRecorder()
	server := newESTestServer(t, func(docs []itemRequest) ([]itemResponse, error) {
		rec.Record(docs)
		return itemsAllOK(docs)
	})

	storageID := component.MustNewID("file_storage")
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{server.URL}
		cfg.Mapping.Mode = "otel"
		cfg.QueueBatchConfig.Get().NumConsumers = 1
		cfg.QueueBatchConfig.Get().Batch.Get().FlushTimeout = 10 * time.Millisecond
		// A persistent queue with the gate off makes ingest wrap raw pdata.
		cfg.QueueBatchConfig.Get().StorageID = &storageID
	})

	host := &storageTestHost{
		ext: map[component.ID]component.Component{
			storageID: newInMemoryStorageExtension(),
		},
	}
	return cfg, host, rec
}

func TestCreateExporter_PersistentQueueFallback(t *testing.T) {
	f := NewFactory()
	set := exportertest.NewNopSettings(metadata.Type)

	t.Run("logs", func(t *testing.T) {
		cfg, host, rec := newPersistentQueueFallbackTest(t)
		exp, err := f.CreateLogs(context.Background(), set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(context.Background(), host))
		t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
		require.NoError(t, exp.ConsumeLogs(context.Background(), benchLogs(3)))
		rec.WaitItems(1)
	})

	t.Run("metrics", func(t *testing.T) {
		cfg, host, rec := newPersistentQueueFallbackTest(t)
		exp, err := f.CreateMetrics(context.Background(), set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(context.Background(), host))
		t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
		require.NoError(t, exp.ConsumeMetrics(context.Background(), benchMetrics(3)))
		rec.WaitItems(1)
	})

	t.Run("traces", func(t *testing.T) {
		cfg, host, rec := newPersistentQueueFallbackTest(t)
		exp, err := f.CreateTraces(context.Background(), set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(context.Background(), host))
		t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
		require.NoError(t, exp.ConsumeTraces(context.Background(), benchTraces(3)))
		rec.WaitItems(1)
	})

	t.Run("profiles", func(t *testing.T) {
		cfg, host, rec := newPersistentQueueFallbackTest(t)
		exp, err := f.(xexporter.Factory).CreateProfiles(context.Background(), set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(context.Background(), host))
		t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
		require.NoError(t, exp.ConsumeProfiles(context.Background(), benchProfiles(1)))
		rec.WaitItems(1)
	})
}

// storageTestHost is a component.Host that exposes a set of extensions, used to
// satisfy the persistent queue's lookup of the configured storage extension.
type storageTestHost struct {
	ext map[component.ID]component.Component
}

func (h *storageTestHost) GetExtensions() map[component.ID]component.Component {
	return h.ext
}

// inMemoryStorageExtension is a minimal storage.Extension backed by an in-memory
// map. It is sufficient to exercise the persistent-queue code paths in tests
// without touching disk.
type inMemoryStorageExtension struct {
	component.StartFunc
	component.ShutdownFunc
}

func newInMemoryStorageExtension() *inMemoryStorageExtension {
	return &inMemoryStorageExtension{}
}

func (*inMemoryStorageExtension) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return &inMemoryStorageClient{data: make(map[string][]byte)}, nil
}

type inMemoryStorageClient struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (c *inMemoryStorageClient) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key], nil
}

func (c *inMemoryStorageClient) Set(_ context.Context, key string, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

func (c *inMemoryStorageClient) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	return nil
}

func (c *inMemoryStorageClient) Batch(_ context.Context, ops ...*storage.Operation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			op.Value = c.data[op.Key]
		case storage.Set:
			c.data[op.Key] = op.Value
		case storage.Delete:
			delete(c.data, op.Key)
		}
	}
	return nil
}

func (*inMemoryStorageClient) Close(context.Context) error { return nil }

var (
	_ storage.Extension = (*inMemoryStorageExtension)(nil)
	_ storage.Client    = (*inMemoryStorageClient)(nil)
	_ component.Host    = (*storageTestHost)(nil)
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

func TestEncodedLogsRequest_ItemsCountAndBytesSize(t *testing.T) {
	req := newEncodedRequest(makeItems(3, 5, 2))
	require.Equal(t, 3, req.ItemsCount())
	require.Equal(t, 10, req.BytesSize())

	empty := newEncodedRequest(nil)
	require.Equal(t, 0, empty.ItemsCount())
	require.Equal(t, 0, empty.BytesSize())
}

func TestEncodedLogsRequest_MergeSplit(t *testing.T) {
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

func TestSplitLogsByBytes(t *testing.T) {
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

func TestSplitLogsByCount(t *testing.T) {
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

func TestGateOffEncoding_WritesLegacyReadsBoth(t *testing.T) {
	// With the feature gate off (the default) and a persistent queue, the
	// factory installs the same polymorphic Encoding as with the gate on: the
	// gate only controls the ingest-time write format. Writes go out in the
	// legacy pdata format (downgrade-safe), while reads handle both that format
	// and early-encoded payloads left over from when the gate was on — so
	// toggling the gate off never strands queued data.
	server := newBenchESServer(t, nil)
	exp := newBenchLogsExporter(t, server.URL)

	storageID := component.MustNewID("file_storage")
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{server.URL}
		cfg.QueueBatchConfig.Get().StorageID = &storageID
	})
	require.False(t, useEarlyEncoding(cfg)) // persistent queue, gate off

	qbs := requestQueueBatchSettings(cfg, newPdataLogsConverter(exp), pref.RefLogs, pref.UnrefLogs,
		pdatareq.UnmarshalLogs, (&plog.ProtoUnmarshaler{}).UnmarshalLogs)
	enc := qbs.Encoding.(encodedEncoding[plog.Logs])

	// Ingest-side write: a pdataRequest marshals to the legacy pdata format...
	pReq, err := newPdataLogsConverter(exp)(context.Background(), benchLogs(2))
	require.NoError(t, err)
	legacyBytes, err := enc.Marshal(context.Background(), pReq)
	require.NoError(t, err)
	require.NotEqual(t, earlyEncodedMagic, legacyBytes[0])

	// ...and reads back as a pdataRequest with the SAME items/bytes sizes as
	// the offered request — the symmetry the persistent queue's size accounting
	// depends on — which converts to encoded items downstream.
	_, req, err := enc.Unmarshal(legacyBytes)
	require.NoError(t, err)
	require.IsType(t, &pdataRequest[plog.Logs]{}, req)
	require.Equal(t, pReq.ItemsCount(), req.ItemsCount())
	require.Equal(t, pReq.BytesSize(), req.BytesSize())
	encReq, err := req.(encodableRequest).toEncoded(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, encReq.ItemsCount())

	// A leftover early-encoded payload (written while the gate was on) is also
	// read, even though the gate is off.
	earlyReq, err := newEncodedConverter(exp, exp.encodeLogRecords)(context.Background(), benchLogs(3))
	require.NoError(t, err)
	earlyBytes, err := enc.Marshal(context.Background(), earlyReq)
	require.NoError(t, err)
	require.Equal(t, earlyEncodedMagic, earlyBytes[0])

	_, req, err = enc.Unmarshal(earlyBytes)
	require.NoError(t, err)
	require.Equal(t, 3, req.(*encodedRequest).ItemsCount())
}

func TestPushLogsRequest_WrongType(t *testing.T) {
	e := &elasticsearchExporter{}
	err := e.pushEncodedRequest(context.Background(), stubRequest{})
	require.ErrorContains(t, err, "expected *encodedRequest")
}

// TestConsumeEncodedItems_RoutesToAllTargets verifies that consumeEncodedItems
// starts and flushes a session for every session target (the mapping-mode
// indexer plus each profiling indexer), exercising the profiling fan-out used by
// early-encoded profiles.
func TestConsumeEncodedItems_RoutesToAllTargets(t *testing.T) {
	var flushed atomic.Int64
	server := newBenchESServer(t, &flushed)
	exp := newBenchLogsExporter(t, server.URL)

	items := []encodedItem{
		{index: "logs-generic", action: "create", mappingMode: MappingOTel, target: targetDefault, doc: []byte(`{"a":1}`)},
		{index: "profiling-events-all", action: "create", target: targetProfilingEvents, doc: []byte(`{"a":1}`)},
		{index: "profiling-stacktraces", action: "create", target: targetProfilingStackTraces, doc: []byte(`{"a":1}`)},
		{index: "profiling-stackframes", action: "create", target: targetProfilingStackFrames, doc: []byte(`{"a":1}`)},
		{index: "profiling-executables", action: "update", target: targetProfilingExecutables, doc: []byte(`{"a":1}`)},
	}

	require.NoError(t, exp.consumeEncodedItems(context.Background(), items))
	require.Equal(t, int64(len(items)), flushed.Load())
}

func TestPushLogsRequest_ContextCancelled(t *testing.T) {
	server := newBenchESServer(t, nil)
	exp := newBenchLogsExporter(t, server.URL)

	req, err := newEncodedConverter(exp, exp.encodeLogRecords)(context.Background(), benchLogs(5))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With the context already cancelled the flush cannot complete; the
	// consumer must surface the context error rather than a bulk error.
	err = exp.pushEncodedRequest(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
}

// stubRequest is a minimal xexporterhelper.Request used to exercise the
// wrong-type branches; its methods are never meaningfully called.
type stubRequest struct{}

func (stubRequest) ItemsCount() int { return 0 }
func (stubRequest) BytesSize() int  { return 0 }
func (stubRequest) MergeSplit(context.Context, int, exporterhelper.RequestSizerType, xexporterhelper.Request) ([]xexporterhelper.Request, error) {
	return nil, nil
}

var _ xexporterhelper.Request = stubRequest{}

// newBenchESServer returns an httptest server that speaks just enough of the
// Elasticsearch bulk protocol for the exporter to flush successfully, echoing a
// success status for every document. It is used by the benchmarks to avoid a
// real Elasticsearch dependency while still exercising the flush path. When
// flushed is non-nil it is incremented by the number of documents in each bulk
// request, so throughput benchmarks can wait for the queue to fully drain.
func newBenchESServer(tb testing.TB, flushed *atomic.Int64) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("X-Elastic-Product", "Elasticsearch")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version": map[string]any{"number": currentESVersion},
		})
	})
	mux.HandleFunc("/_bulk", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Elastic-Product", "Elasticsearch")
		body := r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body = gr
		}
		// Each bulk item is an action line followed by a document line.
		var lines int
		dec := json.NewDecoder(body)
		for dec.More() {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			lines++
		}
		items := lines / 2
		if flushed != nil {
			flushed.Add(int64(items))
		}

		var buf bytes.Buffer
		buf.WriteString(`{"took":1,"errors":false,"items":[`)
		for i := range items {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(`{"create":{"status":200}}`)
		}
		buf.WriteString(`]}`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buf.Bytes())
	})
	server := httptest.NewServer(mux)
	tb.Cleanup(server.Close)
	return server
}

func newBenchLogsExporter(tb testing.TB, url string) *elasticsearchExporter {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{url}
		cfg.Mapping.Mode = "otel"
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(tb, err)
	require.NoError(tb, exp.Start(context.Background(), componenttest.NewNopHost()))
	tb.Cleanup(func() { require.NoError(tb, exp.Shutdown(context.Background())) })
	return exp
}

// benchLogs builds a plog.Logs with nRecords log records under a single
// resource/scope, each carrying a realistic body, timestamps, severity and a
// few attributes so the JSON encoding cost is representative.
func benchLogs(nRecords int) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "benchmark-service")
	rl.Resource().Attributes().PutStr("host.name", "benchmark-host-01")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("benchmark-scope")
	ts := pcommon.NewTimestampFromTime(time.Unix(1700000000, 0))
	for i := range nRecords {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(ts)
		lr.SetObservedTimestamp(ts)
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
		lr.Body().SetStr("this is a representative log message with some detail")
		lr.Attributes().PutStr("http.method", "GET")
		lr.Attributes().PutStr("http.target", fmt.Sprintf("/api/v1/resource/%d", i))
		lr.Attributes().PutInt("http.status_code", 200)
	}
	logs.MarkReadOnly()
	return logs
}

// benchMetrics builds a pmetric.Metrics with nRecords gauge data points, each at
// a distinct timestamp so it becomes its own document group.
func benchMetrics(nRecords int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "benchmark-service")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("benchmark-scope")
	for i := range nRecords {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("metric.gauge.%d", i))
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(int64(1700000000+i), 0)))
		dp.SetIntValue(int64(i))
	}
	metrics.MarkReadOnly()
	return metrics
}

// benchTraces builds a ptrace.Traces with nRecords spans under a single
// resource/scope.
func benchTraces(nRecords int) ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "benchmark-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("benchmark-scope")
	ts := pcommon.NewTimestampFromTime(time.Unix(1700000000, 0))
	for i := range nRecords {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("span.%d", i))
		span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, byte(i)})
		span.SetStartTimestamp(ts)
		span.SetEndTimestamp(ts)
	}
	traces.MarkReadOnly()
	return traces
}

// benchProfiles builds a pprofile.Profiles with nProfiles minimal-but-valid
// profiles sharing one dictionary. Profiles only support the otel mapping mode
// and fan out across the profiling indices, so each profile produces more than
// one bulk document.
func benchProfiles(nProfiles int) pprofile.Profiles {
	profiles := pprofile.NewProfiles()
	dic := profiles.Dictionary()

	dic.StringTable().Append("samples", "count", "cpu", "nanoseconds")
	a := dic.AttributeTable().AppendEmpty()
	a.SetKeyStrindex(4)
	dic.StringTable().Append("process.executable.build_id.htlhash")
	a.Value().SetStr("600DCAFE4A110000F2BF38C493F5FB92")
	a = dic.AttributeTable().AppendEmpty()
	a.SetKeyStrindex(5)
	dic.StringTable().Append("profile.frame.type")
	a.Value().SetStr("native")
	a = dic.AttributeTable().AppendEmpty()
	a.SetKeyStrindex(6)
	dic.StringTable().Append("host.id")
	a.Value().SetStr("localhost")
	dic.StackTable().AppendEmpty().LocationIndices().Append(0)
	dic.MappingTable().AppendEmpty().AttributeIndices().Append(0)
	l := dic.LocationTable().AppendEmpty()
	l.SetMappingIndex(0)
	l.SetAddress(111)
	l.AttributeIndices().Append(1)

	sp := profiles.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty()
	for range nProfiles {
		profile := sp.Profiles().AppendEmpty()
		profile.SampleType().SetTypeStrindex(0)
		profile.SampleType().SetUnitStrindex(1)
		profile.PeriodType().SetTypeStrindex(2)
		profile.PeriodType().SetUnitStrindex(3)
		profile.AttributeIndices().Append(2)
		profile.Samples().AppendEmpty().TimestampsUnixNano().Append(0)
	}
	return profiles
}

// startBenchExporter builds and starts an *elasticsearchExporter over url in the
// otel mapping mode, using indexOf to pick the signal's default index.
func startBenchExporter(tb testing.TB, url string, indexOf func(*Config) string) *elasticsearchExporter {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{url}
		cfg.Mapping.Mode = "otel"
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), indexOf(cfg))
	require.NoError(tb, err)
	require.NoError(tb, exp.Start(context.Background(), componenttest.NewNopHost()))
	tb.Cleanup(func() { require.NoError(tb, exp.Shutdown(context.Background())) })
	return exp
}

// benchmarkSignalExporter runs the three per-signal comparison sub-benchmarks so
// every signal is measured identically:
//
//   - consumer_legacy_encode_and_send: the legacy path, which encodes each record
//     and sends it on the sending-queue consumer goroutine.
//   - consumer_request_send_only: the request path's consumer, which only sends
//     already-encoded bytes (the critical path early encoding aims to speed up).
//   - ingest_encode: the per-record encoding relocated to the ConsumeX caller.
//
// The honest total cost of the request path is ingest_encode + send_only.
func benchmarkSignalExporter[T any](
	b *testing.B,
	exp *elasticsearchExporter,
	data T,
	pushLegacy func(context.Context, T) error,
	encode recordEncoder[T],
) {
	ctx := context.Background()
	converter := newEncodedConverter(exp, encode)
	preEncoded, err := converter(ctx, data)
	require.NoError(b, err)

	b.Run("consumer_legacy_encode_and_send", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := pushLegacy(ctx, data); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("consumer_request_send_only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := exp.pushEncodedRequest(ctx, preEncoded); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ingest_encode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := converter(ctx, data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkLogsExporter compares the two logs code paths. The key result is that
// the sending-queue consumer's critical path is much cheaper in the request
// path (consumer_request_send_only) than in the legacy path
// (consumer_legacy_encode_and_send), because per-record JSON serialization has
// been moved to ingest time (ingest_encode), which runs on the ConsumeLogs
// caller goroutines instead of the consumer goroutine.
func BenchmarkLogsExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(c *Config) string { return c.LogsIndex })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchLogs(n), exp.pushLogsData, exp.encodeLogRecords)
		})
	}
}

// BenchmarkMetricsExporter, BenchmarkTracesExporter and BenchmarkProfilesExporter
// run the identical comparison for the other signals, confirming the generic
// early-encoding path behaves like the logs path across signal types.
func BenchmarkMetricsExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(c *Config) string { return c.MetricsIndex })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchMetrics(n), exp.pushMetricsData, exp.encodeMetricRecords)
		})
	}
}

func BenchmarkTracesExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(c *Config) string { return c.TracesIndex })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchTraces(n), exp.pushTraceData, exp.encodeTraceRecords)
		})
	}
}

func BenchmarkProfilesExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(*Config) string { return "" })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("profiles=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchProfiles(n), exp.pushProfilesData, exp.encodeProfileRecords)
		})
	}
}

// BenchmarkQueuedMemory quantifies the steady-state heap retained per queued
// request, the main memory trade-off of early encoding. It builds many requests,
// holds them all, and reports the heap growth per request measured after a GC:
//
//   - early_encoded_request holds an encodedRequest: the encoded ES JSON for
//     every record plus per-item metadata. This is what an in-memory queue keeps.
//   - early_marshaled_bytes holds the encodedRequest marshaled to its wire
//     format, what a persistent queue writes to disk.
//   - legacy_pdata holds the live plog.Logs object graph, what the legacy
//     in-memory queue keeps.
//   - legacy_pdata_proto_bytes holds the marshaled protobuf, what the legacy
//     persistent queue writes.
//
// Run with a fixed, modest -benchtime (e.g. -benchtime=200x) to bound total
// retention. The reported ns/op is dominated by build cost and is not meaningful;
// read retained_B/req.
func BenchmarkQueuedMemory(b *testing.B) {
	const nRecords = 1000
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(c *Config) string { return c.LogsIndex })
	ctx := context.Background()
	converter := newEncodedConverter(exp, exp.encodeLogRecords)
	encoding := encodedEncoding[plog.Logs]{wrapPdata: newPdataLogsConverter(exp)}
	protoMarshaler := &plog.ProtoMarshaler{}

	retained := func(b *testing.B, build func() any) {
		held := make([]any, 0, b.N)
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		for b.Loop() {
			held = append(held, build())
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(len(held)), "retained_B/req")
		runtime.KeepAlive(held)
	}

	b.Run("early_encoded_request", func(b *testing.B) {
		retained(b, func() any {
			req, err := converter(ctx, benchLogs(nRecords))
			if err != nil {
				b.Fatal(err)
			}
			return req
		})
	})

	b.Run("early_marshaled_bytes", func(b *testing.B) {
		retained(b, func() any {
			req, err := converter(ctx, benchLogs(nRecords))
			if err != nil {
				b.Fatal(err)
			}
			data, err := encoding.Marshal(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			return data
		})
	})

	b.Run("legacy_pdata", func(b *testing.B) {
		retained(b, func() any { return benchLogs(nRecords) })
	})

	b.Run("legacy_pdata_proto_bytes", func(b *testing.B) {
		retained(b, func() any {
			data, err := protoMarshaler.MarshalLogs(benchLogs(nRecords))
			if err != nil {
				b.Fatal(err)
			}
			return data
		})
	})
}

// newConcurrentLogsExporter builds a fully wired logs exporter for either the
// legacy plog path (encoding on the sending-queue consumer goroutines) or the
// request path (encoding at ingest, on the ConsumeLogs caller goroutines). To
// make the placement of the encoding work the dominant factor, batching is
// disabled, compression is turned off (so the consumer's flush is cheap I/O
// rather than CPU-bound gzip), and the number of queue consumers is bounded.
func newConcurrentLogsExporter(tb testing.TB, requestPath bool, url string, numConsumers int) exporter.Logs {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{url}
		cfg.Mapping.Mode = "otel"
		cfg.ClientConfig.Compression = configcompression.Type("") // no compression
		qc := cfg.QueueBatchConfig.Get()
		qc.NumConsumers = numConsumers
		qc.QueueSize = 20000
		qc.Sizer = exporterhelper.RequestSizerTypeRequests
		qc.BlockOnOverflow = true
		qc.Batch = configoptional.None[exporterhelper.BatchConfig]()
	})
	set := exportertest.NewNopSettings(metadata.Type)
	esExp, err := newExporter(cfg, set, cfg.LogsIndex)
	require.NoError(tb, err)

	var logsExp exporter.Logs
	if requestPath {
		logsExp, err = xexporterhelper.NewLogsRequest(
			context.Background(), set,
			newEncodedConverter(esExp, esExp.encodeLogRecords), esExp.pushEncodedRequest,
			exporterhelperOptions(cfg, esExp.Start, esExp.Shutdown, xexporterhelper.QueueBatchSettings{})...,
		)
	} else {
		logsExp, err = exporterhelper.NewLogs(
			context.Background(), set, cfg, esExp.pushLogsData,
			exporterhelperOptions(cfg, esExp.Start, esExp.Shutdown, xexporterhelper.NewLogsQueueBatchSettings())...,
		)
	}
	require.NoError(tb, err)
	require.NoError(tb, logsExp.Start(context.Background(), componenttest.NewNopHost()))
	tb.Cleanup(func() { require.NoError(tb, logsExp.Shutdown(context.Background())) })
	return logsExp
}

// BenchmarkLogsExporterThroughput measures end-to-end throughput (logs/s) with
// many concurrent ConsumeLogs callers feeding a small pool of queue consumers.
// The request path encodes each batch on the (many) caller goroutines before
// enqueueing, so encoding scales with ingest concurrency. The legacy path defers
// encoding to the (few) consumer goroutines, making them the bottleneck. The
// "logs/s" metric reported for request_encode_on_ingest should exceed the one
// for legacy_encode_on_consumer.
func BenchmarkLogsExporterThroughput(b *testing.B) {
	const (
		recordsPerCall = 100
		numConsumers   = 2
	)
	ctx := context.Background()
	logs := benchLogs(recordsPerCall)

	for _, tc := range []struct {
		name        string
		requestPath bool
	}{
		{name: "legacy_encode_on_consumer", requestPath: false},
		{name: "request_encode_on_ingest", requestPath: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var flushed atomic.Int64
			server := newBenchESServer(b, &flushed)
			exp := newConcurrentLogsExporter(b, tc.requestPath, server.URL, numConsumers)

			producers := runtime.GOMAXPROCS(0)
			target := int64(b.N) * recordsPerCall

			b.ResetTimer()

			var next atomic.Int64
			var wg sync.WaitGroup
			for range producers {
				wg.Go(func() {
					for next.Add(1) <= int64(b.N) {
						if err := exp.ConsumeLogs(ctx, logs); err != nil {
							b.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
			// Wait until every enqueued record has actually been flushed so the
			// measurement covers the full ingest-to-send pipeline, not just the
			// enqueue.
			for flushed.Load() < target {
				runtime.Gosched()
			}

			b.StopTimer()
			b.ReportMetric(float64(target)/b.Elapsed().Seconds(), "logs/s")
		})
	}
}

// BenchmarkLogsBytesSize compares the cost of the byte sizer's per-request size
// measurement in the two paths. The default configuration batches by bytes, so
// the queue/batcher calls Request.BytesSize() repeatedly.
//
//   - legacy_proto_size mirrors the built-in plog logsRequest.BytesSize(), which
//     recomputes the protobuf-encoded size of the pdata on every call.
//   - request_precomputed_size is encodedRequest.BytesSize(), which returns
//     the sum of the already-encoded document lengths computed once at ingest.
//
// The request path turns an O(records) proto traversal into an O(1) field read,
// and the reported size is the real encoded payload size rather than the proto
// size.
func BenchmarkLogsBytesSize(b *testing.B) {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{"http://localhost:9200"}
		cfg.Mapping.Mode = "otel"
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(b, err)
	converter := newEncodedConverter(exp, exp.encodeLogRecords)
	marshaler := &plog.ProtoMarshaler{}

	for _, nRecords := range []int{100, 1000} {
		logs := benchLogs(nRecords)
		req, err := converter(context.Background(), logs)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("records=%d/legacy_proto_size", nRecords), func(b *testing.B) {
			b.ReportAllocs()
			var sink int
			for b.Loop() {
				sink = marshaler.LogsSize(logs)
			}
			runtime.KeepAlive(sink)
		})

		b.Run(fmt.Sprintf("records=%d/request_precomputed_size", nRecords), func(b *testing.B) {
			b.ReportAllocs()
			var sink int
			for b.Loop() {
				sink = req.BytesSize()
			}
			runtime.KeepAlive(sink)
		})
	}
}

func TestLogsUseEarlyEncoding(t *testing.T) {
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

func TestLogsRequestEncoding_RoundTrip(t *testing.T) {
	enc := encodedEncoding[plog.Logs]{} // convert unused for the early-encoded path

	items := []encodedItem{
		{index: "logs-a", docID: "id-1", pipeline: "pipe-1", action: "create", mappingMode: MappingOTel, target: targetDefault, doc: []byte(`{"@timestamp":"t","message":"one"}`)},
		{index: "logs-b", action: "create", mappingMode: MappingECS, dynamicTemplates: map[string]string{"field.one": "tmpl", "field.two": "tmpl2"}, doc: []byte(`{"message":"two"}`)},
		// a profiling item exercises a non-default session target + update action
		{index: "profiling-executables", docID: "exe-1", action: "update", mappingMode: MappingOTel, target: targetProfilingExecutables, doc: []byte(`{"exe":1}`)},
		{index: "", docID: "", pipeline: "", action: "create", mappingMode: MappingRaw, doc: []byte("{}")}, // empty string fields
	}
	orig := newEncodedRequest(items)

	b, err := enc.Marshal(context.Background(), orig)
	require.NoError(t, err)
	require.Equal(t, earlyEncodedMagic, b[0])
	require.Equal(t, earlyEncodedVersion, b[1])

	ctx, req, err := enc.Unmarshal(b)
	require.NoError(t, err)
	require.NotNil(t, ctx)

	got := req.(*encodedRequest)
	require.Equal(t, orig.ItemsCount(), got.ItemsCount())
	require.Equal(t, orig.BytesSize(), got.BytesSize())
	require.Equal(t, items, got.items)
}

func TestLogsRequestEncoding_MarshalWrongType(t *testing.T) {
	enc := encodedEncoding[plog.Logs]{}
	_, err := enc.Marshal(context.Background(), stubRequest{})
	require.ErrorContains(t, err, "expected *encodedRequest")
}

func TestLogsRequestEncoding_UnsupportedVersion(t *testing.T) {
	enc := encodedEncoding[plog.Logs]{}
	_, _, err := enc.Unmarshal([]byte{earlyEncodedMagic, 0x02, 0x00})
	require.ErrorContains(t, err, "unsupported early-encoded payload version")
}

func TestLogsRequestEncoding_CorruptPayload(t *testing.T) {
	enc := encodedEncoding[plog.Logs]{}

	// A well-formed request truncated mid-item must error, not panic.
	full, err := enc.Marshal(context.Background(),
		newEncodedRequest(makeItems(10, 20, 30)))
	require.NoError(t, err)
	for _, cut := range []int{3, 6, len(full) - 1} {
		_, _, err := enc.Unmarshal(full[:cut])
		require.Error(t, err)
	}

	// An absurd item count must be rejected rather than allocating.
	bad := []byte{earlyEncodedMagic, earlyEncodedVersion}
	bad = binary.AppendUvarint(bad, ^uint64(0))
	_, _, err = enc.Unmarshal(bad)
	require.ErrorContains(t, err, "item count exceeds payload size")

	// An out-of-range mapping mode must be rejected.
	badMode := []byte{earlyEncodedMagic, earlyEncodedVersion}
	badMode = binary.AppendUvarint(badMode, 1)                       // one item
	badMode = binary.AppendUvarint(badMode, uint64(NumMappingModes)) // invalid mapping mode
	_, _, err = enc.Unmarshal(badMode)
	require.ErrorContains(t, err, "invalid mapping mode")

	// An out-of-range session target must be rejected.
	badTarget := []byte{earlyEncodedMagic, earlyEncodedVersion}
	badTarget = binary.AppendUvarint(badTarget, 1)                         // one item
	badTarget = binary.AppendUvarint(badTarget, uint64(MappingOTel))       // valid mapping mode
	badTarget = binary.AppendUvarint(badTarget, uint64(numSessionTargets)) // invalid target
	_, _, err = enc.Unmarshal(badTarget)
	require.ErrorContains(t, err, "invalid session target")

	// An oversized per-item length prefix must be rejected, not panic. A length
	// near 2^64 would overflow a pos+n bounds check and slice with high < low.
	badLen := []byte{earlyEncodedMagic, earlyEncodedVersion}
	badLen = binary.AppendUvarint(badLen, 1)                   // one item
	badLen = binary.AppendUvarint(badLen, uint64(MappingOTel)) // valid mapping mode
	badLen = binary.AppendUvarint(badLen, uint64(targetDefault))
	badLen = binary.AppendUvarint(badLen, ^uint64(0)) // absurd index length prefix
	require.NotPanics(t, func() {
		_, _, err = enc.Unmarshal(badLen)
	})
	require.Error(t, err)
}

// assertReadsLegacyPayload verifies that encodedEncoding[T].Unmarshal
// transparently decodes both legacy on-disk formats a persistent queue may hold
// from before early encoding was enabled: a pdatareq context-wrapped payload and
// a plain pdata protobuf payload. It re-encodes them through the ingest converter
// and expects a non-empty request. This guards the per-signal unmarshalCtx /
// unmarshalPlain wiring against copy/paste mistakes between signals.
func assertReadsLegacyPayload[T any](
	t *testing.T,
	enc encodedEncoding[T],
	marshalCtx func(context.Context, T) ([]byte, error),
	marshalPlain func(T) ([]byte, error),
	data T,
) {
	t.Helper()
	// Unmarshal wraps pdata payloads in a pdataRequest (mirroring the offered
	// request so queue size accounting stays symmetric); the wrapped request
	// must convert into non-empty encoded items downstream.
	assertConverts := func(t *testing.T, req xexporterhelper.Request) {
		t.Helper()
		require.Positive(t, req.ItemsCount())
		conv, ok := req.(encodableRequest)
		require.True(t, ok, "expected a downstream-convertible request, got %T", req)
		encReq, err := conv.toEncoded(context.Background())
		require.NoError(t, err)
		require.Positive(t, encReq.ItemsCount())
	}

	t.Run("pdatareq wrapped payload", func(t *testing.T) {
		legacy, err := marshalCtx(context.Background(), data)
		require.NoError(t, err)
		require.NotEqual(t, earlyEncodedMagic, legacy[0]) // ensure it is not our format

		_, req, err := enc.Unmarshal(legacy)
		require.NoError(t, err)
		assertConverts(t, req)
	})

	t.Run("plain protobuf payload", func(t *testing.T) {
		legacy, err := marshalPlain(data)
		require.NoError(t, err)

		_, req, err := enc.Unmarshal(legacy)
		require.NoError(t, err)
		assertConverts(t, req)
	})
}

func TestRequestEncoding_ReadsLegacyPayload(t *testing.T) {
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{"http://localhost:9200"}
		cfg.Mapping.Mode = "otel"
	})
	newExp := func(t *testing.T, index string) *elasticsearchExporter {
		exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), index)
		require.NoError(t, err)
		return exp
	}

	t.Run("logs", func(t *testing.T) {
		exp := newExp(t, cfg.LogsIndex)
		enc := encodedEncoding[plog.Logs]{
			wrapPdata:      newPdataConverter(exp, exp.encodeLogRecords, plog.Logs.LogRecordCount, (&plog.ProtoMarshaler{}).LogsSize, pdatareq.MarshalLogs),
			unmarshalCtx:   pdatareq.UnmarshalLogs,
			unmarshalPlain: (&plog.ProtoUnmarshaler{}).UnmarshalLogs,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalLogs, (&plog.ProtoMarshaler{}).MarshalLogs, benchLogs(3))
	})

	t.Run("metrics", func(t *testing.T) {
		exp := newExp(t, cfg.MetricsIndex)
		enc := encodedEncoding[pmetric.Metrics]{
			wrapPdata:      newPdataConverter(exp, exp.encodeMetricRecords, pmetric.Metrics.DataPointCount, (&pmetric.ProtoMarshaler{}).MetricsSize, pdatareq.MarshalMetrics),
			unmarshalCtx:   pdatareq.UnmarshalMetrics,
			unmarshalPlain: (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalMetrics, (&pmetric.ProtoMarshaler{}).MarshalMetrics, benchMetrics(3))
	})

	t.Run("traces", func(t *testing.T) {
		exp := newExp(t, cfg.TracesIndex)
		enc := encodedEncoding[ptrace.Traces]{
			wrapPdata:      newPdataConverter(exp, exp.encodeTraceRecords, ptrace.Traces.SpanCount, (&ptrace.ProtoMarshaler{}).TracesSize, pdatareq.MarshalTraces),
			unmarshalCtx:   pdatareq.UnmarshalTraces,
			unmarshalPlain: (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalTraces, (&ptrace.ProtoMarshaler{}).MarshalTraces, benchTraces(3))
	})

	t.Run("profiles", func(t *testing.T) {
		exp := newExp(t, "")
		enc := encodedEncoding[pprofile.Profiles]{
			wrapPdata:      newPdataConverter(exp, exp.encodeProfileRecords, pprofile.Profiles.SampleCount, (&pprofile.ProtoMarshaler{}).ProfilesSize, pdatareq.MarshalProfiles),
			unmarshalCtx:   pdatareq.UnmarshalProfiles,
			unmarshalPlain: (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalProfiles, (&pprofile.ProtoMarshaler{}).MarshalProfiles, benchProfiles(1))
	})
}

func TestCreateLogsExporter_EarlyEncodingWithPersistentQueue(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(metadata.ExporterElasticsearchEarlyEncodingWithPersistentQueueFeatureGate.ID(), true))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set(metadata.ExporterElasticsearchEarlyEncodingWithPersistentQueueFeatureGate.ID(), false))
	})

	rec := newBulkRecorder()
	server := newESTestServer(t, func(docs []itemRequest) ([]itemResponse, error) {
		rec.Record(docs)
		return itemsAllOK(docs)
	})

	storageID := component.MustNewID("file_storage")
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{server.URL}
		cfg.QueueBatchConfig.Get().NumConsumers = 1
		cfg.QueueBatchConfig.Get().Batch.Get().FlushTimeout = 10 * time.Millisecond
		cfg.QueueBatchConfig.Get().StorageID = &storageID
	})

	f := NewFactory()
	exp, err := f.CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)

	host := &storageTestHost{
		ext: map[component.ID]component.Component{
			storageID: newInMemoryStorageExtension(),
		},
	}
	require.NoError(t, exp.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty().Body().SetStr("hello")

	require.NoError(t, exp.ConsumeLogs(context.Background(), logs))
	rec.WaitItems(1)
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

// TestConsumeEncodedItems_DisallowedMappingMode: an item drained from a
// persistent queue can carry a mapping mode that has since been removed from
// mapping::allowed_modes; there is no bulk indexer for it, and consuming it
// must fail with an error rather than panic on the nil indexer (which would be
// a crash loop, since the persisted item is redelivered on restart).
func TestConsumeEncodedItems_DisallowedMappingMode(t *testing.T) {
	server := newBenchESServer(t, nil)
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{server.URL}
		cfg.Mapping.AllowedModes = []string{"ecs"}
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.LogsIndex)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })

	items := []encodedItem{
		{index: "logs-generic", action: "create", mappingMode: MappingOTel, target: targetDefault, doc: []byte(`{"a":1}`)},
	}
	var consumeErr error
	require.NotPanics(t, func() {
		consumeErr = exp.consumeEncodedItems(context.Background(), items)
	})
	require.ErrorContains(t, consumeErr, "not in mapping::allowed_modes")
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

// newMetricsMergeExporter builds a started exporter against a bulk-recording
// server, for exercising encodeMetricRecords + consumeEncodedItems end to end.
func newMetricsMergeExporter(t *testing.T) (*elasticsearchExporter, *bulkRecorder) {
	rec := newBulkRecorder()
	server := newESTestServer(t, func(docs []itemRequest) ([]itemResponse, error) {
		rec.Record(docs)
		return itemsAllOK(docs)
	})
	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{server.URL}
	})
	exp, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type), cfg.MetricsIndex)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
	return exp, rec
}

// buildGauges builds a metrics payload with one resource/scope and one gauge
// data point per name, all sharing the same timestamp and attributes so they
// belong to a single document group. mode optionally sets the scope mapping
// mode attribute (e.g. "ecs"); empty means the default (otel).
func buildGauges(mode string, ts time.Time, names []string, values []float64) pmetric.Metrics {
	m := pmetric.NewMetrics()
	rm := m.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "merge-test")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("merge-scope")
	if mode != "" {
		sm.Scope().Attributes().PutStr(elasticsearch.MappingModeAttributeName, mode)
	}
	for i, name := range names {
		metric := sm.Metrics().AppendEmpty()
		metric.SetName(name)
		dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		dp.SetDoubleValue(values[i])
		dp.Attributes().PutStr("host.name", "host-1")
	}
	return m
}

func encodeMetricsPayload(t *testing.T, exp *elasticsearchExporter, m pmetric.Metrics) []encodedItem {
	items, perRecordErrs, err := exp.encodeMetricRecords(context.Background(), m)
	require.NoError(t, err)
	require.Empty(t, perRecordErrs)
	return items
}

func decodeSingleDoc(t *testing.T, rec *bulkRecorder) map[string]any {
	docs := rec.WaitItems(1)
	require.Len(t, docs, 1)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(docs[0].Document, &doc))
	return doc
}

// TestConsumeEncodedItems_MergesOTelMetricDocsAcrossPayloads is the
// differential property behind the merge design: a document group split across
// two ingest payloads must produce, after consume-time merging, a document
// semantically identical to encoding the unsplit payload — including
// _metric_names_hash, i.e. the same TSDB identity.
func TestConsumeEncodedItems_MergesOTelMetricDocsAcrossPayloads(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()

	expFull, recFull := newMetricsMergeExporter(t)
	full := buildGauges("", ts, []string{"m.a", "m.b"}, []float64{1.5, 2.5})
	require.NoError(t, expFull.consumeEncodedItems(context.Background(), encodeMetricsPayload(t, expFull, full)))
	fullDoc := decodeSingleDoc(t, recFull)

	expSplit, recSplit := newMetricsMergeExporter(t)
	items := encodeMetricsPayload(t, expSplit, buildGauges("", ts, []string{"m.a"}, []float64{1.5}))
	items = append(items, encodeMetricsPayload(t, expSplit, buildGauges("", ts, []string{"m.b"}, []float64{2.5}))...)
	require.Len(t, items, 2)
	require.Equal(t, itemKindMergeableMetrics, items[0].kind)
	require.NoError(t, expSplit.consumeEncodedItems(context.Background(), items))
	splitDoc := decodeSingleDoc(t, recSplit)

	require.Equal(t, fullDoc, splitDoc)
	require.NotEmpty(t, fullDoc["_metric_names_hash"])
	metrics, ok := splitDoc["metrics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{"m.a": 1.5, "m.b": 2.5}, metrics)
}

// TestConsumeEncodedItems_DuplicateMetricNamesNotMerged: items whose metric
// name sets overlap must stay separate documents (identical duplicates are
// deduplicated server-side by TSDB).
func TestConsumeEncodedItems_DuplicateMetricNamesNotMerged(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	exp, rec := newMetricsMergeExporter(t)

	items := encodeMetricsPayload(t, exp, buildGauges("", ts, []string{"m.a"}, []float64{1}))
	items = append(items, encodeMetricsPayload(t, exp, buildGauges("", ts, []string{"m.a"}, []float64{2}))...)
	require.NoError(t, exp.consumeEncodedItems(context.Background(), items))
	docs := rec.WaitItems(2)
	require.Len(t, docs, 2)
}

// TestConsumeEncodedItems_DeferredECSMetricsGroupAcrossPayloads: ECS-mode
// scopes are deferred as pdata and grouped at consume time, so a group split
// across payloads still becomes one document, identical to encoding the
// unsplit payload.
func TestConsumeEncodedItems_DeferredECSMetricsGroupAcrossPayloads(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()

	expFull, recFull := newMetricsMergeExporter(t)
	full := buildGauges("ecs", ts, []string{"metric.a", "metric.b"}, []float64{1.5, 2.5})
	fullItems := encodeMetricsPayload(t, expFull, full)
	require.Len(t, fullItems, 1)
	require.Equal(t, itemKindDeferredMetrics, fullItems[0].kind)
	// The payload is held by reference — nothing is copied or serialized at
	// ingest. All scopes are deferred, so deferredScopes stays nil (whole
	// payload) and the size is the exact proto size.
	require.Nil(t, fullItems[0].doc)
	require.Nil(t, fullItems[0].deferredScopes)
	require.Positive(t, fullItems[0].deferredMetrics.ResourceMetrics().Len())
	require.Equal(t, (&pmetric.ProtoMarshaler{}).MetricsSize(full), fullItems[0].size())
	require.NoError(t, expFull.consumeEncodedItems(context.Background(), fullItems))
	fullDoc := decodeSingleDoc(t, recFull)

	expSplit, recSplit := newMetricsMergeExporter(t)
	items := encodeMetricsPayload(t, expSplit, buildGauges("ecs", ts, []string{"metric.a"}, []float64{1.5}))
	items = append(items, encodeMetricsPayload(t, expSplit, buildGauges("ecs", ts, []string{"metric.b"}, []float64{2.5}))...)
	require.Len(t, items, 2)
	require.NoError(t, expSplit.consumeEncodedItems(context.Background(), items))
	splitDoc := decodeSingleDoc(t, recSplit)

	require.Equal(t, fullDoc, splitDoc)
}

// TestMetricsDocGroupAssemble exercises the byte-splicing assembly directly:
// empty fragments are skipped without stray commas, _doc_count survives, and
// the result is valid JSON containing the union of metric fields.
func TestMetricsDocGroupAssemble(t *testing.T) {
	mk := func(frag string, names []string, docCount uint64, hinted bool) *encodedItem {
		const prefix = `{"@timestamp":1,"metrics":{`
		var buf bytes.Buffer
		buf.WriteString(prefix)
		buf.WriteString(frag)
		fragStart := len(prefix)
		fragEnd := buf.Len()
		otelserializer.AppendMergedMetricsTail(&buf, docCount, names)
		return &encodedItem{
			kind:           itemKindMergeableMetrics,
			mappingMode:    MappingOTel,
			index:          "metrics-generic",
			fragStart:      fragStart,
			fragEnd:        fragEnd,
			metricNames:    names,
			docCount:       docCount,
			docCountHinted: hinted,
			doc:            buf.Bytes(),
		}
	}

	assemble := func(t *testing.T, m *metricsDocMerger) map[string]any {
		t.Helper()
		require.Len(t, m.groups, 1)
		require.False(t, m.groups[0].conflicted)
		var reader bytes.Reader
		body, _ := m.groups[0].assemble(&reader)
		var out bytes.Buffer
		_, err := body.WriteTo(&out)
		require.NoError(t, err)
		var doc map[string]any
		require.NoError(t, json.Unmarshal(out.Bytes(), &doc), "assembled doc is not valid JSON: %s", out.String())
		return doc
	}

	var merger metricsDocMerger
	merger.add(mk(`"a":1`, []string{"a"}, 0, false))
	merger.add(mk(``, nil, 0, false)) // all data points failed validation
	merger.add(mk(`"b":2`, []string{"b"}, 7, true))
	doc := assemble(t, &merger)
	require.Equal(t, map[string]any{"a": 1.0, "b": 2.0}, doc["metrics"])
	require.Equal(t, 7.0, doc["_doc_count"])
	require.NotEmpty(t, doc["_metric_names_hash"])

	// Last-hinted-wins, as on main: a later hinted zero overrides an earlier
	// value and the field is omitted, matching what serializing the merged
	// data points would produce.
	var hintedZero metricsDocMerger
	hintedZero.add(mk(`"a":1`, []string{"a"}, 5, true))
	hintedZero.add(mk(`"b":2`, []string{"b"}, 0, true))
	doc = assemble(t, &hintedZero)
	require.NotContains(t, doc, "_doc_count")
}

// TestConsumeEncodedItems_OverlappingNamesDisableGroupMerging pins the fix for
// synthesized-identity collisions: items {a}, {a,b}, {b} under one group key
// must NOT merge into two documents that both end up with names {a,b} (which
// share a TSDB identity, so Elasticsearch would silently keep only one).
// Instead the conflicted group emits every original document, whose distinct
// name sets give distinct _metric_names_hash values — all three are stored.
func TestConsumeEncodedItems_OverlappingNamesDisableGroupMerging(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	exp, rec := newMetricsMergeExporter(t)

	items := encodeMetricsPayload(t, exp, buildGauges("", ts, []string{"m.a"}, []float64{1}))
	items = append(items, encodeMetricsPayload(t, exp, buildGauges("", ts, []string{"m.a", "m.b"}, []float64{1, 2}))...)
	items = append(items, encodeMetricsPayload(t, exp, buildGauges("", ts, []string{"m.b"}, []float64{2}))...)
	require.NoError(t, exp.consumeEncodedItems(context.Background(), items))

	docs := rec.WaitItems(3)
	require.Len(t, docs, 3)
	hashes := make(map[string]int)
	for _, d := range docs {
		var doc map[string]any
		require.NoError(t, json.Unmarshal(d.Document, &doc))
		hash, ok := doc["_metric_names_hash"].(string)
		require.True(t, ok)
		hashes[hash]++
	}
	require.Len(t, hashes, 3, "every emitted document must have a distinct TSDB identity: %v", hashes)
}

// TestRequestEncoding_RoundTripMergeFields verifies the persistent-queue wire
// format round-trips the merge metadata and deferred payloads.
func TestRequestEncoding_RoundTripMergeFields(t *testing.T) {
	enc := encodedEncoding[pmetric.Metrics]{}
	items := []encodedItem{
		{
			index:          "metrics-a",
			action:         "create",
			mappingMode:    MappingOTel,
			kind:           itemKindMergeableMetrics,
			groupKey:       metricgroup.NewHashKey(1, 2, 3),
			fragStart:      5,
			fragEnd:        9,
			metricNames:    []string{"a", "b"},
			docCount:       7,
			docCountHinted: true,
			doc:            []byte(`{"m":{"a":1}}`),
		},
		{
			action:      "create",
			mappingMode: MappingECS,
			kind:        itemKindDeferredMetrics,
			doc:         []byte{1, 2, 3},
		},
	}
	b, err := enc.Marshal(context.Background(), newEncodedRequest(items))
	require.NoError(t, err)

	_, req, err := enc.Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, items, req.(*encodedRequest).items)
}

// TestConsumeEncodedItems_MixedModePayload: a payload mixing an OTel scope and
// an ECS scope produces one mergeable doc item plus one deferred item holding
// the FULL original payload; the deferred consume walk must filter to
// ECS-resolving scopes only, so exactly two documents come out — no duplicate
// of the OTel scope.
func TestConsumeEncodedItems_MixedModePayload(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	exp, rec := newMetricsMergeExporter(t)

	m := buildGauges("", ts, []string{"otel.metric"}, []float64{1})
	ecsSM := m.ResourceMetrics().At(0).ScopeMetrics().AppendEmpty()
	ecsSM.Scope().SetName("ecs-scope")
	ecsSM.Scope().Attributes().PutStr(elasticsearch.MappingModeAttributeName, "ecs")
	metric := ecsSM.Metrics().AppendEmpty()
	metric.SetName("ecs.metric")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetDoubleValue(2)

	items := encodeMetricsPayload(t, exp, m)
	require.Len(t, items, 2)
	require.Equal(t, itemKindMergeableMetrics, items[0].kind)
	require.Equal(t, itemKindDeferredMetrics, items[1].kind)

	require.NoError(t, exp.consumeEncodedItems(context.Background(), items))
	docs := rec.WaitItems(2)
	require.Len(t, docs, 2)

	var otelDocs, ecsDocs int
	for _, d := range docs {
		var doc map[string]any
		require.NoError(t, json.Unmarshal(d.Document, &doc))
		if _, ok := doc["metrics"]; ok {
			otelDocs++
		} else {
			// The ECS serializer de-dots "ecs.metric" into nested objects.
			require.Equal(t, map[string]any{"metric": 2.0}, doc["ecs"])
			ecsDocs++
		}
	}
	require.Equal(t, 1, otelDocs)
	require.Equal(t, 1, ecsDocs)

	// Sizing covers only the deferred scope, not the whole payload: at least
	// the exact filtered proto size (the approximation may overcount slightly)
	// and strictly less than the full payload's proto size.
	require.Len(t, items[1].deferredScopes, 1)
	fullSize := (&pmetric.ProtoMarshaler{}).MetricsSize(m)
	filteredExact := (&pmetric.ProtoMarshaler{}).MetricsSize(filteredDeferredMetrics(m, items[1].deferredScopes))
	require.GreaterOrEqual(t, items[1].deferredSize, filteredExact)
	require.Less(t, items[1].deferredSize, fullSize)

	// Persistence also covers only the deferred scope: the OTel scope already
	// exists as an encoded doc item and must not be stored twice.
	enc := encodedEncoding[pmetric.Metrics]{}
	b, err := enc.Marshal(context.Background(), newEncodedRequest(items))
	require.NoError(t, err)
	_, req, err := enc.Unmarshal(b)
	require.NoError(t, err)
	restored := req.(*encodedRequest).items
	require.Len(t, restored, 2)
	persisted, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(restored[1].doc)
	require.NoError(t, err)
	require.Equal(t, 1, persisted.ResourceMetrics().Len())
	require.Equal(t, 1, persisted.ResourceMetrics().At(0).ScopeMetrics().Len())
	require.Equal(t, "ecs-scope", persisted.ResourceMetrics().At(0).ScopeMetrics().At(0).Scope().Name())

	// Draining the restored items still produces exactly the same two docs.
	expDisk, recDisk := newMetricsMergeExporter(t)
	require.NoError(t, expDisk.consumeEncodedItems(context.Background(), restored))
	require.Len(t, recDisk.WaitItems(2), 2)
}

// TestRequestEncoding_DeferredMetricsMarshaledLazily: a deferred item holds
// pdata in memory and is proto-marshaled only when the persistent queue writes
// it; reading it back and consuming it produces the same document.
func TestRequestEncoding_DeferredMetricsMarshaledLazily(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	enc := encodedEncoding[pmetric.Metrics]{}

	expMem, recMem := newMetricsMergeExporter(t)
	items := encodeMetricsPayload(t, expMem, buildGauges("ecs", ts, []string{"metric.a"}, []float64{1.5}))
	require.Len(t, items, 1)
	require.Nil(t, items[0].doc)

	b, err := enc.Marshal(context.Background(), newEncodedRequest(items))
	require.NoError(t, err)
	_, req, err := enc.Unmarshal(b)
	require.NoError(t, err)
	restored := req.(*encodedRequest).items
	require.Len(t, restored, 1)
	require.NotNil(t, restored[0].doc)
	require.Equal(t, itemKindDeferredMetrics, restored[0].kind)

	// In-memory item and disk round-tripped item must produce the same doc.
	require.NoError(t, expMem.consumeEncodedItems(context.Background(), items))
	memDoc := decodeSingleDoc(t, recMem)

	expDisk, recDisk := newMetricsMergeExporter(t)
	require.NoError(t, expDisk.consumeEncodedItems(context.Background(), restored))
	diskDoc := decodeSingleDoc(t, recDisk)
	require.Equal(t, memDoc, diskDoc)
}

// TestRequestEncoding_CorruptMergeFields: invalid fragment offsets and item
// kinds in a persisted payload must error, not panic.
func TestRequestEncoding_CorruptMergeFields(t *testing.T) {
	enc := encodedEncoding[pmetric.Metrics]{}
	valid := []encodedItem{{
		index:       "metrics-a",
		action:      "create",
		mappingMode: MappingOTel,
		kind:        itemKindMergeableMetrics,
		groupKey:    metricgroup.NewHashKey(1, 2, 3),
		fragStart:   5,
		fragEnd:     20, // exceeds len(doc)==13; Marshal doesn't validate, Unmarshal must
		metricNames: []string{"a"},
		doc:         []byte(`{"m":{"a":1}}`),
	}}
	b, err := enc.Marshal(context.Background(), newEncodedRequest(valid))
	require.NoError(t, err)
	_, _, err = enc.Unmarshal(b)
	require.ErrorContains(t, err, "invalid metric fragment offsets")
}

// TestMetricsConverter_UnsupportedModeFailsConversion: when the resolved
// mapping mode cannot encode the signal at all (raw mode does not support
// metrics), every group fails deterministically and the conversion must
// return an error rather than reporting success while dropping 100% of the
// payload.
func TestMetricsConverter_UnsupportedModeFailsConversion(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	exp, _ := newMetricsMergeExporter(t)

	m := buildGauges("raw", ts, []string{"m.a"}, []float64{1})
	req, err := newEncodedConverter(exp, exp.encodeMetricRecords)(context.Background(), m)
	require.Error(t, err)
	require.ErrorContains(t, err, "does not support metrics")
	require.Nil(t, req)
}

// failingSink stands in for a streaming sink whose add fails transiently
// (e.g. a forced bulk flush against an unreachable Elasticsearch).
type failingSink struct{ err error }

func (f failingSink) add(context.Context, encodedItem, encodedDoc) error { return f.err }

// TestEmitMetricsGroups_SinkAddErrorsStayRetryable: sink.add errors must be
// returned separately from deterministic encode errors — the deferred-metrics
// consume path returns them to the queue for retry rather than dropping them
// as "failed to encode".
func TestEmitMetricsGroups_SinkAddErrorsStayRetryable(t *testing.T) {
	exp, _ := newMetricsMergeExporter(t)
	groups := newMetricsGroups()
	m := buildGauges("", time.Unix(1719000000, 0).UTC(), []string{"m.a"}, []float64{1})
	_, _, err := exp.collectMetricsGroups(context.Background(), groups, m, nil, nil)
	require.NoError(t, err)

	sinkErr := errors.New("transient flush failure")
	encodeErrs, addErrs, err := exp.emitMetricsGroups(context.Background(), failingSink{err: sinkErr}, groups)
	require.NoError(t, err)
	require.Empty(t, encodeErrs)
	require.Len(t, addErrs, 1)
	require.ErrorIs(t, addErrs[0], sinkErr)
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

// TestDeferredMetricsSize_OverEstimatesPersistedSize pins the sizing
// invariant: the estimate must never under-count the persisted filtered
// payload. The payload shape (a whole ResourceMetrics excluded, another
// deferred) exercises the per-ResourceMetrics outer framing bytes that
// ResourceMetricsSize excludes.
func TestDeferredMetricsSize_OverEstimatesPersistedSize(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	exp, _ := newMetricsMergeExporter(t)

	// rm0: OTel-only scope (excluded entirely); rm1: ECS scope (deferred).
	m := buildGauges("", ts, []string{"otel.metric"}, []float64{1})
	rm1 := m.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("service.name", "merge-test-2")
	sm := rm1.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("ecs-scope")
	sm.Scope().Attributes().PutStr(elasticsearch.MappingModeAttributeName, "ecs")
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("ecs.metric")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetDoubleValue(2)

	items := encodeMetricsPayload(t, exp, m)
	require.Len(t, items, 2)
	deferredItem := items[1]
	require.Equal(t, itemKindDeferredMetrics, deferredItem.kind)
	require.Len(t, deferredItem.deferredScopes, 1)

	persisted, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(
		filteredDeferredMetrics(deferredItem.deferredMetrics, deferredItem.deferredScopes))
	require.NoError(t, err)
	require.GreaterOrEqual(t, deferredItem.deferredSize, len(persisted))
}
