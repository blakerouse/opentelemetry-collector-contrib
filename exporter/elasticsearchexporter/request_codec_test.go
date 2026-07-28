// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
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
	enc := qbs.Encoding.(queueEncoding[plog.Logs])

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

func TestRequestEncoding_RoundTrip(t *testing.T) {
	enc := queueEncoding[plog.Logs]{} // convert unused for the early-encoded path

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

func TestRequestEncoding_MarshalWrongType(t *testing.T) {
	enc := queueEncoding[plog.Logs]{}
	_, err := enc.Marshal(context.Background(), stubRequest{})
	require.ErrorContains(t, err, "expected *encodedRequest")
}

func TestRequestEncoding_UnsupportedVersion(t *testing.T) {
	enc := queueEncoding[plog.Logs]{}
	_, _, err := enc.Unmarshal([]byte{earlyEncodedMagic, 0x02, 0x00})
	require.ErrorContains(t, err, "unsupported early-encoded payload version")
}

func TestRequestEncoding_CorruptPayload(t *testing.T) {
	enc := queueEncoding[plog.Logs]{}

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

// assertReadsLegacyPayload verifies that queueEncoding[T].Unmarshal
// transparently decodes both legacy on-disk formats a persistent queue may hold
// from before early encoding was enabled: a pdatareq context-wrapped payload and
// a plain pdata protobuf payload. It re-encodes them through the ingest converter
// and expects a non-empty request. This guards the per-signal unmarshalCtx /
// unmarshalPlain wiring against copy/paste mistakes between signals.
func assertReadsLegacyPayload[T any](
	t *testing.T,
	enc queueEncoding[T],
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
		enc := queueEncoding[plog.Logs]{
			wrapPdata:      newPdataConverter(exp, exp.encodeLogRecords, plog.Logs.LogRecordCount, (&plog.ProtoMarshaler{}).LogsSize, pdatareq.MarshalLogs),
			unmarshalCtx:   pdatareq.UnmarshalLogs,
			unmarshalPlain: (&plog.ProtoUnmarshaler{}).UnmarshalLogs,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalLogs, (&plog.ProtoMarshaler{}).MarshalLogs, benchLogs(3))
	})

	t.Run("metrics", func(t *testing.T) {
		exp := newExp(t, cfg.MetricsIndex)
		enc := queueEncoding[pmetric.Metrics]{
			wrapPdata:      newPdataConverter(exp, exp.encodeMetricRecords, pmetric.Metrics.DataPointCount, (&pmetric.ProtoMarshaler{}).MetricsSize, pdatareq.MarshalMetrics),
			unmarshalCtx:   pdatareq.UnmarshalMetrics,
			unmarshalPlain: (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalMetrics, (&pmetric.ProtoMarshaler{}).MarshalMetrics, benchMetrics(3))
	})

	t.Run("traces", func(t *testing.T) {
		exp := newExp(t, cfg.TracesIndex)
		enc := queueEncoding[ptrace.Traces]{
			wrapPdata:      newPdataConverter(exp, exp.encodeTraceRecords, ptrace.Traces.SpanCount, (&ptrace.ProtoMarshaler{}).TracesSize, pdatareq.MarshalTraces),
			unmarshalCtx:   pdatareq.UnmarshalTraces,
			unmarshalPlain: (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces,
		}
		assertReadsLegacyPayload(t, enc, pdatareq.MarshalTraces, (&ptrace.ProtoMarshaler{}).MarshalTraces, benchTraces(3))
	})

	t.Run("profiles", func(t *testing.T) {
		exp := newExp(t, "")
		enc := queueEncoding[pprofile.Profiles]{
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

// TestRequestEncoding_RoundTripMergeFields verifies the persistent-queue wire
// format round-trips the merge metadata and deferred payloads.
func TestRequestEncoding_RoundTripMergeFields(t *testing.T) {
	enc := queueEncoding[pmetric.Metrics]{}
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

// TestRequestEncoding_DeferredMetricsMarshaledLazily: a deferred item holds
// pdata in memory and is proto-marshaled only when the persistent queue writes
// it; reading it back and consuming it produces the same document.
func TestRequestEncoding_DeferredMetricsMarshaledLazily(t *testing.T) {
	ts := time.Unix(1719000000, 0).UTC()
	enc := queueEncoding[pmetric.Metrics]{}

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
	enc := queueEncoding[pmetric.Metrics]{}
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
