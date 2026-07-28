// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
)

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

// TestOversizedDocumentDelivered_EndToEnd guards the reliance on the upstream
// batcher flushing over-maxSize requests instead of dropping them: a document
// larger than batch max_size, pushed through a factory-built exporter with the
// queue and batcher enabled, must still be delivered. If this fails after an
// exporterhelper upgrade, the batcher contract changed; see splitByBytes.
func TestOversizedDocumentDelivered_EndToEnd(t *testing.T) {
	rec := newBulkRecorder()
	server := newESTestServer(t, func(docs []itemRequest) ([]itemResponse, error) {
		rec.Record(docs)
		return itemsAllOK(docs)
	})

	cfg := withDefaultConfig(func(cfg *Config) {
		cfg.Endpoints = []string{server.URL}
		qc := cfg.QueueBatchConfig.Get()
		qc.NumConsumers = 1
		batch := qc.Batch.Get()
		batch.FlushTimeout = 10 * time.Millisecond
		batch.MinSize = 100
		batch.MaxSize = 200
	})

	f := NewFactory()
	exp, err := f.CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })

	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(strings.Repeat("x", 1024)) // encodes far beyond MaxSize

	require.NoError(t, exp.ConsumeLogs(context.Background(), logs))
	docs := rec.WaitItems(1)
	require.Len(t, docs, 1)
	require.Greater(t, len(docs[0].Document), 200)
}
