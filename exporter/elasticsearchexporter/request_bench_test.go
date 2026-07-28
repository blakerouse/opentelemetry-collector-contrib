// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
)

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
//   - consumer_encode_and_send: encoding AND sending on the sending-queue
//     consumer goroutine, the pre-early-encoding cost baseline.
//   - consumer_request_send_only: the request path's consumer, which only sends
//     already-encoded bytes (the critical path early encoding aims to speed up).
//   - ingest_encode: the per-record encoding relocated to the ConsumeX caller.
//
// The honest total cost of the request path is ingest_encode + send_only.
func benchmarkSignalExporter[T any](
	b *testing.B,
	exp *elasticsearchExporter,
	data T,
	encode recordEncoder[T],
) {
	ctx := context.Background()
	converter := newEncodedConverter(exp, encode)
	preEncoded, err := converter(ctx, data)
	require.NoError(b, err)

	b.Run("consumer_encode_and_send", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			req, err := converter(ctx, data)
			if err != nil {
				b.Fatal(err)
			}
			if err := exp.pushEncodedRequest(ctx, req); err != nil {
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

// BenchmarkLogsExporter compares where the encoding work lands. The key result
// is that the sending-queue consumer's critical path is much cheaper when it
// only sends (consumer_request_send_only) than when it also encodes
// (consumer_encode_and_send), because per-record JSON serialization has moved
// to ingest time (ingest_encode), which runs on the ConsumeLogs caller
// goroutines instead of the consumer goroutine.
func BenchmarkLogsExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(c *Config) string { return c.LogsIndex })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchLogs(n), exp.encodeLogRecords)
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
			benchmarkSignalExporter(b, exp, benchMetrics(n), exp.encodeMetricRecords)
		})
	}
}

func BenchmarkTracesExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(c *Config) string { return c.TracesIndex })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchTraces(n), exp.encodeTraceRecords)
		})
	}
}

func BenchmarkProfilesExporter(b *testing.B) {
	exp := startBenchExporter(b, newBenchESServer(b, nil).URL, func(*Config) string { return "" })
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("profiles=%d", n), func(b *testing.B) {
			benchmarkSignalExporter(b, exp, benchProfiles(n), exp.encodeProfileRecords)
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
//   - pdata_request holds the live plog.Logs object graph, what a pdata
//     in-memory queue keeps.
//   - pdata_proto_bytes holds the marshaled protobuf, what a pdata
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
	encoding := queueEncoding[plog.Logs]{wrapPdata: newPdataLogsConverter(exp)}
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

	b.Run("pdata_request", func(b *testing.B) {
		retained(b, func() any { return benchLogs(nRecords) })
	})

	b.Run("pdata_proto_bytes", func(b *testing.B) {
		retained(b, func() any {
			data, err := protoMarshaler.MarshalLogs(benchLogs(nRecords))
			if err != nil {
				b.Fatal(err)
			}
			return data
		})
	})
}

// newConcurrentLogsExporter builds a fully wired logs exporter that encodes
// either at ingest (on the ConsumeLogs caller goroutines, the request path) or
// on the sending-queue consumer goroutines (the pre-early-encoding baseline,
// reconstructed via a pdata queue whose push converts and sends). To make the
// placement of the encoding work the dominant factor, batching is disabled,
// compression is turned off (so the consumer's flush is cheap I/O rather than
// CPU-bound gzip), and the number of queue consumers is bounded.
func newConcurrentLogsExporter(tb testing.TB, encodeOnIngest bool, url string, numConsumers int) exporter.Logs {
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
	if encodeOnIngest {
		logsExp, err = xexporterhelper.NewLogsRequest(
			context.Background(), set,
			newEncodedConverter(esExp, esExp.encodeLogRecords), esExp.pushEncodedRequest,
			exporterhelperOptions(cfg, esExp.Start, esExp.Shutdown, xexporterhelper.QueueBatchSettings{})...,
		)
	} else {
		converter := newEncodedConverter(esExp, esExp.encodeLogRecords)
		encodeOnConsumer := func(ctx context.Context, ld plog.Logs) error {
			req, err := converter(ctx, ld)
			if err != nil {
				return err
			}
			return esExp.pushEncodedRequest(ctx, req)
		}
		logsExp, err = exporterhelper.NewLogs(
			context.Background(), set, cfg, encodeOnConsumer,
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
// Encoding at ingest scales with the (many) caller goroutines; encoding on the
// (few) consumer goroutines makes them the bottleneck. The "logs/s" metric for
// encode_on_ingest should exceed the one for encode_on_consumer.
func BenchmarkLogsExporterThroughput(b *testing.B) {
	const (
		recordsPerCall = 100
		numConsumers   = 2
	)
	ctx := context.Background()
	logs := benchLogs(recordsPerCall)

	for _, tc := range []struct {
		name           string
		encodeOnIngest bool
	}{
		{name: "encode_on_consumer", encodeOnIngest: false},
		{name: "encode_on_ingest", encodeOnIngest: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var flushed atomic.Int64
			server := newBenchESServer(b, &flushed)
			exp := newConcurrentLogsExporter(b, tc.encodeOnIngest, server.URL, numConsumers)

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
//   - pdata_proto_size mirrors pdataRequest.BytesSize(), which
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

		b.Run(fmt.Sprintf("records=%d/pdata_proto_size", nRecords), func(b *testing.B) {
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
