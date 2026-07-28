// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
)

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
	enc := queueEncoding[pmetric.Metrics]{}
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
