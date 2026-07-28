// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
)

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
