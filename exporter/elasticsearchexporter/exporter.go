// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/elastic/go-docappender/v2"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/datapoints"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metricgroup"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/pool"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
)

type elasticsearchExporter struct {
	set                 exporter.Settings
	config              *Config
	index               string
	logstashFormat      LogstashFormatSettings
	defaultMappingMode  MappingMode
	allowedMappingModes map[string]MappingMode
	bulkIndexers        bulkIndexers
	bufferPool          *pool.BufferPool

	documentEncoders         [NumMappingModes]documentEncoder
	documentRouters          [NumMappingModes]documentRouter
	spanEventDocumentRouters [NumMappingModes]documentRouter

	telemetryBuilder *metadata.TelemetryBuilder
}

func newExporter(cfg *Config, set exporter.Settings, index string) (*elasticsearchExporter, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize internal telemetry: %w", err)
	}

	allowedMappingModes := cfg.allowedMappingModes()
	defaultMappingMode := MappingOTel

	if _, ok := allowedMappingModes[MappingOTel.String()]; !ok && len(cfg.Mapping.AllowedModes) > 0 {
		defaultMappingMode = allowedMappingModes[canonicalMappingModeName(cfg.Mapping.AllowedModes[0])]
	}

	exporter := &elasticsearchExporter{
		set:                 set,
		config:              cfg,
		index:               index,
		logstashFormat:      cfg.LogstashFormat,
		allowedMappingModes: allowedMappingModes,
		defaultMappingMode:  defaultMappingMode,
		bufferPool:          pool.NewBufferPool(),
		bulkIndexers:        bulkIndexers{telemetryBuilder: telemetryBuilder},
		telemetryBuilder:    telemetryBuilder,
	}
	for mappingMode := range NumMappingModes {
		encoder, err := newEncoder(mappingMode)
		if err != nil {
			return nil, err
		}
		exporter.documentEncoders[mappingMode] = encoder
		exporter.documentRouters[mappingMode] = newDocumentRouter(mappingMode, index, cfg)
		exporter.spanEventDocumentRouters[mappingMode] = newDocumentRouter(mappingMode, cfg.LogsIndex, cfg)
	}
	return exporter, nil
}

func (e *elasticsearchExporter) Start(ctx context.Context, host component.Host) error {
	if err := e.bulkIndexers.start(ctx, e.config, e.set, host, e.allowedMappingModes); err != nil {
		return fmt.Errorf("error starting bulk indexers: %w", err)
	}
	return nil
}

func (e *elasticsearchExporter) Shutdown(ctx context.Context) error {
	if err := e.bulkIndexers.shutdown(ctx); err != nil {
		return fmt.Errorf("error shutting down bulk indexers: %w", err)
	}
	if e.telemetryBuilder != nil {
		e.telemetryBuilder.Shutdown()
		e.telemetryBuilder = nil
	}
	return nil
}

// The push*Data functions are the legacy (consumer-goroutine) path: they stream
// each record to bulk indexer sessions via a sessionSink and flush. The
// encode*Records functions are the early (ingest-goroutine) path: they collect
// each record into []encodedItem via an itemSink for the request queue. Both are
// thin wrappers over pushStreaming/collectItems and the per-signal emit*
// iteration, so document encoding lives in one place; only the sink differs.

// pushStreaming runs a per-signal emit against streaming bulk indexer sessions
// (the legacy path) and flushes. emit is a bound *elasticsearchExporter method.
func pushStreaming[T any](
	ctx context.Context,
	e *elasticsearchExporter,
	emit func(context.Context, docSink, T) ([]error, error),
	data T,
) error {
	var sessions encodedSessionSet
	sessions.init(e)
	defer sessions.end()
	errs, err := emit(ctx, sessionSink{sessions: &sessions}, data)
	if err != nil {
		return err
	}
	return flushSessions(ctx, &sessions, errs)
}

// collectItems runs a per-signal emit against a buffering itemSink (the early
// path), returning the materialized items. capacity is a preallocation hint.
func collectItems[T any](
	ctx context.Context,
	emit func(context.Context, docSink, T) ([]error, error),
	data T,
	capacity int,
) ([]encodedItem, []error, error) {
	sink := &itemSink{items: make([]encodedItem, 0, capacity)}
	perRecordErrs, err := emit(ctx, sink, data)
	if err != nil {
		return nil, nil, err
	}
	return sink.items, perRecordErrs, nil
}

func (e *elasticsearchExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	return pushStreaming(ctx, e, e.emitLogs, ld)
}

func (e *elasticsearchExporter) encodeLogRecords(ctx context.Context, ld plog.Logs) ([]encodedItem, []error, error) {
	return collectItems(ctx, e.emitLogs, ld, ld.LogRecordCount())
}

// emitLogs iterates ld, encoding each log record and handing it to sink.
// perRecordErrs are deterministic per-record errors; see recordEncoder for how
// each path handles them. A returned error aborts the whole batch.
func (e *elasticsearchExporter) emitLogs(ctx context.Context, sink docSink, ld plog.Logs) ([]error, error) {
	defaultMappingMode, err := e.getRequestMappingMode(ctx)
	if err != nil {
		return nil, err
	}
	var perRecordErrs []error
	for _, rl := range ld.ResourceLogs().All() {
		resource := rl.Resource()
		for _, ill := range rl.ScopeLogs().All() {
			scope := ill.Scope()
			mappingMode, err := e.getScopeMappingMode(scope, defaultMappingMode)
			if err != nil {
				return nil, err
			}
			router := e.documentRouters[int(mappingMode)]
			encoder := e.documentEncoders[int(mappingMode)]
			ec := encodingContext{
				resource:          resource,
				resourceSchemaURL: rl.SchemaUrl(),
				scope:             scope,
				scopeSchemaURL:    ill.SchemaUrl(),
			}
			for _, lr := range ill.LogRecords().All() {
				if err := e.emitLogRecord(ctx, sink, router, encoder, ec, lr, mappingMode); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return nil, cerr
					}
					if errors.Is(err, ErrInvalidTypeForBodyMapMode) {
						e.set.Logger.Warn("dropping log record", zap.Error(err))
						continue
					}
					perRecordErrs = append(perRecordErrs, err)
				}
			}
		}
	}
	return perRecordErrs, nil
}

func (e *elasticsearchExporter) emitLogRecord(
	ctx context.Context,
	sink docSink,
	router documentRouter,
	encoder documentEncoder,
	ec encodingContext,
	record plog.LogRecord,
	mappingMode MappingMode,
) error {
	ctrl := extractControlAttrs(record.Attributes(), e.config.LogsDynamicID.Enabled, e.config.LogsDynamicPipeline.Enabled)
	if ctrl.noindex {
		return nil
	}
	index, err := router.routeLogRecord(ec.resource, ec.scope, record.Attributes())
	if err != nil {
		return err
	}
	buf := e.bufferPool.NewPooledBuffer()
	if err := encoder.encodeLog(ec, record, index, buf.Buffer); err != nil {
		buf.Recycle()
		return fmt.Errorf("failed to encode log event: %w", err)
	}
	return sink.add(ctx, encodedItem{
		index:       index.Index,
		docID:       ctrl.docID,
		pipeline:    ctrl.pipeline,
		action:      docappender.ActionCreate,
		mappingMode: mappingMode,
		target:      targetDefault,
	}, pooledDoc(buf))
}

// flushSessions flushes all bulk indexer sessions started while streaming and
// joins the flush error into errs, honoring context cancellation. It is the
// shared tail of the legacy push*Data consumers.
func flushSessions(ctx context.Context, sessions *encodedSessionSet, errs []error) error {
	if err := sessions.flush(ctx); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

type dataPointsGroup struct {
	resource          pcommon.Resource
	resourceSchemaURL string
	scope             pcommon.InstrumentationScope
	scopeSchemaURL    string
	dataPoints        []datapoints.DataPoint
}

func (p *dataPointsGroup) addDataPoint(dp datapoints.DataPoint) {
	p.dataPoints = append(p.dataPoints, dp)
}

func (e *elasticsearchExporter) pushMetricsData(ctx context.Context, metrics pmetric.Metrics) error {
	return pushStreaming(ctx, e, e.emitMetrics, metrics)
}

func (e *elasticsearchExporter) encodeMetricRecords(ctx context.Context, metrics pmetric.Metrics) ([]encodedItem, []error, error) {
	// The document count is the number of data point groups, not known until
	// grouping completes inside emitMetrics, so no useful preallocation hint here.
	return collectItems(ctx, e.emitMetrics, metrics, 0)
}

// emitMetrics groups data points into documents, then encodes each group and
// hands it to sink. Validation errors are logged (not returned) so upstream does
// not retry; other per-record errors are returned via perRecordErrs.
func (e *elasticsearchExporter) emitMetrics(ctx context.Context, sink docSink, metrics pmetric.Metrics) ([]error, error) {
	defaultMappingMode, err := e.getRequestMappingMode(ctx)
	if err != nil {
		return nil, err
	}

	type mappingIndexKey struct {
		mappingMode MappingMode
		index       elasticsearch.Index
	}

	// Maintain a 2 layer map to avoid storing lots of copies of index strings
	groupedDataPointsByIndex := make(map[mappingIndexKey]map[metricgroup.HashKey]*dataPointsGroup)

	var validationErrs []error // log instead of returning these so that upstream does not retry
	var perRecordErrs []error
	for _, resourceMetrics := range metrics.ResourceMetrics().All() {
		resource := resourceMetrics.Resource()
		var hasher metricgroup.DataPointHasher
		var prevScopeMappingMode MappingMode
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics().All() {
			scope := scopeMetrics.Scope()
			mappingMode, err := e.getScopeMappingMode(scope, defaultMappingMode)
			if err != nil {
				return nil, err
			}
			router := e.documentRouters[int(mappingMode)]
			if hasher == nil || mappingMode != prevScopeMappingMode {
				hasher = newDataPointHasher(mappingMode)
				hasher.UpdateResource(resource)
			}
			prevScopeMappingMode = mappingMode

			hasher.UpdateScope(scope)
			for _, metric := range scopeMetrics.Metrics().All() {
				upsertDataPoint := func(dp datapoints.DataPoint) error {
					if dp.HasMappingHint(elasticsearch.HintNoIndex) {
						return nil
					}
					index, err := router.routeDataPoint(resource, scope, dp.Attributes())
					if err != nil {
						return err
					}
					key := mappingIndexKey{
						mappingMode: mappingMode,
						index:       index,
					}
					groupedDataPoints, ok := groupedDataPointsByIndex[key]
					if !ok {
						groupedDataPoints = make(map[metricgroup.HashKey]*dataPointsGroup)
						groupedDataPointsByIndex[key] = groupedDataPoints
					}
					hasher.UpdateDataPoint(dp)
					hashKey := hasher.HashKey()

					if dpGroup, ok := groupedDataPoints[hashKey]; !ok {
						groupedDataPoints[hashKey] = &dataPointsGroup{
							resource:          resource,
							resourceSchemaURL: resourceMetrics.SchemaUrl(),
							scope:             scope,
							scopeSchemaURL:    scopeMetrics.SchemaUrl(),
							dataPoints:        []datapoints.DataPoint{dp},
						}
					} else {
						dpGroup.addDataPoint(dp)
					}
					return nil
				}

				switch metric.Type() {
				case pmetric.MetricTypeSum:
					for _, dp := range metric.Sum().DataPoints().All() {
						if err := upsertDataPoint(datapoints.NewNumber(metric, dp)); err != nil {
							validationErrs = append(validationErrs, err)
							continue
						}
					}
				case pmetric.MetricTypeGauge:
					for _, dp := range metric.Gauge().DataPoints().All() {
						if err := upsertDataPoint(datapoints.NewNumber(metric, dp)); err != nil {
							validationErrs = append(validationErrs, err)
							continue
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					if metric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative {
						validationErrs = append(validationErrs, fmt.Errorf("dropping cumulative temporality exponential histogram %q", metric.Name()))
						continue
					}
					for _, dp := range metric.ExponentialHistogram().DataPoints().All() {
						if err := upsertDataPoint(datapoints.NewExponentialHistogram(metric, dp)); err != nil {
							validationErrs = append(validationErrs, err)
							continue
						}
					}
				case pmetric.MetricTypeHistogram:
					if metric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative {
						validationErrs = append(validationErrs, fmt.Errorf("dropping cumulative temporality histogram %q", metric.Name()))
						continue
					}
					for _, dp := range metric.Histogram().DataPoints().All() {
						if err := upsertDataPoint(datapoints.NewHistogram(metric, dp)); err != nil {
							validationErrs = append(validationErrs, err)
							continue
						}
					}
				case pmetric.MetricTypeSummary:
					for _, dp := range metric.Summary().DataPoints().All() {
						if err := upsertDataPoint(datapoints.NewSummary(metric, dp)); err != nil {
							validationErrs = append(validationErrs, err)
							continue
						}
					}
				}
			}
		}
	}

	for key, groupedDataPoints := range groupedDataPointsByIndex {
		for _, dpGroup := range groupedDataPoints {
			buf := e.bufferPool.NewPooledBuffer()
			encoder := e.documentEncoders[int(key.mappingMode)]
			dynamicTemplates, err := encoder.encodeMetrics(
				encodingContext{
					resource:          dpGroup.resource,
					resourceSchemaURL: dpGroup.resourceSchemaURL,
					scope:             dpGroup.scope,
					scopeSchemaURL:    dpGroup.scopeSchemaURL,
				},
				dpGroup.dataPoints,
				&validationErrs,
				key.index,
				buf.Buffer,
			)
			if err != nil {
				buf.Recycle()
				perRecordErrs = append(perRecordErrs, err)
				continue
			}
			if err := sink.add(ctx, encodedItem{
				index:            key.index.Index,
				action:           docappender.ActionCreate,
				dynamicTemplates: dynamicTemplates,
				mappingMode:      key.mappingMode,
				target:           targetDefault,
			}, pooledDoc(buf)); err != nil {
				if cerr := ctx.Err(); cerr != nil {
					return nil, cerr
				}
				perRecordErrs = append(perRecordErrs, err)
			}
		}
	}
	if len(validationErrs) > 0 {
		e.set.Logger.Warn("validation errors", zap.Error(errors.Join(validationErrs...)))
	}
	return perRecordErrs, nil
}

func (e *elasticsearchExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	return pushStreaming(ctx, e, e.emitTraces, td)
}

func (e *elasticsearchExporter) encodeTraceRecords(ctx context.Context, td ptrace.Traces) ([]encodedItem, []error, error) {
	// SpanCount is a lower bound (span events add more items) but a good hint.
	return collectItems(ctx, e.emitTraces, td, td.SpanCount())
}

// emitTraces iterates td, encoding each span and its span events and handing each
// to sink.
func (e *elasticsearchExporter) emitTraces(ctx context.Context, sink docSink, td ptrace.Traces) ([]error, error) {
	defaultMappingMode, err := e.getRequestMappingMode(ctx)
	if err != nil {
		return nil, err
	}
	var perRecordErrs []error
	for _, il := range td.ResourceSpans().All() {
		resource := il.Resource()
		for _, scopeSpan := range il.ScopeSpans().All() {
			scope := scopeSpan.Scope()
			mappingMode, err := e.getScopeMappingMode(scope, defaultMappingMode)
			if err != nil {
				return nil, err
			}
			router := e.documentRouters[int(mappingMode)]
			spanEventRouter := e.spanEventDocumentRouters[int(mappingMode)]
			encoder := e.documentEncoders[int(mappingMode)]
			ec := encodingContext{
				resource:          resource,
				resourceSchemaURL: il.SchemaUrl(),
				scope:             scope,
				scopeSchemaURL:    scopeSpan.SchemaUrl(),
			}
			for _, span := range scopeSpan.Spans().All() {
				if err := e.emitSpan(ctx, sink, router, encoder, ec, span, mappingMode); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return nil, cerr
					}
					perRecordErrs = append(perRecordErrs, err)
				}
				for _, spanEvent := range span.Events().All() {
					if err := e.emitSpanEvent(ctx, sink, spanEventRouter, encoder, ec, span, spanEvent, mappingMode); err != nil {
						perRecordErrs = append(perRecordErrs, err)
					}
				}
			}
		}
	}
	return perRecordErrs, nil
}

func (e *elasticsearchExporter) emitSpan(
	ctx context.Context,
	sink docSink,
	router documentRouter,
	encoder documentEncoder,
	ec encodingContext,
	span ptrace.Span,
	mappingMode MappingMode,
) error {
	ctrl := extractControlAttrs(span.Attributes(), e.config.TracesDynamicID.Enabled, false)
	if ctrl.noindex {
		return nil
	}
	index, err := router.routeSpan(ec.resource, ec.scope, span.Attributes())
	if err != nil {
		return err
	}
	buf := e.bufferPool.NewPooledBuffer()
	if err := encoder.encodeSpan(ec, span, index, buf.Buffer); err != nil {
		buf.Recycle()
		return fmt.Errorf("failed to encode trace record: %w", err)
	}
	return sink.add(ctx, encodedItem{
		index:       index.Index,
		docID:       ctrl.docID,
		action:      docappender.ActionCreate,
		mappingMode: mappingMode,
		target:      targetDefault,
	}, pooledDoc(buf))
}

func (e *elasticsearchExporter) emitSpanEvent(
	ctx context.Context,
	sink docSink,
	router documentRouter,
	encoder documentEncoder,
	ec encodingContext,
	span ptrace.Span,
	spanEvent ptrace.SpanEvent,
	mappingMode MappingMode,
) error {
	ctrl := extractControlAttrs(spanEvent.Attributes(), e.config.TracesDynamicID.Enabled, false)
	if ctrl.noindex {
		return nil
	}
	routerIndex, err := router.routeSpanEvent(ec.resource, ec.scope, spanEvent.Attributes())
	if err != nil {
		return err
	}
	buf := e.bufferPool.NewPooledBuffer()
	index, err := encoder.encodeSpanEvent(ec, span, spanEvent, routerIndex, buf.Buffer)
	if err != nil || buf.Buffer.Len() == 0 {
		buf.Recycle()
		return err
	}
	return sink.add(ctx, encodedItem{
		index:       index.Index,
		docID:       ctrl.docID,
		action:      docappender.ActionCreate,
		mappingMode: mappingMode,
		target:      targetDefault,
	}, pooledDoc(buf))
}

// controlAttrs holds the values of control-channel attributes the orchestrator
// needs before encoding a doc: whether to skip indexing entirely (_noindex
// mapping hint), the dynamic document ID, and the dynamic ingest pipeline.
type controlAttrs struct {
	noindex  bool
	docID    string
	pipeline string
}

// extractControlAttrs walks attrs once, collecting every control-channel value
// the push functions would otherwise read via separate pcommon.Map.Get calls.
// captureDocID and capturePipeline mirror the existing config gates
// (Logs/TracesDynamicID.Enabled, LogsDynamicPipeline.Enabled): when false, the
// corresponding value is left empty even if the attribute is present.
func extractControlAttrs(attrs pcommon.Map, captureDocID, capturePipeline bool) controlAttrs {
	var c controlAttrs
	remaining := 1 // MappingHintsAttrKey is always a candidate
	if captureDocID {
		remaining++
	}
	if capturePipeline {
		remaining++
	}
	for k, v := range attrs.All() {
		switch k {
		case elasticsearch.MappingHintsAttrKey:
			remaining--
			if v.Type() != pcommon.ValueTypeSlice {
				break
			}
			for _, h := range v.Slice().All() {
				if h.Str() == string(elasticsearch.HintNoIndex) {
					c.noindex = true
					// If _noindex is specified, nothing else matters.
					return c
				}
			}
		case elasticsearch.DocumentIDAttributeName:
			if captureDocID {
				remaining--
				c.docID = v.AsString()
			}
		case elasticsearch.DocumentPipelineAttributeName:
			if capturePipeline {
				remaining--
				c.pipeline = v.AsString()
			}
		}
		if remaining == 0 {
			break
		}
	}
	return c
}

func (e *elasticsearchExporter) pushProfilesData(ctx context.Context, pd pprofile.Profiles) error {
	return pushStreaming(ctx, e, e.emitProfiles, pd)
}

func (e *elasticsearchExporter) encodeProfileRecords(ctx context.Context, pd pprofile.Profiles) ([]encodedItem, []error, error) {
	// A profile fans out into a variable number of documents, so there is no
	// cheap, accurate preallocation hint.
	return collectItems(ctx, e.emitProfiles, pd, 0)
}

// emitProfiles iterates pd, encoding each profile. A single profile fans out into
// multiple documents (stack traces, frames, events, executables, …), each routed
// to its own bulk indexer via the item's sessionTarget; the encoder drives this
// through the callback.
func (e *elasticsearchExporter) emitProfiles(ctx context.Context, sink docSink, pd pprofile.Profiles) ([]error, error) {
	// TODO add support for routing profiles to different data_stream.namespaces?
	defaultMappingMode, err := e.getRequestMappingMode(ctx)
	if err != nil {
		return nil, err
	}
	dic := pd.Dictionary()
	var perRecordErrs []error
	for _, rp := range pd.ResourceProfiles().All() {
		resource := rp.Resource()
		for _, sp := range rp.ScopeProfiles().All() {
			scope := sp.Scope()
			mappingMode, err := e.getScopeMappingMode(scope, defaultMappingMode)
			if err != nil {
				return nil, err
			}
			encoder := e.documentEncoders[int(mappingMode)]
			for _, profile := range sp.Profiles().All() {
				ec := encodingContext{
					resource:          resource,
					resourceSchemaURL: rp.SchemaUrl(),
					scope:             scope,
					scopeSchemaURL:    sp.SchemaUrl(),
				}
				err := encoder.encodeProfile(ec, dic, profile, func(buf *bytes.Buffer, docID, index string) error {
					target, action := profileIndexTarget(index)
					return sink.add(ctx, encodedItem{
						index:       index,
						docID:       docID,
						action:      action,
						mappingMode: mappingMode,
						target:      target,
					}, rawDoc(buf))
				})
				if err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return nil, cerr
					}
					if errors.Is(err, ErrInvalidTypeForBodyMapMode) {
						e.set.Logger.Warn("dropping profile record", zap.Error(err))
						continue
					}
					perRecordErrs = append(perRecordErrs, err)
				}
			}
		}
	}
	return perRecordErrs, nil
}

// profileIndexTarget maps a profiling document's target index to the bulk
// indexer session it should be written to and the bulk action to use. It mirrors
// the routing in pushProfileRecord.
func profileIndexTarget(index string) (sessionTarget, string) {
	switch index {
	case otelserializer.StackTraceIndex:
		return targetProfilingStackTraces, docappender.ActionCreate
	case otelserializer.StackFrameIndex:
		return targetProfilingStackFrames, docappender.ActionCreate
	case otelserializer.AllEventsIndex:
		return targetProfilingEvents, docappender.ActionCreate
	case otelserializer.ExecutablesIndex:
		return targetProfilingExecutables, docappender.ActionUpdate
	case otelserializer.ExecutablesSymQueueIndex,
		otelserializer.LeafFramesSymQueueIndex,
		otelserializer.HostsMetadataIndex:
		// These regular indices have a low write-frequency and can share the
		// executables session.
		return targetProfilingExecutables, docappender.ActionCreate
	default:
		return targetDefault, docappender.ActionCreate
	}
}

// mappingModeSessions holds mapping-mode specific bulk indexer sessions.
type mappingModeSessions struct {
	indexers *[NumMappingModes]bulkIndexer
	sessions [NumMappingModes]bulkIndexerSession
	sessionList
}

// StartSession starts a new session for the given mapping mode if one has
// not yet been started, otherwise it returns the existing session.
//
// Note: this is not safe for concurrent use. It is expected to be used
// within a single Consume* call.
func (s *mappingModeSessions) StartSession(ctx context.Context, mappingMode MappingMode) bulkIndexerSession {
	if session := s.sessions[int(mappingMode)]; session != nil {
		return session
	}
	session := s.indexers[int(mappingMode)].StartSession(ctx)
	s.sessions[mappingMode] = session
	s.sessionList = append(s.sessionList, session)
	return session
}

// sessionList holds a list of bulkIndexerSession instances.
//
// This provides Flush and End methods that flush/end all sessions in the list.
type sessionList []bulkIndexerSession

// Flush concurrently flushes all sessions.
func (sessions *sessionList) Flush(ctx context.Context) error {
	var g errgroup.Group
	for _, session := range *sessions {
		g.Go(func() error {
			return session.Flush(ctx)
		})
	}
	return g.Wait()
}

// End ends all sessions.
func (sessions *sessionList) End() {
	for _, session := range *sessions {
		session.End()
	}
}

func (e *elasticsearchExporter) getRequestMappingMode(ctx context.Context) (MappingMode, error) {
	const metadataKey = "x-elastic-mapping-mode"

	values := client.FromContext(ctx).Metadata.Get(metadataKey)
	switch n := len(values); n {
	case 0:
		return e.defaultMappingMode, nil
	case 1:
		mode, err := e.parseMappingMode(values[0])
		if err != nil {
			return -1, consumererror.NewPermanent(fmt.Errorf("invalid context mapping mode: %w", err))
		}
		return mode, nil

	default:
		return -1, consumererror.NewPermanent(fmt.Errorf("expected one value for client metadata key %q, got %d", metadataKey, n))
	}
}

func (e *elasticsearchExporter) getScopeMappingMode(
	scope pcommon.InstrumentationScope, defaultMode MappingMode,
) (MappingMode, error) {
	attr, ok := scope.Attributes().Get(elasticsearch.MappingModeAttributeName)
	if !ok {
		return defaultMode, nil
	}
	mode, err := e.parseMappingMode(attr.AsString())
	if err != nil {
		return -1, consumererror.NewPermanent(fmt.Errorf("invalid scope mapping mode: %w", err))
	}
	return mode, nil
}

func (e *elasticsearchExporter) parseMappingMode(s string) (MappingMode, error) {
	mode, ok := e.allowedMappingModes[canonicalMappingModeName(s)]
	if !ok {
		return -1, fmt.Errorf(
			"unsupported mapping mode %q, expected one of %q",
			s, e.config.Mapping.AllowedModes,
		)
	}
	return mode, nil
}

func newDataPointHasher(mode MappingMode) metricgroup.DataPointHasher {
	switch mode {
	case MappingOTel:
		return &metricgroup.OTelDataPointHasher{}
	default:
		// Defaults to ECS for backward compatibility
		return &metricgroup.ECSDataPointHasher{}
	}
}
