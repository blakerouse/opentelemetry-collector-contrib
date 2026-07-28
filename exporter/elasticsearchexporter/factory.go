// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"compress/gzip"
	"context"
	"maps"
	"net/http"
	"slices"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	pdatareq "go.opentelemetry.io/collector/pdata/xpdata/request"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
)

// NewFactory creates a factory for Elastic exporter.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xexporter.WithLogs(createLogsExporter, metadata.LogsStability),
		xexporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		xexporter.WithTraces(createTracesExporter, metadata.TracesStability),
		xexporter.WithProfiles(createProfilesExporter, metadata.ProfilesStability),
	)
}

func createDefaultConfig() component.Config {
	qs := exporterhelper.NewDefaultQueueConfig()
	qs.QueueSize = 10
	qs.BlockOnOverflow = true
	qs.Batch = configoptional.Some(exporterhelper.BatchConfig{
		FlushTimeout: 10 * time.Second,
		MinSize:      1e+6,
		MaxSize:      5e+6,
		Sizer:        exporterhelper.RequestSizerTypeBytes,
	})

	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Timeout = 90 * time.Second
	httpClientConfig.Compression = configcompression.TypeGzip
	httpClientConfig.CompressionParams.Level = gzip.BestSpeed

	return &Config{
		QueueBatchConfig: configoptional.Some(qs),
		ClientConfig:     httpClientConfig,
		LogsDynamicID: DynamicIDSettings{
			Enabled: false,
		},
		LogsDynamicPipeline: DynamicPipelineSettings{
			Enabled: false,
		},
		Retry: RetrySettings{
			Enabled:         true,
			MaxRetries:      0, // default is set in exporter code
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     1 * time.Minute,
			RetryOnStatus: []int{
				http.StatusTooManyRequests,
			},
		},
		Mapping: MappingsSettings{
			Mode:         "otel",
			AllowedModes: slices.Sorted(maps.Keys(canonicalMappingModes)),
		},
		LogstashFormat: LogstashFormatSettings{
			Enabled:         false,
			PrefixSeparator: "-",
			DateFormat:      "%Y.%m.%d",
		},
		TelemetrySettings: TelemetrySettings{
			LogRequestBody:              false,
			LogResponseBody:             false,
			LogFailedDocsInput:          false,
			LogFailedDocsInputRateLimit: time.Second,
		},
		IncludeSourceOnError: nil,
	}
}

// Every signal follows the same request-based shape (NewXRequest +
// pushEncodedRequest). useEarlyEncoding (see request.go) only selects the
// ingest-time converter: newEncodedConverter serializes each record at ingest
// (and, with a persistent queue, stores the early-encoded format on disk),
// while newPdataConverter wraps the raw pdata so the persistent queue keeps
// the legacy pdata on-disk format and records are encoded on the consumer
// goroutine at drain. Draining always reads both formats.

// createLogsExporter creates a new exporter for logs.
//
// Logs are directly indexed into Elasticsearch.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	cf := cfg.(*Config)
	handleDeprecatedConfig(cf, set.Logger)
	handleTelemetryConfig(cf, set.Logger)

	exp, err := newExporter(cf, set, cf.LogsIndex)
	if err != nil {
		return nil, err
	}

	pdataConverter := newPdataConverter(exp, exp.encodeLogRecords,
		plog.Logs.LogRecordCount, (&plog.ProtoMarshaler{}).LogsSize, pdatareq.MarshalLogs)
	qbs := requestQueueBatchSettings(cf, pdataConverter, pref.RefLogs, pref.UnrefLogs,
		pdatareq.UnmarshalLogs, (&plog.ProtoUnmarshaler{}).UnmarshalLogs)
	converter := newEncodedConverter(exp, exp.encodeLogRecords)
	if !useEarlyEncoding(cf) {
		converter = pdataConverter
	}
	return xexporterhelper.NewLogsRequest(ctx, set, converter, exp.pushEncodedRequest,
		exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	cf := cfg.(*Config)
	handleDeprecatedConfig(cf, set.Logger)
	handleTelemetryConfig(cf, set.Logger)

	exp, err := newExporter(cf, set, cf.MetricsIndex)
	if err != nil {
		return nil, err
	}

	pdataConverter := newPdataConverter(exp, exp.encodeMetricRecords,
		pmetric.Metrics.DataPointCount, (&pmetric.ProtoMarshaler{}).MetricsSize, pdatareq.MarshalMetrics)
	qbs := requestQueueBatchSettings(cf, pdataConverter, pref.RefMetrics, pref.UnrefMetrics,
		pdatareq.UnmarshalMetrics, (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics)
	converter := newEncodedConverter(exp, exp.encodeMetricRecords)
	if !useEarlyEncoding(cf) {
		converter = pdataConverter
	}
	return xexporterhelper.NewMetricsRequest(ctx, set, converter, exp.pushEncodedRequest,
		exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
}

func createTracesExporter(ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	cf := cfg.(*Config)
	handleDeprecatedConfig(cf, set.Logger)
	handleTelemetryConfig(cf, set.Logger)

	exp, err := newExporter(cf, set, cf.TracesIndex)
	if err != nil {
		return nil, err
	}

	pdataConverter := newPdataConverter(exp, exp.encodeTraceRecords,
		ptrace.Traces.SpanCount, (&ptrace.ProtoMarshaler{}).TracesSize, pdatareq.MarshalTraces)
	qbs := requestQueueBatchSettings(cf, pdataConverter, pref.RefTraces, pref.UnrefTraces,
		pdatareq.UnmarshalTraces, (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces)
	converter := newEncodedConverter(exp, exp.encodeTraceRecords)
	if !useEarlyEncoding(cf) {
		converter = pdataConverter
	}
	return xexporterhelper.NewTracesRequest(ctx, set, converter, exp.pushEncodedRequest,
		exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
}

// createProfilesExporter creates a new exporter for profiles.
//
// Profiles are directly indexed into Elasticsearch.
func createProfilesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (xexporter.Profiles, error) {
	cf := cfg.(*Config)
	handleDeprecatedConfig(cf, set.Logger)
	handleTelemetryConfig(cf, set.Logger)

	exp, err := newExporter(cf, set, "")
	if err != nil {
		return nil, err
	}

	pdataConverter := newPdataConverter(exp, exp.encodeProfileRecords,
		pprofile.Profiles.SampleCount, (&pprofile.ProtoMarshaler{}).ProfilesSize, pdatareq.MarshalProfiles)
	qbs := requestQueueBatchSettings(cf, pdataConverter, pref.RefProfiles, pref.UnrefProfiles,
		pdatareq.UnmarshalProfiles, (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles)
	converter := newEncodedConverter(exp, exp.encodeProfileRecords)
	if !useEarlyEncoding(cf) {
		converter = pdataConverter
	}
	return xexporterhelper.NewProfilesRequest(ctx, set, converter, exp.pushEncodedRequest,
		exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
}

// requestQueueBatchSettings builds the QueueBatchSettings for the request
// pipeline: reference counting for requests that hold pdata, and — when a
// persistent queue is configured — the Encoding that writes both request
// formats and reads both back. pdataConverter wraps pdata payloads read from
// disk so the request keeps the sizing it was offered with.
//
// Upstream exporterhelper behaviors this exporter relies on; verify them when
// bumping exporterhelper:
//   - the batcher flushes over-maxSize requests returned by MergeSplit
//     immediately, retaining only the last (smallest) as the pending batch;
//   - the memory queue refs a request on Offer and unrefs it after consume,
//     while the persistent queue marshals on Offer and never holds requests;
//   - batching/merging happens after dequeue, so persisted requests always
//     round-trip through Encoding.Marshal/Unmarshal;
//   - the request-converter path wraps converter errors as permanent (see
//     unwrapPermanent).
func requestQueueBatchSettings[T any](
	cf *Config,
	pdataConverter xexporterhelper.RequestConverterFunc[T],
	ref, unref func(T),
	unmarshalCtx func([]byte) (context.Context, T, error),
	unmarshalPlain func([]byte) (T, error),
) xexporterhelper.QueueBatchSettings {
	var qbs xexporterhelper.QueueBatchSettings
	// The memory queue refs on Offer and unrefs after consume; requests can
	// hold pdata by reference (pdataRequest, deferred metric items).
	qbs.ReferenceCounter = pdataRefCounter[T]{
		ref: ref, unref: unref,
		refMetrics: pref.RefMetrics, unrefMetrics: pref.UnrefMetrics,
	}
	if cf.QueueBatchConfig.HasValue() && cf.QueueBatchConfig.Get().StorageID != nil {
		qbs.Encoding = queueEncoding[T]{
			wrapPdata:      pdataConverter,
			unmarshalCtx:   unmarshalCtx,
			unmarshalPlain: unmarshalPlain,
		}
	}
	applyMetadataPartitioner(cf, &qbs)
	return qbs
}

func applyMetadataPartitioner(cf *Config, qbs *xexporterhelper.QueueBatchSettings) {
	if len(cf.MetadataKeys) > 0 {
		partitioner := metadataKeysPartitioner{keys: cf.MetadataKeys}
		qbs.Partitioner = partitioner
		qbs.MergeCtx = partitioner.MergeCtx
	}
}

func exporterhelperOptions(
	cfg *Config,
	start component.StartFunc,
	shutdown component.ShutdownFunc,
	qbs xexporterhelper.QueueBatchSettings,
) []exporterhelper.Option {
	// not setting capabilities as they will default to non-mutating but will be updated
	// by the base-exporter to mutating if batching is enabled.
	return []exporterhelper.Option{
		exporterhelper.WithStart(start),
		exporterhelper.WithShutdown(shutdown),
		xexporterhelper.WithQueueBatch(cfg.QueueBatchConfig, qbs),
		// Effectively disable timeout_sender because timeout is enforced in bulk indexer.
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
	}
}
