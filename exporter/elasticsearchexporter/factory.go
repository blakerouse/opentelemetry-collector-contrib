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

// Every signal follows the same shape: when useEarlyEncoding is true (the
// default; see encoded.go) each record is serialized at ingest via the
// request-based API (NewXRequest + newEncodedConverter + pushEncodedRequest);
// otherwise the exporter falls back to the legacy pdata path (NewX + pushXData),
// which serializes on the sending-queue consumer and stores pdata in the queue.

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

	if useEarlyEncoding(cf) {
		converter := newEncodedConverter(exp, exp.encodeLogRecords)
		qbs := earlyEncodingQueueBatchSettings(cf, converter, pdatareq.UnmarshalLogs, (&plog.ProtoUnmarshaler{}).UnmarshalLogs)
		return xexporterhelper.NewLogsRequest(ctx, set, converter, exp.pushEncodedRequest,
			exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
	}
	qbs := legacyQueueBatchSettings(cf, xexporterhelper.NewLogsQueueBatchSettings())
	return exporterhelper.NewLogs(ctx, set, cfg, exp.pushLogsData,
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

	if useEarlyEncoding(cf) {
		converter := newEncodedConverter(exp, exp.encodeMetricRecords)
		qbs := earlyEncodingQueueBatchSettings(cf, converter, pdatareq.UnmarshalMetrics, (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics)
		return xexporterhelper.NewMetricsRequest(ctx, set, converter, exp.pushEncodedRequest,
			exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
	}
	qbs := legacyQueueBatchSettings(cf, xexporterhelper.NewMetricsQueueBatchSettings())
	return exporterhelper.NewMetrics(ctx, set, cfg, exp.pushMetricsData,
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

	if useEarlyEncoding(cf) {
		converter := newEncodedConverter(exp, exp.encodeTraceRecords)
		qbs := earlyEncodingQueueBatchSettings(cf, converter, pdatareq.UnmarshalTraces, (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces)
		return xexporterhelper.NewTracesRequest(ctx, set, converter, exp.pushEncodedRequest,
			exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
	}
	qbs := legacyQueueBatchSettings(cf, xexporterhelper.NewTracesQueueBatchSettings())
	return exporterhelper.NewTraces(ctx, set, cfg, exp.pushTraceData,
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

	if useEarlyEncoding(cf) {
		converter := newEncodedConverter(exp, exp.encodeProfileRecords)
		qbs := earlyEncodingQueueBatchSettings(cf, converter, pdatareq.UnmarshalProfiles, (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles)
		return xexporterhelper.NewProfilesRequest(ctx, set, converter, exp.pushEncodedRequest,
			exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
	}
	qbs := legacyQueueBatchSettings(cf, xexporterhelper.NewProfilesQueueBatchSettings())
	return xexporterhelper.NewProfiles(ctx, set, cfg, exp.pushProfilesData,
		exporterhelperOptions(cf, exp.Start, exp.Shutdown, qbs)...)
}

// earlyEncodingQueueBatchSettings builds the QueueBatchSettings for the
// request-based (early encoding) path. When a persistent sending queue is
// configured it installs the custom Encoding so requests can be marshaled to
// disk (and legacy pdata payloads still read back); see encoded.go.
func earlyEncodingQueueBatchSettings[T any](
	cf *Config,
	converter xexporterhelper.RequestConverterFunc[T],
	unmarshalCtx func([]byte) (context.Context, T, error),
	unmarshalPlain func([]byte) (T, error),
) xexporterhelper.QueueBatchSettings {
	var qbs xexporterhelper.QueueBatchSettings
	if cf.QueueBatchConfig.HasValue() && cf.QueueBatchConfig.Get().StorageID != nil {
		qbs.Encoding = encodedEncoding[T]{
			convert:        converter,
			unmarshalCtx:   unmarshalCtx,
			unmarshalPlain: unmarshalPlain,
		}
	}
	applyMetadataPartitioner(cf, &qbs)
	return qbs
}

// legacyQueueBatchSettings augments the signal's built-in pdata QueueBatchSettings
// with metadata_keys partitioning if configured.
func legacyQueueBatchSettings(cf *Config, qbs xexporterhelper.QueueBatchSettings) xexporterhelper.QueueBatchSettings {
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
