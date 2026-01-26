// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package reloadreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/reloadreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/reloadreceiver/internal/metadata"
)

var receivers = sharedcomponent.NewSharedComponents()

// NewFactory creates a factory for the reload receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithTraces(createTracesReceiver, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	r := receivers.GetOrAdd(cfg, func() component.Component {
		return newReloadReceiver(params, cfg.(*Config))
	})
	r.Component.(*reloadReceiver).nextLogs = consumer
	return r, nil
}

func createMetricsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	r := receivers.GetOrAdd(cfg, func() component.Component {
		return newReloadReceiver(params, cfg.(*Config))
	})
	r.Component.(*reloadReceiver).nextMetrics = consumer
	return r, nil
}

func createTracesReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Traces,
) (receiver.Traces, error) {
	r := receivers.GetOrAdd(cfg, func() component.Component {
		return newReloadReceiver(params, cfg.(*Config))
	})
	r.Component.(*reloadReceiver).nextTraces = consumer
	return r, nil
}
