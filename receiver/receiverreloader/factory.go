// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package receiverreloader // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receiverreloader"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receiverreloader/internal/metadata"
)

var receivers = sharedcomponent.NewSharedComponents()

// NewFactory creates a factory for the receiver reloader.
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
		return newReceiverReloader(params, cfg.(*Config))
	})
	r.Component.(*receiverReloader).nextLogs = consumer
	return r, nil
}

func createMetricsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	r := receivers.GetOrAdd(cfg, func() component.Component {
		return newReceiverReloader(params, cfg.(*Config))
	})
	r.Component.(*receiverReloader).nextMetrics = consumer
	return r, nil
}

func createTracesReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Traces,
) (receiver.Traces, error) {
	r := receivers.GetOrAdd(cfg, func() component.Component {
		return newReceiverReloader(params, cfg.(*Config))
	})
	r.Component.(*receiverReloader).nextTraces = consumer
	return r, nil
}
