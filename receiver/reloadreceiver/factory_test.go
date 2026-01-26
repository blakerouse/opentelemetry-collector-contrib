// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package reloadreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/reloadreceiver/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "reload", factory.Type().String())
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg)

	rCfg, ok := cfg.(*Config)
	require.True(t, ok)

	assert.Equal(t, "", rCfg.File)
}

func TestCreateReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.File = "/tmp/test-receivers.yaml"

	// Test metrics receiver creation
	metricsReceiver, err := factory.CreateMetrics(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, metricsReceiver)

	// Test logs receiver creation
	logsReceiver, err := factory.CreateLogs(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, logsReceiver)

	// Test traces receiver creation
	tracesReceiver, err := factory.CreateTraces(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, tracesReceiver)

	// They should all return the same shared component
	assert.Same(t, metricsReceiver, logsReceiver)
	assert.Same(t, logsReceiver, tracesReceiver)

	// Cleanup
	err = metricsReceiver.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestCreateReceiverInvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	// File is empty, which is invalid

	_, err := factory.CreateMetrics(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	// The receiver is created but will fail on Start, not on Create
	require.NoError(t, err)
}

func TestType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "reload", factory.Type().String())
}

func TestComponentLifecycle(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.File = "/tmp/nonexistent.yaml"

	recv, err := factory.CreateMetrics(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	// Start should fail (either because NopHost doesn't implement
	// hostcapabilities.ComponentFactory or because file doesn't exist)
	err = recv.Start(context.Background(), componenttest.NewNopHost())
	require.Error(t, err)
}
