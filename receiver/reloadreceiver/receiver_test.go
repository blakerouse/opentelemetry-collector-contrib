// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package reloadreceiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/reloadreceiver/internal/metadata"
)

// mockHost implements the host interface for testing.
type mockHost struct {
	component.Host
	factories map[component.Type]component.Factory
}

func newMockHost() *mockHost {
	return &mockHost{
		Host:      nil,
		factories: make(map[component.Type]component.Factory),
	}
}

func (m *mockHost) GetExtensions() map[component.ID]component.Component {
	return nil
}

func (m *mockHost) GetFactory(kind component.Kind, componentType component.Type) component.Factory {
	if kind == component.KindReceiver {
		return m.factories[componentType]
	}
	return nil
}

func TestReceiverStartShutdown(t *testing.T) {
	// Create a temp config file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "receivers.yaml")

	// Write empty receivers config
	err := os.WriteFile(configFile, []byte("receivers: {}\n"), 0600)
	require.NoError(t, err)

	cfg := &Config{
		File: configFile,
	}

	r := newReloadReceiver(receivertest.NewNopSettings(metadata.Type), cfg)
	r.nextMetrics = consumertest.NewNop()

	host := newMockHost()

	// Start
	err = r.Start(context.Background(), host)
	require.NoError(t, err)

	// Shutdown
	err = r.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestReceiverStartFailsWithMissingFile(t *testing.T) {
	cfg := &Config{
		File: "/nonexistent/path/receivers.yaml",
	}

	r := newReloadReceiver(receivertest.NewNopSettings(metadata.Type), cfg)
	r.nextMetrics = consumertest.NewNop()

	host := newMockHost()

	err := r.Start(context.Background(), host)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load initial config")
}

func TestReceiverStartFailsWithInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "receivers.yaml")

	// Write invalid YAML
	err := os.WriteFile(configFile, []byte("this is not valid yaml: [[["), 0600)
	require.NoError(t, err)

	cfg := &Config{
		File: configFile,
	}

	r := newReloadReceiver(receivertest.NewNopSettings(metadata.Type), cfg)
	r.nextMetrics = consumertest.NewNop()

	host := newMockHost()

	err = r.Start(context.Background(), host)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse yaml")
}

func TestConfigEqual(t *testing.T) {
	tests := []struct {
		name  string
		a     map[string]any
		b     map[string]any
		equal bool
	}{
		{
			name:  "both nil",
			a:     nil,
			b:     nil,
			equal: true,
		},
		{
			name:  "both empty",
			a:     map[string]any{},
			b:     map[string]any{},
			equal: true,
		},
		{
			name:  "same values",
			a:     map[string]any{"endpoint": "localhost:8080", "timeout": 30},
			b:     map[string]any{"endpoint": "localhost:8080", "timeout": 30},
			equal: true,
		},
		{
			name:  "different values",
			a:     map[string]any{"endpoint": "localhost:8080"},
			b:     map[string]any{"endpoint": "localhost:9090"},
			equal: false,
		},
		{
			name:  "different keys",
			a:     map[string]any{"endpoint": "localhost:8080"},
			b:     map[string]any{"host": "localhost:8080"},
			equal: false,
		},
		{
			name:  "extra key in b",
			a:     map[string]any{"endpoint": "localhost:8080"},
			b:     map[string]any{"endpoint": "localhost:8080", "timeout": 30},
			equal: false,
		},
		{
			name: "nested maps equal",
			a: map[string]any{
				"tls": map[string]any{"enabled": true, "cert": "/path/to/cert"},
			},
			b: map[string]any{
				"tls": map[string]any{"enabled": true, "cert": "/path/to/cert"},
			},
			equal: true,
		},
		{
			name: "nested maps different",
			a: map[string]any{
				"tls": map[string]any{"enabled": true},
			},
			b: map[string]any{
				"tls": map[string]any{"enabled": false},
			},
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := configEqual(tt.a, tt.b)
			assert.Equal(t, tt.equal, result)
		})
	}
}

func TestWrappedReceiverStartShutdown(t *testing.T) {
	// Create a mock receiver that tracks start/shutdown calls
	startCalled := false
	shutdownCalled := false

	mockRcvr := &mockReceiver{
		startFunc: func(_ context.Context, _ component.Host) error {
			startCalled = true
			return nil
		},
		shutdownFunc: func(_ context.Context) error {
			shutdownCalled = true
			return nil
		},
	}

	wrapper := &wrappedReceiver{
		metrics: mockRcvr,
	}

	err := wrapper.Start(context.Background(), nil)
	require.NoError(t, err)
	assert.True(t, startCalled)

	err = wrapper.Shutdown(context.Background())
	require.NoError(t, err)
	assert.True(t, shutdownCalled)
}

func TestWrappedReceiverNilReceivers(t *testing.T) {
	wrapper := &wrappedReceiver{
		logs:    nil,
		metrics: nil,
		traces:  nil,
	}

	// Should not panic with nil receivers
	err := wrapper.Start(context.Background(), nil)
	require.NoError(t, err)

	err = wrapper.Shutdown(context.Background())
	require.NoError(t, err)
}

// mockReceiver implements receiver.Metrics for testing.
type mockReceiver struct {
	startFunc    func(context.Context, component.Host) error
	shutdownFunc func(context.Context) error
}

func (m *mockReceiver) Start(ctx context.Context, host component.Host) error {
	if m.startFunc != nil {
		return m.startFunc(ctx, host)
	}
	return nil
}

func (m *mockReceiver) Shutdown(ctx context.Context) error {
	if m.shutdownFunc != nil {
		return m.shutdownFunc(ctx)
	}
	return nil
}

var _ receiver.Metrics = (*mockReceiver)(nil)
