// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package reloadreceiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestFileWatcherDetectsChanges(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test.yaml")

	// Create initial file
	err := os.WriteFile(configFile, []byte("initial"), 0600)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	watcher := newFileWatcher(configFile, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	changes, err := watcher.Watch(ctx)
	require.NoError(t, err)

	// Wait a bit for watcher to start
	time.Sleep(100 * time.Millisecond)

	// Modify file
	err = os.WriteFile(configFile, []byte("modified"), 0600)
	require.NoError(t, err)

	// Should receive change notification
	select {
	case <-changes:
		// Success
	case <-ctx.Done():
		t.Fatal("timed out waiting for change notification")
	}

	watcher.Stop()
}

func TestFileWatcherHandlesNonexistentDirectory(t *testing.T) {
	logger := zaptest.NewLogger(t)
	watcher := newFileWatcher("/nonexistent/directory/file.yaml", logger)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Watch should fail because the directory doesn't exist
	_, err := watcher.Watch(ctx)
	require.Error(t, err)

	watcher.Stop()
}

func TestFileWatcherStop(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test.yaml")

	err := os.WriteFile(configFile, []byte("test"), 0600)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	watcher := newFileWatcher(configFile, logger)

	ctx := context.Background()
	changes, err := watcher.Watch(ctx)
	require.NoError(t, err)

	// Stop the watcher
	watcher.Stop()

	// Channel should be closed eventually
	select {
	case _, ok := <-changes:
		assert.False(t, ok, "channel should be closed after Stop")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}
}

func TestFileWatcherDoubleStop(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test.yaml")

	err := os.WriteFile(configFile, []byte("test"), 0600)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	watcher := newFileWatcher(configFile, logger)

	ctx := context.Background()
	_, err = watcher.Watch(ctx)
	require.NoError(t, err)

	// Should not panic on double stop
	watcher.Stop()
	watcher.Stop()
}

func TestFileWatcherDetectsCreate(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test.yaml")

	// Don't create the file yet
	logger := zaptest.NewLogger(t)
	watcher := newFileWatcher(configFile, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	changes, err := watcher.Watch(ctx)
	require.NoError(t, err)

	// Wait a bit for watcher to start
	time.Sleep(100 * time.Millisecond)

	// Create the file
	err = os.WriteFile(configFile, []byte("created"), 0600)
	require.NoError(t, err)

	// Should receive change notification
	select {
	case <-changes:
		// Success
	case <-ctx.Done():
		t.Fatal("timed out waiting for create notification")
	}

	watcher.Stop()
}

func TestFileWatcherWatchingModes(t *testing.T) {
	// Test that watcher correctly identifies watching mode
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test.yaml")

	err := os.WriteFile(configFile, []byte("test"), 0600)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	watcher := newFileWatcher(configFile, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = watcher.Watch(ctx)
	require.NoError(t, err)

	// Should be watching directory (default preferred mode)
	assert.True(t, watcher.watchingDir, "should be watching parent directory by default")

	watcher.Stop()
}
