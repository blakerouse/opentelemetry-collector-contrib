// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package receiverreloader // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receiverreloader"

import (
	"context"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

// fileWatcher watches a file for changes using fsnotify.
type fileWatcher struct {
	path           string
	logger         *zap.Logger
	watcher        *fsnotify.Watcher
	stop           chan struct{}
	watchingDir    bool // true if watching parent directory, false if watching file directly
}

func newFileWatcher(path string, logger *zap.Logger) *fileWatcher {
	return &fileWatcher{
		path:   path,
		logger: logger,
		stop:   make(chan struct{}),
	}
}

// Watch starts watching the file and returns a channel that receives notifications on changes.
func (fw *fileWatcher) Watch(ctx context.Context) (<-chan struct{}, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	fw.watcher = watcher

	// Buffered channel to avoid losing events if the receiver is temporarily busy
	changes := make(chan struct{}, 1)

	// Try to watch the parent directory first. This is preferred because it handles
	// atomic file replacements (e.g., Kubernetes ConfigMap updates) where the file
	// is replaced by creating a new file and renaming it.
	watchDir := filepath.Dir(fw.path)
	if err := watcher.Add(watchDir); err != nil {
		fw.logger.Debug("failed to watch parent directory, falling back to watching file directly",
			zap.String("directory", watchDir),
			zap.Error(err))

		// Fall back to watching the file directly. This may not catch atomic
		// replacements, but it works when we don't have permission to watch
		// the parent directory.
		if err := watcher.Add(fw.path); err != nil {
			watcher.Close()
			return nil, err
		}
		fw.watchingDir = false
		fw.logger.Info("watching file directly", zap.String("path", fw.path))
	} else {
		fw.watchingDir = true
		fw.logger.Info("watching parent directory for file changes",
			zap.String("directory", watchDir),
			zap.String("file", fw.path))
	}

	go fw.watchLoop(ctx, changes)

	return changes, nil
}

func (fw *fileWatcher) watchLoop(ctx context.Context, changes chan<- struct{}) {
	defer fw.watcher.Close()
	defer close(changes)

	for {
		select {
		case <-ctx.Done():
			return
		case <-fw.stop:
			return
		case event, ok := <-fw.watcher.Events:
			if !ok {
				return
			}

			// When watching the parent directory, filter events to only process
			// those for our target file.
			if fw.watchingDir && event.Name != fw.path {
				continue
			}

			// Handle relevant file events.
			if event.Op&fsnotify.Write == fsnotify.Write ||
				event.Op&fsnotify.Create == fsnotify.Create ||
				event.Op&fsnotify.Remove == fsnotify.Remove ||
				event.Op&fsnotify.Chmod == fsnotify.Chmod {
				fw.logger.Debug("detected file change",
					zap.String("path", fw.path),
					zap.String("operation", event.Op.String()))

				// If watching file directly and it was removed, try to re-add the watch.
				// This handles the case where the file is deleted and recreated.
				if !fw.watchingDir && (event.Op&fsnotify.Remove == fsnotify.Remove) {
					// Try to re-add watch on the file (it may have been recreated)
					_ = fw.watcher.Add(fw.path)
				}

				select {
				case changes <- struct{}{}:
				default:
					// Don't block if no one is listening
				}
			}

		case err, ok := <-fw.watcher.Errors:
			if !ok {
				return
			}
			fw.logger.Error("file watcher error", zap.Error(err))
		}
	}
}

// Stop stops the file watcher.
func (fw *fileWatcher) Stop() {
	select {
	case <-fw.stop:
		// Already closed
	default:
		close(fw.stop)
	}
}
