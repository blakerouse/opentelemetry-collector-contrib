// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package receiverreloader // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receiverreloader"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/hostcapabilities"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// host is the interface that the component.Host passed to receiverreloader must implement.
type host interface {
	component.Host
	hostcapabilities.ComponentFactory
}

// receiverReloader dynamically manages other receivers based on a watched configuration file.
type receiverReloader struct {
	cfg    *Config
	params receiver.Settings
	host   host

	nextLogs    consumer.Logs
	nextMetrics consumer.Metrics
	nextTraces  consumer.Traces

	mu              sync.Mutex
	receivers       map[string]*wrappedReceiver // id -> running receiver
	receiverConfigs map[string]map[string]any   // id -> config (for comparison)
	watcher         *fileWatcher
	cancel          context.CancelFunc
}

func newReceiverReloader(params receiver.Settings, cfg *Config) *receiverReloader {
	return &receiverReloader{
		cfg:             cfg,
		params:          params,
		receivers:       make(map[string]*wrappedReceiver),
		receiverConfigs: make(map[string]map[string]any),
	}
}

// Start begins the receiver reloader.
func (r *receiverReloader) Start(ctx context.Context, h component.Host) error {
	rHost, ok := h.(host)
	if !ok {
		return errors.New("the receiver reloader is not compatible with the provided component.Host")
	}
	r.host = rHost

	// Load initial config
	if err := r.loadAndApply(); err != nil {
		return fmt.Errorf("failed to load initial config from %s: %w", r.cfg.File, err)
	}

	// Start file watcher
	watchCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.watcher = newFileWatcher(r.cfg.File, r.params.Logger)
	changes, err := r.watcher.Watch(watchCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to start file watcher: %w", err)
	}
	go r.watchLoop(watchCtx, changes)

	r.params.Logger.Info("receiver reloader started", zap.String("file", r.cfg.File))
	return nil
}

// Shutdown stops the receiver reloader and all managed receivers.
func (r *receiverReloader) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.watcher != nil {
		r.watcher.Stop()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for id, rcvr := range r.receivers {
		if err := rcvr.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown %s: %w", id, err))
		}
	}
	r.receivers = make(map[string]*wrappedReceiver)
	r.receiverConfigs = make(map[string]map[string]any)

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

func (r *receiverReloader) watchLoop(ctx context.Context, changes <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-changes:
			if !ok {
				return
			}
			r.params.Logger.Info("config file changed, reloading", zap.String("file", r.cfg.File))
			if err := r.loadAndApply(); err != nil {
				r.params.Logger.Error("failed to reload config", zap.Error(err))
			}
		}
	}
}

func (r *receiverReloader) loadAndApply() error {
	data, err := os.ReadFile(r.cfg.File)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var dynamicCfg DynamicReceiversConfig
	if err := yaml.Unmarshal(data, &dynamicCfg); err != nil {
		return fmt.Errorf("failed to parse yaml: %w", err)
	}

	if dynamicCfg.Receivers == nil {
		dynamicCfg.Receivers = make(map[string]map[string]any)
	}

	return r.applyConfig(dynamicCfg.Receivers)
}

func (r *receiverReloader) applyConfig(newConfigs map[string]map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error

	// Find receivers to remove (exist in current but not in new)
	for id := range r.receivers {
		if _, exists := newConfigs[id]; !exists {
			r.params.Logger.Info("removing receiver", zap.String("id", id))
			if err := r.stopReceiverLocked(id); err != nil {
				errs = append(errs, err)
			}
		}
	}

	// Find receivers to add or update
	for id, newCfg := range newConfigs {
		oldCfg, exists := r.receiverConfigs[id]
		if !exists {
			// New receiver
			r.params.Logger.Info("adding receiver", zap.String("id", id))
			if err := r.startReceiverLocked(id, newCfg); err != nil {
				r.params.Logger.Error("failed to start receiver", zap.String("id", id), zap.Error(err))
				errs = append(errs, err)
			}
		} else if !configEqual(oldCfg, newCfg) {
			// Config changed - restart
			r.params.Logger.Info("restarting receiver due to config change", zap.String("id", id))
			if err := r.stopReceiverLocked(id); err != nil {
				errs = append(errs, err)
			}
			if err := r.startReceiverLocked(id, newCfg); err != nil {
				r.params.Logger.Error("failed to restart receiver", zap.String("id", id), zap.Error(err))
				errs = append(errs, err)
			}
		}
		// If config unchanged, do nothing
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

func (r *receiverReloader) startReceiverLocked(id string, cfg map[string]any) error {
	// Parse receiver ID to get type
	receiverID := component.ID{}
	if err := receiverID.UnmarshalText([]byte(id)); err != nil {
		return fmt.Errorf("invalid receiver id %q: %w", id, err)
	}

	// Get factory
	factory := r.host.GetFactory(component.KindReceiver, receiverID.Type())
	if factory == nil {
		return fmt.Errorf("unknown receiver type: %s", receiverID.Type())
	}
	receiverFactory, ok := factory.(receiver.Factory)
	if !ok {
		return fmt.Errorf("factory for %s is not a receiver factory", receiverID.Type())
	}

	// Create config
	receiverCfg := receiverFactory.CreateDefaultConfig()
	if len(cfg) > 0 {
		conf := confmap.NewFromStringMap(cfg)
		if err := conf.Unmarshal(receiverCfg); err != nil {
			return fmt.Errorf("failed to unmarshal config for %s: %w", id, err)
		}
	}

	// Validate config if it implements Validate()
	if validator, ok := receiverCfg.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("invalid config for %s: %w", id, err)
		}
	}

	// Create receiver settings
	settings := receiver.Settings{
		ID:                receiverID,
		TelemetrySettings: r.params.TelemetrySettings,
	}
	settings.Logger = r.params.Logger.With(zap.String("receiver", id))

	// Create receivers for each signal type
	wrapper := &wrappedReceiver{}
	var createErr error

	if r.nextMetrics != nil {
		var err error
		wrapper.metrics, err = receiverFactory.CreateMetrics(context.Background(), settings, receiverCfg, r.nextMetrics)
		if err != nil {
			if errors.Is(err, pipeline.ErrSignalNotSupported) {
				r.params.Logger.Debug("receiver doesn't support metrics", zap.String("id", id))
			} else {
				createErr = multierr.Append(createErr, fmt.Errorf("failed to create metrics receiver: %w", err))
			}
		}
	}

	if r.nextLogs != nil {
		var err error
		wrapper.logs, err = receiverFactory.CreateLogs(context.Background(), settings, receiverCfg, r.nextLogs)
		if err != nil {
			if errors.Is(err, pipeline.ErrSignalNotSupported) {
				r.params.Logger.Debug("receiver doesn't support logs", zap.String("id", id))
			} else {
				createErr = multierr.Append(createErr, fmt.Errorf("failed to create logs receiver: %w", err))
			}
		}
	}

	if r.nextTraces != nil {
		var err error
		wrapper.traces, err = receiverFactory.CreateTraces(context.Background(), settings, receiverCfg, r.nextTraces)
		if err != nil {
			if errors.Is(err, pipeline.ErrSignalNotSupported) {
				r.params.Logger.Debug("receiver doesn't support traces", zap.String("id", id))
			} else {
				createErr = multierr.Append(createErr, fmt.Errorf("failed to create traces receiver: %w", err))
			}
		}
	}

	if createErr != nil {
		return createErr
	}

	if wrapper.metrics == nil && wrapper.logs == nil && wrapper.traces == nil {
		return fmt.Errorf("receiver %s doesn't support any of the configured signals", id)
	}

	// Start receiver
	if err := wrapper.Start(context.Background(), r.host); err != nil {
		return fmt.Errorf("failed to start receiver %s: %w", id, err)
	}

	r.receivers[id] = wrapper
	r.receiverConfigs[id] = cfg

	r.params.Logger.Info("started receiver", zap.String("id", id))
	return nil
}

func (r *receiverReloader) stopReceiverLocked(id string) error {
	rcvr, exists := r.receivers[id]
	if !exists {
		return nil
	}

	if err := rcvr.Shutdown(context.Background()); err != nil {
		r.params.Logger.Error("failed to stop receiver", zap.String("id", id), zap.Error(err))
		// Still remove from maps even if shutdown fails
	}

	delete(r.receivers, id)
	delete(r.receiverConfigs, id)

	r.params.Logger.Info("stopped receiver", zap.String("id", id))
	return nil
}

// configEqual performs a deep comparison of two config maps.
func configEqual(a, b map[string]any) bool {
	return reflect.DeepEqual(a, b)
}

// wrappedReceiver combines multiple signal receivers into a single component.
type wrappedReceiver struct {
	logs    receiver.Logs
	metrics receiver.Metrics
	traces  receiver.Traces
}

func (w *wrappedReceiver) Start(ctx context.Context, host component.Host) error {
	var err error
	for _, r := range []component.Component{w.logs, w.metrics, w.traces} {
		if r != nil {
			if e := r.Start(ctx, host); e != nil {
				err = multierr.Append(err, e)
			}
		}
	}
	return err
}

func (w *wrappedReceiver) Shutdown(ctx context.Context) error {
	var err error
	for _, r := range []component.Component{w.logs, w.metrics, w.traces} {
		if r != nil {
			if e := r.Shutdown(ctx); e != nil {
				err = multierr.Append(err, e)
			}
		}
	}
	return err
}
