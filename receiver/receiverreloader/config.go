// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package receiverreloader // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receiverreloader"

import (
	"errors"
)

// Config defines configuration for the reload receiver.
type Config struct {
	// File is the path to the dynamic receivers configuration file.
	File string `mapstructure:"file"`
}

// Validate checks if the receiver configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.File == "" {
		return errors.New("file is required")
	}
	return nil
}

// DynamicReceiversConfig is the structure of the watched configuration file.
type DynamicReceiversConfig struct {
	Receivers map[string]map[string]any `yaml:"receivers"`
}
