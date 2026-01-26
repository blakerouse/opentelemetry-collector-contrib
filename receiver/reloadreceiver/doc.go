// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package reloadreceiver implements a receiver that dynamically manages other receivers
// based on a configuration file that is watched for changes.
package reloadreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/reloadreceiver"
