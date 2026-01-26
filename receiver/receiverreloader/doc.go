// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package receiverreloader implements a receiver that dynamically manages other receivers
// based on a configuration file that is watched for changes.
package receiverreloader // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receiverreloader"
