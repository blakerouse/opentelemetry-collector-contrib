// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/lestrrat-go/strftime"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

// unwrapPermanent strips a single consumererror permanent wrapper from err,
// if present. The request-converter path in exporterhelper re-wraps converter
// errors as permanent, so passing an already-permanent error through would
// double the "Permanent error:" prefix in the surfaced message.
func unwrapPermanent(err error) error {
	if consumererror.IsPermanent(err) {
		if inner := errors.Unwrap(err); inner != nil {
			return inner
		}
	}
	return err
}

func generateIndexWithLogstashFormat(index string, conf *LogstashFormatSettings, t time.Time) (string, error) {
	if conf.Enabled {
		partIndex := fmt.Sprintf("%s%s", index, conf.PrefixSeparator)
		var buf bytes.Buffer
		p, err := strftime.New(fmt.Sprintf("%s%s", partIndex, conf.DateFormat))
		if err != nil {
			return partIndex, err
		}
		err = p.Format(&buf, t)
		if err != nil {
			return partIndex, err
		}
		index = buf.String()
	}
	return index, nil
}
