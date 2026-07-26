// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

func TestUnwrapPermanent(t *testing.T) {
	t.Run("permanent is unwrapped once", func(t *testing.T) {
		inner := errors.New("boom")
		got := unwrapPermanent(consumererror.NewPermanent(inner))
		require.Equal(t, inner, got)
		require.False(t, consumererror.IsPermanent(got))
		// The re-wrap that the request-converter path performs must yield a
		// single "Permanent error:" prefix.
		require.EqualError(t, consumererror.NewPermanent(got), "Permanent error: boom")
	})
	t.Run("non-permanent is returned unchanged", func(t *testing.T) {
		err := errors.New("boom")
		require.Equal(t, err, unwrapPermanent(err))
	})
	t.Run("nil is returned unchanged", func(t *testing.T) {
		require.NoError(t, unwrapPermanent(nil))
	})
}
