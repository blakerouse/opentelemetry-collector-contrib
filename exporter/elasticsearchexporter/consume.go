// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/pool"
)

// pushEncodedRequest is the request consumer (runs on the sending-queue
// consumer goroutines). It assembles the pre-encoded bulk items into bulk
// indexer sessions and flushes them; a pdataRequest that reached here
// unconverted is converted on the spot.
func (e *elasticsearchExporter) pushEncodedRequest(ctx context.Context, req xexporterhelper.Request) error {
	r, ok := req.(*encodedRequest)
	if !ok {
		conv, isEncodable := req.(encodableRequest)
		if !isEncodable {
			return fmt.Errorf("elasticsearchexporter: pushEncodedRequest got %T, expected *encodedRequest", req)
		}
		var err error
		if r, err = conv.toEncoded(ctx); err != nil {
			return err
		}
	}
	return e.consumeEncodedItems(ctx, r.items)
}

// consumeEncodedItems assembles already-encoded items into the appropriate
// bulk indexer sessions and flushes them. Plain items stream as-is; mergeable
// metric documents with the same group identity merge by byte splicing, and
// deferred ECS-mode metrics are grouped and encoded over the whole batch.
func (e *elasticsearchExporter) consumeEncodedItems(ctx context.Context, items []encodedItem) error {
	var sessions encodedSessionSet
	sessions.init(e)
	defer sessions.end()

	// One reader reused for all items: the bulk indexer copies the body
	// synchronously in Add, so no per-item reader allocation is needed.
	var reader bytes.Reader
	var errs []error
	var merger metricsDocMerger
	var deferred []*encodedItem
	for i := range items {
		item := &items[i]
		switch item.kind {
		case itemKindMergeableMetrics:
			merger.add(item)
			continue
		case itemKindDeferredMetrics:
			deferred = append(deferred, item)
			continue
		}
		session := sessions.get(ctx, item)
		reader.Reset(item.doc)
		if err := session.Add(
			ctx,
			item.index,
			item.docID,
			item.pipeline,
			&reader,
			item.dynamicTemplates,
			item.action,
		); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			errs = append(errs, err)
		}
	}

	for _, group := range merger.groups {
		if group.conflicted {
			// Overlapping names in the group: emit the original documents
			// unmerged so no two synthesized documents share a TSDB identity.
			for _, item := range group.items {
				reader.Reset(item.doc)
				if err := sessions.get(ctx, item).Add(
					ctx, item.index, item.docID, item.pipeline, &reader, item.dynamicTemplates, item.action,
				); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					errs = append(errs, err)
				}
			}
			continue
		}
		item := group.items[0]
		body, dynamicTemplates := group.assemble(&reader)
		session := sessions.get(ctx, item)
		if err := session.Add(
			ctx,
			item.index,
			item.docID,
			item.pipeline,
			body,
			dynamicTemplates,
			item.action,
		); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			errs = append(errs, err)
		}
	}

	if len(deferred) > 0 {
		if err := e.consumeDeferredMetrics(ctx, &sessions, deferred); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			errs = append(errs, err)
		}
	}

	if err := sessions.flush(ctx); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// encodedSessionSet lazily starts the bulk indexer sessions needed to consume a
// batch of encodedItems: one per mapping mode for targetDefault items, plus one
// per profiling indexer for profiling items.
type encodedSessionSet struct {
	e         *elasticsearchExporter
	modes     mappingModeSessions
	profiling [numSessionTargets]bulkIndexerSession
	extra     sessionList
}

func (s *encodedSessionSet) init(e *elasticsearchExporter) {
	s.e = e
	s.modes = mappingModeSessions{indexers: &e.bulkIndexers.modes}
}

func (s *encodedSessionSet) get(ctx context.Context, item *encodedItem) bulkIndexerSession {
	if item.target == targetDefault {
		return s.modes.StartSession(ctx, item.mappingMode)
	}
	if s.profiling[item.target] == nil {
		s.profiling[item.target] = s.e.profilingIndexer(item.target).StartSession(ctx)
		s.extra = append(s.extra, s.profiling[item.target])
	}
	return s.profiling[item.target]
}

func (s *encodedSessionSet) flush(ctx context.Context) error {
	return errors.Join(s.modes.Flush(ctx), s.extra.Flush(ctx))
}

func (s *encodedSessionSet) end() {
	s.modes.End()
	s.extra.End()
}

func (e *elasticsearchExporter) profilingIndexer(t sessionTarget) bulkIndexer {
	switch t {
	case targetProfilingEvents:
		return e.bulkIndexers.profilingEvents
	case targetProfilingStackTraces:
		return e.bulkIndexers.profilingStackTraces
	case targetProfilingStackFrames:
		return e.bulkIndexers.profilingStackFrames
	case targetProfilingExecutables:
		return e.bulkIndexers.profilingExecutables
	default:
		return nil // unreachable: targetDefault is handled by the caller
	}
}

// docSink consumes one freshly-encoded document together with its bulk-item
// metadata. It is the seam that lets one per-record encode path serve both
// sinks: sessionSink streams the document straight to a bulk indexer session,
// itemSink copies it into an encodedItem for the request queue. The sink takes
// ownership of doc.
type docSink interface {
	add(ctx context.Context, item encodedItem, doc encodedDoc) error
}

// encodedDoc abstracts the freshly-encoded bytes over pooled buffers (logs,
// traces and metrics, which own their buffer) and caller-owned buffers
// (profiles, whose buffer is reused by the encoder after the callback returns).
type encodedDoc struct {
	// writerTo is streamed to bulkIndexerSession.Add. For a pooled buffer its
	// WriteTo recycles the buffer once the bulk indexer has read it.
	writerTo io.WriterTo
	// buf is the underlying buffer, read when copying the document out.
	buf *bytes.Buffer
	// recycle releases a pooled buffer after it has been copied. It is nil for
	// caller-owned buffers, which must not be recycled here.
	recycle func()
}

func pooledDoc(buf pool.PooledBuffer) encodedDoc {
	return encodedDoc{writerTo: buf, buf: buf.Buffer, recycle: buf.Recycle}
}

func rawDoc(buf *bytes.Buffer) encodedDoc {
	return encodedDoc{writerTo: buf, buf: buf}
}

// sessionSink streams encoded documents directly to bulk indexer sessions,
// keeping one pooled buffer in flight at a time, freed by the bulk indexer as
// it reads it.
type sessionSink struct {
	sessions *encodedSessionSet
}

func (s sessionSink) add(ctx context.Context, item encodedItem, doc encodedDoc) error {
	// Not recycling on Add error: the buffer may already have been read (and thus
	// recycled) by the bulk indexer.
	return s.sessions.get(ctx, &item).Add(
		ctx, item.index, item.docID, item.pipeline, doc.writerTo, item.dynamicTemplates, item.action,
	)
}

// itemSink copies encoded documents into encodedItems for the request queue,
// materializing each document so it can outlive the pooled buffer and travel
// through the sending queue.
type itemSink struct {
	items []encodedItem
}

func (s *itemSink) add(_ context.Context, item encodedItem, doc encodedDoc) error {
	item.doc = make([]byte, doc.buf.Len())
	copy(item.doc, doc.buf.Bytes())
	if doc.recycle != nil {
		doc.recycle()
	}
	s.items = append(s.items, item)
	return nil
}
