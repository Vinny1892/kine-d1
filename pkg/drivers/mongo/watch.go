package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	errChangeStreamHistoryLost = 286
	errChangeStreamFatalError  = 280
)

// Watch delivers a range's events starting from a revision.
func (b *Backend) Watch(ctx context.Context, key, end string, revision int64) server.WatchResult {
	events := make(chan []*server.Event, 100)
	errc := make(chan error, 1)

	rev, err := b.CurrentRevision(ctx)
	if err != nil {
		errc <- err
		close(events)
		close(errc)
		return server.WatchResult{Errorc: errc, Events: events}
	}

	compact := b.compactRevision()
	if revision > 0 && compact > 0 && revision < compact {
		errc <- server.ErrCompacted
		close(events)
		close(errc)
		return server.WatchResult{CurrentRevision: rev, CompactRevision: compact, Errorc: errc}
	}

	live, err := b.broadcaster.Subscribe(ctx, b.connectStream)
	if err != nil {
		errc <- fmt.Errorf("subscribe to the change stream: %w", err)
		close(events)
		close(errc)
		return server.WatchResult{CurrentRevision: rev, CompactRevision: compact, Errorc: errc}
	}

	go b.forward(ctx, key, end, revision, live, events, errc)

	return server.WatchResult{
		CurrentRevision: rev,
		CompactRevision: compact,
		Events:          events,
		Errorc:          errc,
	}
}

// forward delivers the requested history, then filters the live stream.
func (b *Backend) forward(ctx context.Context, key, end string, revision int64,
	live <-chan server.Events, events chan []*server.Event, errc chan error) {

	defer close(events)
	defer close(errc)

	cutoff := revision
	if cutoff > 0 {
		antigos, err := b.after(ctx, key, end, cutoff, 0)
		if err != nil {
			errc <- err
			return
		}
		if len(antigos) > 0 {
			events <- recordsToEvents(antigos)
			cutoff = antigos[len(antigos)-1].Rev
		}
	} else {
		current, err := b.CurrentRevision(ctx)
		if err != nil {
			errc <- err
			return
		}
		cutoff = current
	}

	for {
		select {
		case <-ctx.Done():
			return
		case lote, ok := <-live:
			if !ok {
				return
			}
			var mine []*server.Event
			for _, e := range lote {
				if e.KV == nil || e.KV.ModRevision <= cutoff {
					continue // already covered by history replay
				}
				if !inRange(e.KV.Key, key, end) {
					continue
				}
				cutoff = e.KV.ModRevision
				mine = append(mine, e)
			}
			if len(mine) > 0 {
				select {
				case events <- mine:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// connectStream is the broadcaster's ConnectFunc, called once per process.
func (b *Backend) connectStream() (chan server.Events, error) {
	out := make(chan server.Events, 100)
	go b.streamLoop(out)
	return out, nil
}

// streamLoop keeps a change stream alive, reopening it when it drops.
func (b *Backend) streamLoop(out chan server.Events) {
	defer close(out)

	var token bson.Raw
	backoff := time.Second

	for {
		if b.ctx.Err() != nil {
			return
		}

		cs, err := b.openStream(token)
		if err != nil {
			if b.ctx.Err() != nil {
				return
			}
			ChangeStreamReconnects.WithLabelValues("falha_ao_abrir").Inc()
			logrus.Errorf("Failed to open the change stream: %v", err)
			if !b.sleepOrDone(backoff) {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second

		b.streams.Add(1)
		token = b.consume(cs, out, token)
		b.streams.Add(-1)
		cs.Close(context.Background())
	}
}

// consume reads the stream until it drops, returning the latest resume token.
func (b *Backend) consume(cs *mongo.ChangeStream, out chan server.Events, token bson.Raw) bson.Raw {
	for cs.Next(b.ctx) {
		var ev struct {
			ClusterTime  bson.Timestamp `bson:"clusterTime"`
			FullDocument *Record        `bson:"fullDocument"`
		}
		if err := cs.Decode(&ev); err != nil {
			logrus.Errorf("Failed to decode change stream event: %v", err)
			continue
		}
		token = cs.ResumeToken()

		r := ev.FullDocument
		if r == nil {
			continue
		}
		rev, err := EncodeRevision(ev.ClusterTime, b.cfg.EpochBase)
		if err != nil {
			logrus.Errorf("Invalid clusterTime in change stream: %v", err)
			continue
		}
		r.Rev = rev
		if r.Created {
			r.CreateRevision = rev
		}
		b.observeRevision(rev)

		select {
		case out <- recordsToEvents([]*Record{r}):
		case <-b.ctx.Done():
			return token
		}
	}

	err := cs.Err()
	if err == nil || errors.Is(err, context.Canceled) {
		return token
	}

	if historyLost(err) {
		ChangeStreamReconnects.WithLabelValues("historico_perdido").Inc()
		logrus.Warnf("Change stream invalidated (%v): backfilling by direct read", err)
		if b.backfill(out) {
			return nil // restart without a token: the window was covered
		}
	}
	ChangeStreamReconnects.WithLabelValues("queda").Inc()
	logrus.Errorf("Change stream dropped: %v", err)
	return token
}

// recover backfills the missed interval by reading the collection directly.
func (b *Backend) backfill(out chan server.Events) bool {
	b.mu.RLock()
	since := b.currentRev
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(b.ctx, 2*time.Minute)
	defer cancel()

	missed, err := b.after(ctx, "", "", since, 0)
	if err != nil {
		logrus.Errorf("Failed to backfill the missed watch interval: %v", err)
		return false
	}
	if len(missed) == 0 {
		return true
	}
	logrus.Infof("Backfilled %d events missed due to change stream invalidation", len(missed))
	for _, r := range missed {
		b.observeRevision(r.Rev)
		select {
		case out <- recordsToEvents([]*Record{r}):
		case <-b.ctx.Done():
			return false
		}
	}
	return true
}

// openStream opens the change stream, resuming from the token when present.
func (b *Backend) openStream(token bson.Raw) (*mongo.ChangeStream, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
	}

	opts := options.ChangeStream()
	if len(token) > 0 {
		opts.SetResumeAfter(token)
	} else {
		b.mu.RLock()
		rev := b.currentRev
		b.mu.RUnlock()
		if rev > 0 {
			ts := DecodeRevision(rev, b.cfg.EpochBase)
			opts.SetStartAtOperationTime(&ts)
		}
	}
	return b.col.Watch(b.ctx, pipeline, opts)
}

// historyLost reports whether the error means the oplog no longer covers the resume point.
func historyLost(err error) bool {
	var ce mongo.ServerError
	if errors.As(err, &ce) {
		return ce.HasErrorCode(errChangeStreamHistoryLost) ||
			ce.HasErrorCode(errChangeStreamFatalError)
	}
	return false
}

func (b *Backend) sleepOrDone(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-b.ctx.Done():
		return false
	}
}

func inRange(name, key, end string) bool {
	if key == "" {
		return true
	}
	if end == "" {
		return name == key
	}
	return name >= key && name < end
}

func recordsToEvents(rs []*Record) []*server.Event {
	out := make([]*server.Event, 0, len(rs))
	for _, r := range rs {
		e := &server.Event{
			Create: r.Created,
			Delete: r.Deleted,
			KV:     r.toKV(),
		}
		if r.PrevRevision > 0 {
			e.PrevKV = &server.KeyValue{
				Key:            r.Name,
				Value:          r.OldValue,
				Version:        r.Version - 1,
				CreateRevision: r.CreateRevision,
				ModRevision:    r.PrevRevision,
			}
		}
		out = append(out, e)
	}
	return out
}
