// Package mongo implements a MongoDB backend for kine.
package mongo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/ttl"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// explicit interface check
var _ server.Backend = (*Backend)(nil)

// Backend implements server.Backend on top of MongoDB.
type Backend struct {
	cfg    *Config
	client *mongo.Client
	col    *mongo.Collection
	meta   *mongo.Collection

	mu           sync.RWMutex
	currentRev   int64
	compactRev   int64
	ultimoEvento time.Time

	synced *sync.Cond

	broadcaster broadcaster.Broadcaster
	ctx         context.Context

	streams atomic.Int64
}

func init() {
	drivers.Register("mongodb", New)
	drivers.Register("mongodb+srv", New)
}

// New builds the backend. It returns leaderElect=true because MongoDB is a
func New(ctx context.Context, wg *sync.WaitGroup, drvCfg *drivers.Config) (bool, server.Backend, error) {
	dsn := drvCfg.Endpoint
	if dsn == "" {
		return false, nil, fmt.Errorf("empty endpoint: provide a mongodb:// connection string")
	}

	cfg, err := ParseDSN(dsn)
	if err != nil {
		return false, nil, err
	}

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority()).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetRetryWrites(true)

	client, err := mongo.Connect(opts)
	if err != nil {
		return false, nil, fmt.Errorf("connect to MongoDB: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ServerSelectionTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return false, nil, fmt.Errorf("ping MongoDB: %w", err)
	}

	db := client.Database(cfg.Database)
	b := &Backend{
		cfg:    cfg,
		client: client,
		col:    db.Collection(cfg.Collection),
		meta:   db.Collection(cfg.Collection + metaCollSuffix),
	}
	b.synced = sync.NewCond(b.mu.RLocker())

	registerMetrics(drvCfg.MetricsRegisterer)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logrus.Info("Closing MongoDB connection…")
		if err := client.Disconnect(context.Background()); err != nil {
			logrus.Errorf("Failed to disconnect from MongoDB: %v", err)
		}
	}()

	return true, b, nil
}

// Start prepares the schema and loads the initial state.
func (b *Backend) Start(ctx context.Context) error {
	b.ctx = ctx

	if err := b.setup(ctx); err != nil {
		return err
	}

	if err := b.ensureCompactKey(ctx); err != nil {
		return err
	}

	rev, err := b.loadCurrentRevision(ctx)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.currentRev = rev
	b.mu.Unlock()

	cr, err := b.loadCompactRevision(ctx)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.compactRev = cr
	b.mu.Unlock()

	logrus.Infof("MongoDB started: current revision=%d, compacted revision=%d", rev, cr)
	go ttl.Run(ctx, b)
	go b.collectGauges(ctx)
	return nil
}

// collectGauges periodically refreshes the metrics not derived from operations.
func (b *Backend) collectGauges(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		b.mu.RLock()
		rev, comp := b.currentRev, b.compactRev
		ultimo := b.ultimoEvento
		b.mu.RUnlock()

		CurrentRevisionGauge.Set(float64(rev))
		CompactedRevisionGauge.Set(float64(comp))
		if !ultimo.IsZero() {
			ChangeStreamLag.Set(time.Since(ultimo).Seconds())
		}

		var stats struct {
			StorageSize    int64 `bson:"storageSize"`
			TotalIndexSize int64 `bson:"totalIndexSize"`
			Size           int64 `bson:"size"`
		}
		err := b.client.Database(b.cfg.Database).
			RunCommand(ctx, bson.D{{Key: "collStats", Value: b.cfg.Collection}}).
			Decode(&stats)
		if err != nil {
			continue
		}
		StorageBytes.WithLabelValues("dados").Set(float64(stats.Size))
		StorageBytes.WithLabelValues("storage").Set(float64(stats.StorageSize))
		StorageBytes.WithLabelValues("indices").Set(float64(stats.TotalIndexSize))
	}
}

func (b *Backend) ensureCompactKey(ctx context.Context) error {
	n, err := b.col.CountDocuments(ctx, bson.M{"name": compactRevKey})
	if err != nil {
		return fmt.Errorf("check compact_rev_key: %w", err)
	}
	if n > 0 {
		return nil
	}
	_, err = b.append(ctx, &Record{
		Name:    compactRevKey,
		Created: true,
		Value:   []byte(""),
	})
	if mongo.IsDuplicateKeyError(err) {
		return nil // another instance created it first
	}
	return err
}

func (b *Backend) loadCurrentRevision(ctx context.Context) (int64, error) {
	var r Record
	err := b.col.FindOne(ctx, bson.M{},
		options.FindOne().SetSort(bson.D{{Key: "rev", Value: -1}}).
			SetProjection(bson.M{"rev": 1})).Decode(&r)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read current revision: %w", err)
	}
	return r.Rev, nil
}

func (b *Backend) loadCompactRevision(ctx context.Context) (int64, error) {
	var meta metaDoc
	err := b.meta.FindOne(ctx, bson.M{"_id": metaID}).Decode(&meta)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read compacted revision: %w", err)
	}
	return meta.CompactRevision, nil
}

// CurrentRevision returns the latest known revision.
func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	b.mu.RLock()
	rev := b.currentRev
	b.mu.RUnlock()
	if rev != 0 {
		return rev, nil
	}
	return b.loadCurrentRevision(ctx)
}

// DbSize returns the space used, data plus indexes.
func (b *Backend) DbSize(ctx context.Context) (int64, error) {
	var stats struct {
		StorageSize    int64 `bson:"storageSize"`
		TotalIndexSize int64 `bson:"totalIndexSize"`
	}
	err := b.client.Database(b.cfg.Database).
		RunCommand(ctx, bson.D{{Key: "collStats", Value: b.cfg.Collection}}).
		Decode(&stats)
	if err != nil {
		return 0, fmt.Errorf("collStats: %w", err)
	}
	return stats.StorageSize + stats.TotalIndexSize, nil
}

// WaitForSyncTo blocks until the given revision has been observed.
func (b *Backend) WaitForSyncTo(revision int64) {
	b.mu.RLock()
	for b.currentRev < revision {
		b.synced.Wait()
	}
	b.mu.RUnlock()
}

// openStreams reports how many change streams this process holds.
func (b *Backend) openStreams() int64 { return b.streams.Load() }

// observeRevision records a seen revision and wakes up whoever waits for it.
func (b *Backend) observeRevision(rev int64) {
	b.mu.Lock()
	if rev > b.currentRev {
		b.currentRev = rev
	}
	b.ultimoEvento = time.Now()
	b.mu.Unlock()
	b.synced.Broadcast()
}
