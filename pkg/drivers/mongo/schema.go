package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Record is one entry in the revision log — the BSON equivalent of the SQLite
type Record struct {
	ID             bson.ObjectID `bson:"_id,omitempty"`
	Rev            int64         `bson:"rev"`
	Name           string        `bson:"name"`
	Created        bool          `bson:"created"`
	Deleted        bool          `bson:"deleted"`
	CreateRevision int64         `bson:"create_revision"`
	PrevRevision   int64         `bson:"prev_revision"`
	Version        int64         `bson:"version"`
	Lease          int64         `bson:"lease"`
	Value          []byte        `bson:"value,omitempty"`
	OldValue       []byte        `bson:"old_value,omitempty"`
}

// metaDoc holds the cluster state that must survive restarts.
type metaDoc struct {
	ID              string `bson:"_id"`
	EpochBase       int64  `bson:"epoch_base"`
	CompactRevision int64  `bson:"compact_revision"`
}

const (
	metaID          = "kine-meta"
	compactRevKey   = "compact_rev_key"
	metaCollSuffix  = "_meta"
	idxNameRev      = "name_rev"
	idxRev          = "rev"
	idxNamePrevUniq = "name_prev_uniq"
	idxPrevRev      = "prev_revision"
	legacyIdxTTL    = "lease_ttl"
)

// setup creates collections and indexes, and resolves the cluster epoch base.
func (b *Backend) setup(ctx context.Context) error {
	logrus.Info("Configuring MongoDB collections and indexes…")

	idx := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "name", Value: 1}, {Key: "rev", Value: -1}},
			Options: options.Index().SetName(idxNameRev),
		},
		{
			Keys:    bson.D{{Key: "rev", Value: 1}},
			Options: options.Index().SetName(idxRev),
		},
		{
			Keys:    bson.D{{Key: "name", Value: 1}, {Key: "prev_revision", Value: 1}},
			Options: options.Index().SetName(idxNamePrevUniq).SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "prev_revision", Value: 1}},
			Options: options.Index().SetName(idxPrevRev),
		},
	}

	if _, err := b.col.Indexes().CreateMany(ctx, idx); err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	if err := b.col.Indexes().DropOne(ctx, legacyIdxTTL); err != nil {
		var serverErr mongo.ServerError
		if !errors.As(err, &serverErr) || !serverErr.HasErrorCode(27) {
			return fmt.Errorf("drop legacy TTL index: %w", err)
		}
	}

	if err := b.resolveEpochBase(ctx); err != nil {
		return err
	}

	logrus.Infof("MongoDB ready: database=%s collection=%s epochBase=%d",
		b.cfg.Database, b.cfg.Collection, b.cfg.EpochBase)
	return nil
}

// resolveEpochBase reads the stored epoch base, or writes the configured one.
func (b *Backend) resolveEpochBase(ctx context.Context) error {
	var meta metaDoc
	err := b.meta.FindOne(ctx, bson.M{"_id": metaID}).Decode(&meta)
	switch {
	case err == nil:
		if b.cfg.EpochBase != defaultEpochBase && b.cfg.EpochBase != meta.EpochBase {
			return fmt.Errorf(
				"kine_epoch_base=%d conflicts with the value %d already stored in this cluster: "+
					"changing it would invalidate every existing revision",
				b.cfg.EpochBase, meta.EpochBase)
		}
		b.cfg.EpochBase = meta.EpochBase
		return nil

	case err == mongo.ErrNoDocuments:
		_, err := b.meta.InsertOne(ctx, metaDoc{
			ID:        metaID,
			EpochBase: b.cfg.EpochBase,
		})
		if mongo.IsDuplicateKeyError(err) {
			return b.resolveEpochBase(ctx)
		}
		if err != nil {
			return fmt.Errorf("write metadata: %w", err)
		}
		logrus.Infof("Cluster epoch base set to %d", b.cfg.EpochBase)
		return nil

	default:
		return fmt.Errorf("read metadata: %w", err)
	}
}
