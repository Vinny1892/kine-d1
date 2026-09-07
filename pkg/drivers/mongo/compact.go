package mongo

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Compact removes superseded revisions and tombstones up to the target revision.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	b.mu.RLock()
	current := b.compactRev
	b.mu.RUnlock()

	if revision <= current {
		return current, server.ErrCompacted
	}

	cur, err := b.CurrentRevision(ctx)
	if err != nil {
		return current, err
	}
	if revision > cur {
		return current, server.ErrFutureRev
	}

	sess, err := b.client.StartSession()
	if err != nil {
		return current, fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	var deleted int64
	_, err = sess.WithTransaction(ctx, func(sctx context.Context) (interface{}, error) {
		res, err := b.meta.UpdateOne(sctx,
			bson.M{"_id": metaID, "compact_revision": current},
			bson.M{"$set": bson.M{"compact_revision": revision}})
		if err != nil {
			return nil, err
		}
		if res.MatchedCount == 0 {
			return nil, server.ErrCompacted
		}

		dist := b.col.Distinct(sctx, "prev_revision", bson.M{
			"name":          bson.M{"$ne": compactRevKey},
			"prev_revision": bson.M{"$gt": int64(0)},
			"rev":           bson.M{"$gt": int64(0), "$lte": revision},
		})
		var targets []int64
		if err := dist.Decode(&targets); err != nil {
			return nil, fmt.Errorf("collect superseded revisions: %w", err)
		}

		del, err := b.col.DeleteMany(sctx, bson.M{
			"$or": []bson.M{
				{"rev": bson.M{"$in": targets}},
				{"deleted": true, "rev": bson.M{"$gt": int64(0), "$lte": revision}},
			},
		})
		if err != nil {
			return nil, err
		}
		deleted = del.DeletedCount
		return nil, nil
	})

	if err == server.ErrCompacted {
		updated, lerr := b.loadCompactRevision(ctx)
		if lerr == nil {
			b.mu.Lock()
			b.compactRev = updated
			b.mu.Unlock()
			return updated, server.ErrCompacted
		}
		return current, server.ErrCompacted
	}
	if err != nil {
		return current, fmt.Errorf("compact to %d: %w", revision, err)
	}

	b.mu.Lock()
	b.compactRev = revision
	b.mu.Unlock()

	logrus.Infof("COMPACT removed %d documents, compacted to %d/%d", deleted, revision, cur)
	return revision, nil
}

// compactRevision returns the known compacted revision.
func (b *Backend) compactRevision() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.compactRev
}

var _ = mongo.ErrNoDocuments
