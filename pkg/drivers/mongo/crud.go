package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// append writes a record to the log and returns the assigned revision.
func (b *Backend) append(ctx context.Context, r *Record) (rev int64, err error) {
	start := time.Now()
	defer func() { observe("append", start, err) }()

	sess, err := b.client.StartSession()
	if err != nil {
		return 0, fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	sctx := mongo.NewSessionContext(ctx, sess)

	res, err := b.col.InsertOne(sctx, r)
	if err != nil {
		return 0, err
	}

	ts := sess.OperationTime()
	if ts == nil {
		return 0, fmt.Errorf("MongoDB returned no operationTime: cannot derive the revision")
	}
	rev, err = EncodeRevision(*ts, b.cfg.EpochBase)
	if err != nil {
		return 0, err
	}

	set := bson.M{"rev": rev}
	if r.Created {
		set["create_revision"] = rev
	}
	if _, err := b.col.UpdateByID(sctx, res.InsertedID, bson.M{"$set": set}); err != nil {
		return 0, fmt.Errorf("write revision %d: %w", rev, err)
	}

	r.Rev = rev
	b.observeRevision(rev)
	return rev, nil
}

// currentRecord returns the most recent record for a key.
func (b *Backend) currentRecord(ctx context.Context, key string) (*Record, error) {
	var r Record
	err := b.col.FindOne(ctx,
		bson.M{"name": key, "rev": bson.M{"$gt": 0}},
		options.FindOne().SetSort(bson.D{{Key: "rev", Value: -1}}),
	).Decode(&r)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// recordAt returns a key's record as it stood at a given revision.
func (b *Backend) recordAt(ctx context.Context, key string, revision int64) (*Record, error) {
	if revision <= 0 {
		return b.currentRecord(ctx, key)
	}
	var r Record
	err := b.col.FindOne(ctx,
		bson.M{"name": key, "rev": bson.M{"$gt": 0, "$lte": revision}},
		options.FindOne().SetSort(bson.D{{Key: "rev", Value: -1}}),
	).Decode(&r)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Record) toKV() *server.KeyValue {
	if r == nil {
		return nil
	}
	return &server.KeyValue{
		Key:            r.Name,
		Value:          r.Value,
		Version:        r.Version,
		CreateRevision: r.CreateRevision,
		ModRevision:    r.Rev,
		Lease:          r.Lease,
	}
}

// Get returns a key's value, optionally at a historical revision.
func (b *Backend) Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	rev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	if revision > 0 {
		b.mu.RLock()
		cr := b.compactRev
		b.mu.RUnlock()
		if cr > 0 && revision < cr {
			return rev, nil, server.ErrCompacted
		}
	}

	r, err := b.recordAt(ctx, key, revision)
	if err != nil {
		return rev, nil, err
	}
	if r == nil || r.Deleted {
		return rev, nil, nil
	}
	kv := r.toKV()
	if keysOnly {
		kv.Value = nil
	}
	return rev, kv, nil
}

// Create inserts a key that does not yet exist.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	existing, err := b.currentRecord(ctx, key)
	if err != nil {
		return 0, err
	}
	if existing != nil && !existing.Deleted {
		return 0, server.ErrKeyExists
	}

	prev := int64(0)
	if existing != nil {
		prev = existing.Rev
	}

	rev, err := b.append(ctx, &Record{
		Name:         key,
		Created:      true,
		PrevRevision: prev,
		Lease:        lease,
		Value:        value,
		Version:      1,
	})
	if mongo.IsDuplicateKeyError(err) {
		return 0, server.ErrKeyExists
	}
	return rev, err
}

// Update replaces a key's value if the given revision is the current one.
func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	cur, err := b.currentRecord(ctx, key)
	if err != nil {
		return 0, nil, false, err
	}
	rev, _ := b.CurrentRevision(ctx)

	if cur == nil || cur.Deleted {
		return rev, nil, false, nil
	}
	if revision != 0 && cur.Rev != revision {
		return rev, cur.toKV(), false, nil
	}

	updated := &Record{
		Name:           key,
		CreateRevision: cur.CreateRevision,
		PrevRevision:   cur.Rev,
		Lease:          lease,
		Value:          value,
		OldValue:       cur.Value,
		Version:        cur.Version + 1,
	}
	newRev, err := b.append(ctx, updated)
	if mongo.IsDuplicateKeyError(err) {
		return rev, cur.toKV(), false, nil
	}
	if err != nil {
		return rev, nil, false, err
	}
	updated.CreateRevision = cur.CreateRevision
	return newRev, updated.toKV(), true, nil
}

// Delete marks a key as deleted by writing a tombstone to the log.
func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	cur, err := b.currentRecord(ctx, key)
	if err != nil {
		return 0, nil, false, err
	}
	rev, _ := b.CurrentRevision(ctx)

	if cur == nil || cur.Deleted {
		return rev, nil, true, nil
	}
	if revision != 0 && cur.Rev != revision {
		return rev, cur.toKV(), false, nil
	}

	tomb := &Record{
		Name:           key,
		Deleted:        true,
		CreateRevision: cur.CreateRevision,
		PrevRevision:   cur.Rev,
		Lease:          cur.Lease,
		OldValue:       cur.Value,
		Version:        cur.Version + 1,
	}
	if _, err := b.append(ctx, tomb); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return rev, cur.toKV(), false, nil
		}
		return rev, nil, false, err
	}
	logrus.Tracef("DELETE %s, rev=%d", key, tomb.Rev)
	return tomb.Rev, cur.toKV(), true, nil
}
