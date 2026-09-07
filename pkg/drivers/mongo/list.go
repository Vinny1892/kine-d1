package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// rangeFilter builds the key filter.
func rangeFilter(key, end string) bson.M {
	if end == "" {
		return bson.M{"name": key}
	}
	return bson.M{"name": bson.M{"$gte": key, "$lt": end}}
}

// currentPipeline builds the aggregation returning each key's latest revision.
func currentPipeline(key, end string, revision, limit int64, includeDeleted, keysOnly bool) mongo.Pipeline {
	match := rangeFilter(key, end)
	match["rev"] = bson.M{"$gt": int64(0)}
	if revision > 0 {
		match["rev"] = bson.M{"$gt": int64(0), "$lte": revision}
	}

	p := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$sort", Value: bson.D{{Key: "name", Value: 1}, {Key: "rev", Value: -1}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$name"},
			{Key: "doc", Value: bson.M{"$first": "$$ROOT"}},
		}}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$doc"}}},
	}
	if !includeDeleted {
		p = append(p, bson.D{{Key: "$match", Value: bson.M{"deleted": false}}})
	}
	p = append(p, bson.D{{Key: "$sort", Value: bson.D{{Key: "name", Value: 1}}}})
	if limit > 0 {
		p = append(p, bson.D{{Key: "$limit", Value: limit}})
	}
	if keysOnly {
		p = append(p, bson.D{{Key: "$project", Value: bson.M{"value": 0, "old_value": 0}}})
	}
	return p
}

// List returns the current state of the keys in a range.
func (b *Backend) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (_ int64, _ []*server.KeyValue, err error) {
	start := time.Now()
	op := "list"
	if keysOnly {
		op = "list_keysonly"
	}
	defer func() { observe(op, start, err) }()

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

	cur, err := b.col.Aggregate(ctx, currentPipeline(key, end, revision, limit, false, keysOnly))
	if err != nil {
		return rev, nil, fmt.Errorf("list %q: %w", key, err)
	}
	defer cur.Close(ctx)

	var out []*server.KeyValue
	for cur.Next(ctx) {
		var r Record
		if err := cur.Decode(&r); err != nil {
			return rev, nil, err
		}
		out = append(out, r.toKV())
	}
	return rev, out, cur.Err()
}

// Count returns how many keys exist in the range.
func (b *Backend) Count(ctx context.Context, key, end string, revision int64) (_ int64, _ int64, err error) {
	start := time.Now()
	defer func() { observe("count", start, err) }()

	rev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, err
	}

	p := currentPipeline(key, end, revision, 0, false, true)
	p = append(p, bson.D{{Key: "$count", Value: "n"}})

	cur, err := b.col.Aggregate(ctx, p)
	if err != nil {
		return rev, 0, err
	}
	defer cur.Close(ctx)

	if !cur.Next(ctx) {
		return rev, 0, cur.Err()
	}
	var res struct {
		N int64 `bson:"n"`
	}
	if err := cur.Decode(&res); err != nil {
		return rev, 0, err
	}
	return rev, res.N, nil
}

// after returns records with a revision greater than the given one, in order.
func (b *Backend) after(ctx context.Context, key, end string, rev, limit int64) ([]*Record, error) {
	filtro := bson.M{"rev": bson.M{"$gt": rev}}
	if key != "" {
		for k, v := range rangeFilter(key, end) {
			filtro[k] = v
		}
	}
	opt := options.Find().SetSort(bson.D{{Key: "rev", Value: 1}})
	if limit > 0 {
		opt.SetLimit(limit)
	}
	cur, err := b.col.Find(ctx, filtro, opt)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []*Record
	for cur.Next(ctx) {
		var r Record
		if err := cur.Decode(&r); err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	return out, cur.Err()
}
