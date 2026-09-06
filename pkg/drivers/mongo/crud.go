package mongo

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// append grava um registro no log e devolve a revisão atribuída.
//
// A revisão vem do clusterTime da própria escrita (session.OperationTime), que
// só é conhecido DEPOIS do insert. Por isso são duas operações: o insert e um
// update que grava o campo `rev`.
//
// Medido no MSPIKE-5: 30,7ms para o insert sozinho contra 59,5ms com o update.
// Envolver as duas numa transação piora (80,9ms) sem ganho real — se o processo
// morrer entre elas, o documento fica com rev=0 e é invisível para o watch e
// para o List, que filtram por rev. Um documento órfão assim é inerte, não
// corrompe nada, e a chave pode ser reescrita porque a constraint
// (name, prev_revision) continua valendo.
func (b *Backend) append(ctx context.Context, r *Record) (int64, error) {
	sess, err := b.client.StartSession()
	if err != nil {
		return 0, fmt.Errorf("abrir sessão: %w", err)
	}
	defer sess.EndSession(ctx)

	sctx := mongo.NewSessionContext(ctx, sess)

	res, err := b.col.InsertOne(sctx, r)
	if err != nil {
		return 0, err
	}

	ts := sess.OperationTime()
	if ts == nil {
		return 0, fmt.Errorf("MongoDB não devolveu operationTime: impossível derivar a revisão")
	}
	rev, err := EncodeRevision(*ts, b.cfg.EpochBase)
	if err != nil {
		return 0, err
	}

	set := bson.M{"rev": rev}
	if r.Created {
		// Numa criação a revisão de criação é a própria.
		set["create_revision"] = rev
	}
	if _, err := b.col.UpdateByID(sctx, res.InsertedID, bson.M{"$set": set}); err != nil {
		return 0, fmt.Errorf("gravar revisão %d: %w", rev, err)
	}

	r.Rev = rev
	b.observeRevision(rev)
	return rev, nil
}

// currentRecord devolve o registro mais recente de uma chave.
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

// recordAt devolve o registro de uma chave como estava numa dada revisão.
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
		CreateRevision: r.CreateRevision,
		ModRevision:    r.Rev,
		Lease:          r.Lease,
	}
}

// Get devolve o valor de uma chave, opcionalmente numa revisão histórica.
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

// Create insere uma chave que ainda não existe.
//
// A exclusividade vem do índice único (name, prev_revision): uma criação usa
// prev_revision=0, então duas criações concorrentes da mesma chave colidem e a
// segunda recebe ErrKeyExists — o mesmo mecanismo do driver SQLite.
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
		ExpiresAt:    b.expiryFor(lease),
	})
	if mongo.IsDuplicateKeyError(err) {
		return 0, server.ErrKeyExists
	}
	return rev, err
}

// Update substitui o valor de uma chave, se a revisão informada for a corrente.
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
		// Conflito de revisão: devolve o valor atual para o chamador comparar.
		return rev, cur.toKV(), false, nil
	}

	novo := &Record{
		Name:           key,
		CreateRevision: cur.CreateRevision,
		PrevRevision:   cur.Rev,
		Lease:          lease,
		Value:          value,
		OldValue:       cur.Value,
		ExpiresAt:      b.expiryFor(lease),
	}
	newRev, err := b.append(ctx, novo)
	if mongo.IsDuplicateKeyError(err) {
		// Outra escrita venceu a corrida por esta mesma revisão anterior.
		return rev, cur.toKV(), false, nil
	}
	if err != nil {
		return rev, nil, false, err
	}
	novo.CreateRevision = cur.CreateRevision
	return newRev, novo.toKV(), true, nil
}

// Delete marca uma chave como apagada, gravando uma tombstone no log.
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
