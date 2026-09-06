package mongo

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Compact remove as revisões substituídas e as tombstones até a revisão alvo.
//
// Diferente dos backends SQL, aqui há transação de verdade: o MongoDB suporta
// transações multi-documento em replica set, e este caminho é raro o bastante
// para não sofrer com a contenção que inviabilizou o contador de revisões
// (ver docs/adr/0001-revisao-por-clustertime.md).
//
// A revisão compactada vive no documento de metadados, não numa linha especial
// como nos backends SQL. O compare-and-swap sobre ela é o que impede duas
// instâncias de compactarem em cima uma da outra.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	b.mu.RLock()
	atual := b.compactRev
	b.mu.RUnlock()

	if revision <= atual {
		return atual, server.ErrCompacted
	}

	cur, err := b.CurrentRevision(ctx)
	if err != nil {
		return atual, err
	}
	if revision > cur {
		return atual, server.ErrFutureRev
	}

	sess, err := b.client.StartSession()
	if err != nil {
		return atual, fmt.Errorf("abrir sessão: %w", err)
	}
	defer sess.EndSession(ctx)

	var apagadas int64
	_, err = sess.WithTransaction(ctx, func(sctx context.Context) (interface{}, error) {
		// CAS: só avança se a revisão compactada ainda for a que lemos.
		res, err := b.meta.UpdateOne(sctx,
			bson.M{"_id": metaID, "compact_revision": atual},
			bson.M{"$set": bson.M{"compact_revision": revision}})
		if err != nil {
			return nil, err
		}
		if res.MatchedCount == 0 {
			// Outra instância compactou nesse meio-tempo. É situação normal
			// num cluster multi-servidor, não falha.
			return nil, server.ErrCompacted
		}

		// Apaga o que a revisão alvo tornou obsoleto. Espelha o CompactSQL do
		// driver SQLite (pkg/drivers/sqlite/sqlite.go:91), e a direção importa:
		// o que se apaga são as revisões APONTADAS como anteriores por um
		// documento mais novo — não as que possuem prev_revision. Inverter isso
		// apaga justamente o registro mais recente de cada chave.
		dist := b.col.Distinct(sctx, "prev_revision", bson.M{
			"name":          bson.M{"$ne": compactRevKey},
			"prev_revision": bson.M{"$gt": int64(0)},
			"rev":           bson.M{"$gt": int64(0), "$lte": revision},
		})
		var alvos []int64
		if err := dist.Decode(&alvos); err != nil {
			return nil, fmt.Errorf("coletar revisões substituídas: %w", err)
		}

		del, err := b.col.DeleteMany(sctx, bson.M{
			"$or": []bson.M{
				// as revisões que foram sobrescritas
				{"rev": bson.M{"$in": alvos}},
				// e as tombstones já assimiladas por todos os watchers
				{"deleted": true, "rev": bson.M{"$gt": int64(0), "$lte": revision}},
			},
		})
		if err != nil {
			return nil, err
		}
		apagadas = del.DeletedCount
		return nil, nil
	})

	if err == server.ErrCompacted {
		novo, lerr := b.loadCompactRevision(ctx)
		if lerr == nil {
			b.mu.Lock()
			b.compactRev = novo
			b.mu.Unlock()
			return novo, server.ErrCompacted
		}
		return atual, server.ErrCompacted
	}
	if err != nil {
		return atual, fmt.Errorf("compactar até %d: %w", revision, err)
	}

	b.mu.Lock()
	b.compactRev = revision
	b.mu.Unlock()

	logrus.Infof("COMPACT removeu %d documentos, compactado até %d/%d", apagadas, revision, cur)
	return revision, nil
}

// compactRevision devolve a revisão compactada conhecida.
func (b *Backend) compactRevision() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.compactRev
}

var _ = mongo.ErrNoDocuments
