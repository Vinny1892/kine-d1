package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Documento do log de revisões. É o equivalente BSON da tabela `kine` do
// driver SQLite (pkg/drivers/sqlite/sqlite.go:25).
//
// Diferença estrutural: em SQL a revisão é o `id` AUTOINCREMENT; aqui é o
// campo Rev, derivado do clusterTime (ver revision.go). O `_id` continua sendo
// o ObjectId natural do MongoDB, porque a revisão só é conhecida DEPOIS da
// escrita e não pode servir de chave primária no momento do insert.
type Record struct {
	ID             bson.ObjectID `bson:"_id,omitempty"`
	Rev            int64         `bson:"rev"`
	Name           string        `bson:"name"`
	Created        bool          `bson:"created"`
	Deleted        bool          `bson:"deleted"`
	CreateRevision int64         `bson:"create_revision"`
	PrevRevision   int64         `bson:"prev_revision"`
	Lease          int64         `bson:"lease"`
	Value          []byte        `bson:"value,omitempty"`
	OldValue       []byte        `bson:"old_value,omitempty"`
	ExpiresAt      *time.Time    `bson:"expires_at,omitempty"`
}

// metaDoc guarda o estado do cluster que precisa sobreviver a reinícios.
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
	idxLeaseTTL     = "lease_ttl"
)

// setup cria coleções e índices, e resolve o epoch base do cluster.
//
// É idempotente: CreateIndexes ignora índices já existentes com a mesma
// definição, e o documento de metadados é criado com upsert.
func (b *Backend) setup(ctx context.Context) error {
	logrus.Info("Configurando coleções e índices do MongoDB…")

	idx := []mongo.IndexModel{
		{
			// Get da última revisão de uma chave, e o ListCurrent por prefixo.
			Keys:    bson.D{{Key: "name", Value: 1}, {Key: "rev", Value: -1}},
			Options: options.Index().SetName(idxNameRev),
		},
		{
			// After — a query do watch e da recuperação histórica.
			Keys:    bson.D{{Key: "rev", Value: 1}},
			Options: options.Index().SetName(idxRev),
		},
		{
			// É esta constraint que produz o ErrKeyExists do etcd quando dois
			// clientes tentam criar a mesma chave. Espelha o índice
			// kine_name_prev_revision_uindex do driver SQLite.
			Keys:    bson.D{{Key: "name", Value: 1}, {Key: "prev_revision", Value: 1}},
			Options: options.Index().SetName(idxNamePrevUniq).SetUnique(true),
		},
		{
			// Compactação: encontra as revisões substituídas.
			Keys:    bson.D{{Key: "prev_revision", Value: 1}},
			Options: options.Index().SetName(idxPrevRev),
		},
		{
			// Lease: o próprio MongoDB expira os documentos, dispensando a
			// varredura que o pkg/ttl faz nos backends SQL.
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetName(idxLeaseTTL).SetExpireAfterSeconds(0),
		},
	}

	if _, err := b.col.Indexes().CreateMany(ctx, idx); err != nil {
		return fmt.Errorf("criar índices: %w", err)
	}

	if err := b.resolveEpochBase(ctx); err != nil {
		return err
	}

	logrus.Infof("MongoDB pronto: banco=%s coleção=%s epochBase=%d",
		b.cfg.Database, b.cfg.Collection, b.cfg.EpochBase)
	return nil
}

// resolveEpochBase lê o epoch base gravado ou grava o configurado.
//
// O valor NUNCA pode mudar depois de gravado: ele define o significado de toda
// revisão já entregue ao apiserver. Se a DSN pedir um valor diferente do que
// está no banco, isso é erro de configuração e não uma migração silenciosa.
func (b *Backend) resolveEpochBase(ctx context.Context) error {
	var meta metaDoc
	err := b.meta.FindOne(ctx, bson.M{"_id": metaID}).Decode(&meta)
	switch {
	case err == nil:
		if b.cfg.EpochBase != defaultEpochBase && b.cfg.EpochBase != meta.EpochBase {
			return fmt.Errorf(
				"kine_epoch_base=%d conflita com o valor %d já gravado neste cluster: "+
					"mudá-lo invalidaria todas as revisões existentes",
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
			// Outra instância gravou primeiro — relê e adota o valor dela.
			return b.resolveEpochBase(ctx)
		}
		if err != nil {
			return fmt.Errorf("gravar metadados: %w", err)
		}
		logrus.Infof("Epoch base do cluster definido em %d", b.cfg.EpochBase)
		return nil

	default:
		return fmt.Errorf("ler metadados: %w", err)
	}
}
