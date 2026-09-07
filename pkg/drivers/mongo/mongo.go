// Package mongo implementa um backend MongoDB para o kine.
//
// Diferente dos drivers SQLite/PostgreSQL/MySQL, que compartilham o SQL de
// pkg/drivers/generic através da interface server.Dialect, este driver
// implementa server.Backend diretamente — como fazem os drivers nats e t4.
// O motivo é estrutural: server.Dialect devolve *sql.Rows, e MongoDB não fala
// SQL. Ver IDEA.md, seção 3.
//
// As duas decisões de projeto que moldam o resto do pacote:
//
//   - A revisão do etcd vem do clusterTime do MongoDB, não de um contador.
//     Ver revision.go e docs/adr/0001-revisao-por-clustertime.md.
//   - O watch usa Change Streams, dispensando o laço de polling que os
//     backends SQL rodam uma vez por segundo.
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

// Backend implementa server.Backend sobre MongoDB.
type Backend struct {
	cfg    *Config
	client *mongo.Client
	col    *mongo.Collection
	meta   *mongo.Collection

	// currentRev é a última revisão conhecida. Mantida atualizada tanto pelas
	// escritas locais quanto pelo Change Stream, que vê também as escritas de
	// outras instâncias.
	mu         sync.RWMutex
	currentRev int64
	compactRev int64
	// ultimoEvento é quando o change stream entregou algo pela última vez.
	// Alimenta o kine_mongo_change_stream_lag_seconds, que é a métrica que
	// detecta o modo de falha mais provável: sob saturação o Atlas não devolve
	// erro, só atrasa (MSPIKE-6).
	ultimoEvento time.Time

	// notify acorda quem espera por WaitForSyncTo.
	synced *sync.Cond

	broadcaster broadcaster.Broadcaster
	ctx         context.Context

	// streams conta quantos change streams este processo tem abertos. Só o
	// MW-2 depende disso: o valor deve ser 1 por processo, não 1 por watcher.
	streams atomic.Int64
}

func init() {
	drivers.Register("mongodb", New)
	drivers.Register("mongodb+srv", New)
}

// New constrói o backend. Devolve leaderElect=true porque o MongoDB é um
// datastore compartilhado: todos os servidores do plano de controle enxergam o
// mesmo estado, e só o líder deve compactar.
func New(ctx context.Context, wg *sync.WaitGroup, drvCfg *drivers.Config) (bool, server.Backend, error) {
	// O util.SchemeAndAddress do kine corta o scheme da DSN; aqui precisamos
	// dele de volta, porque mongodb+srv:// muda a resolução de host.
	dsn := drvCfg.Endpoint
	if dsn == "" {
		return false, nil, fmt.Errorf("endpoint vazio: informe uma connection string mongodb://")
	}

	cfg, err := ParseDSN(dsn)
	if err != nil {
		return false, nil, err
	}

	// readConcern/writeConcern majority são obrigatórios: o kine precisa ler a
	// revisão que acabou de gravar. Medido no MSPIKE-1 — majority custa 26,6ms
	// contra 27,2ms do default, latência indistinguível.
	opts := options.Client().
		ApplyURI(cfg.URI).
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority()).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetRetryWrites(true)

	client, err := mongo.Connect(opts)
	if err != nil {
		return false, nil, fmt.Errorf("conectar ao MongoDB: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ServerSelectionTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return false, nil, fmt.Errorf("ping ao MongoDB: %w", err)
	}

	db := client.Database(cfg.Database)
	b := &Backend{
		cfg:    cfg,
		client: client,
		col:    db.Collection(cfg.Collection),
		meta:   db.Collection(cfg.Collection + metaCollSuffix),
	}
	b.synced = sync.NewCond(b.mu.RLocker())

	registrarMetricas(drvCfg.MetricsRegisterer)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logrus.Info("Fechando conexão com o MongoDB…")
		if err := client.Disconnect(context.Background()); err != nil {
			logrus.Errorf("Falha ao desconectar do MongoDB: %v", err)
		}
	}()

	return true, b, nil
}

// Start prepara o schema e carrega o estado inicial.
func (b *Backend) Start(ctx context.Context) error {
	b.ctx = ctx

	if err := b.setup(ctx); err != nil {
		return err
	}

	// O kine espera que a chave compact_rev_key exista — é dela que os
	// backends SQL leem a revisão compactada.
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

	logrus.Infof("MongoDB iniciado: revisão atual=%d, revisão compactada=%d", rev, cr)
	go ttl.Run(ctx, b)
	go b.coletarGauges(ctx)
	return nil
}

// coletarGauges atualiza periodicamente as métricas que não vêm de operações:
// revisões, uso de storage e atraso do change stream.
//
// O intervalo é longo de propósito. Um collStats a cada poucos segundos
// consumiria parte do orçamento de operações do M0 — e o que se está medindo é
// justamente a saturação desse orçamento.
func (b *Backend) coletarGauges(ctx context.Context) {
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
		return fmt.Errorf("verificar compact_rev_key: %w", err)
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
		return nil // outra instância criou primeiro
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
		return 0, fmt.Errorf("ler revisão atual: %w", err)
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
		return 0, fmt.Errorf("ler revisão compactada: %w", err)
	}
	return meta.CompactRevision, nil
}

// CurrentRevision devolve a última revisão conhecida.
func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	b.mu.RLock()
	rev := b.currentRev
	b.mu.RUnlock()
	if rev != 0 {
		return rev, nil
	}
	return b.loadCurrentRevision(ctx)
}

// DbSize devolve o tamanho ocupado, somando dados e índices.
//
// Relevante no M0, cujo teto de 512 MB não expande: ao encher, o cluster para.
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

// WaitForSyncTo bloqueia até que a revisão informada tenha sido observada.
func (b *Backend) WaitForSyncTo(revision int64) {
	b.mu.RLock()
	for b.currentRev < revision {
		b.synced.Wait()
	}
	b.mu.RUnlock()
}

// streamsAbertos devolve quantos change streams este processo mantém.
func (b *Backend) streamsAbertos() int64 { return b.streams.Load() }

// observeRevision registra uma revisão vista e acorda quem espera por ela.
func (b *Backend) observeRevision(rev int64) {
	b.mu.Lock()
	if rev > b.currentRev {
		b.currentRev = rev
	}
	b.ultimoEvento = time.Now()
	b.mu.Unlock()
	b.synced.Broadcast()
}
