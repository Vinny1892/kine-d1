package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// O watch usa Change Streams em vez do laço de polling que os backends SQL
// rodam uma vez por segundo (pkg/logstructured/sqllog/sql.go:486). Além de
// derrubar a latência de watch de ~1s para dezenas de milissegundos, isso
// elimina as 86.400 queries/dia que um cluster parado fazia.
//
// A ordenação é de graça: como a revisão É o clusterTime (revision.go), a
// ordem em que o oplog entrega os eventos é a ordem das revisões. Medido no
// MSPIKE-4: zero eventos fora de ordem em 40, contra 13 de 29 quando a
// revisão vinha de um contador.

// Watch entrega os eventos de um range a partir de uma revisão.
func (b *Backend) Watch(ctx context.Context, key, end string, revision int64) server.WatchResult {
	eventos := make(chan []*server.Event, 100)
	errc := make(chan error, 1)

	rev, err := b.CurrentRevision(ctx)
	if err != nil {
		errc <- err
		close(eventos)
		close(errc)
		return server.WatchResult{Errorc: errc, Events: eventos}
	}

	compact := b.compactRevision()
	if revision > 0 && compact > 0 && revision < compact {
		errc <- server.ErrCompacted
		close(eventos)
		close(errc)
		return server.WatchResult{CurrentRevision: rev, CompactRevision: compact, Errorc: errc}
	}

	go b.watchLoop(ctx, key, end, revision, eventos, errc)

	return server.WatchResult{
		CurrentRevision: rev,
		CompactRevision: compact,
		Events:          eventos,
		Errorc:          errc,
	}
}

func (b *Backend) watchLoop(ctx context.Context, key, end string, revision int64,
	eventos chan []*server.Event, errc chan error) {

	defer close(eventos)
	defer close(errc)

	// 1) Recuperação histórica: tudo que aconteceu entre a revisão pedida e
	//    agora precisa ser entregue antes de ligar o stream ao vivo. Sem isso,
	//    um watcher que reconecta perde a janela.
	corte := revision
	if corte > 0 {
		antigos, err := b.after(ctx, key, end, corte, 0)
		if err != nil {
			errc <- err
			return
		}
		if len(antigos) > 0 {
			eventos <- recordsToEvents(antigos)
			corte = antigos[len(antigos)-1].Rev
		}
	} else {
		atual, err := b.CurrentRevision(ctx)
		if err != nil {
			errc <- err
			return
		}
		corte = atual
	}

	// 2) Stream ao vivo, retomado a partir do clusterTime correspondente à
	//    última revisão já entregue — assim a emenda com o histórico não tem
	//    buraco nem duplicata.
	// Só inserts interessam. O kine é um log append-only: toda mutação — criar,
	// atualizar, apagar — insere um documento novo. O único update que existe é
	// o que grava o campo `rev` logo depois do insert (ver crud.go), e ele não
	// representa mutação nenhuma.
	//
	// Escutar apenas inserts é o que torna a ordem correta de graça: a revisão
	// É o clusterTime do insert, e o oplog entrega os eventos nessa ordem. Os
	// updates, por serem uma segunda viagem de rede, podem chegar embaralhados
	// entre escritas concorrentes — foi o que travou o apiserver em
	// autoregister-completion.
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
	}

	opts := options.ChangeStream()
	if corte > 0 {
		ts := DecodeRevision(corte, b.cfg.EpochBase)
		opts.SetStartAtOperationTime(&ts)
	}

	cs, err := b.col.Watch(ctx, pipeline, opts)
	if err != nil {
		errc <- fmt.Errorf("abrir change stream: %w", err)
		return
	}
	defer cs.Close(context.Background())

	for cs.Next(ctx) {
		var ev struct {
			ClusterTime  bson.Timestamp `bson:"clusterTime"`
			FullDocument *Record        `bson:"fullDocument"`
		}
		if err := cs.Decode(&ev); err != nil {
			logrus.Errorf("Falha ao decodificar evento do change stream: %v", err)
			continue
		}
		r := ev.FullDocument
		if r == nil {
			continue
		}
		// A revisão vem do clusterTime do evento, não do campo `rev` do
		// documento. São o mesmo valor — o clusterTime do insert é o que a
		// escrita gravou —, mas o do evento já está disponível aqui e chega na
		// ordem do oplog, enquanto o campo depende de um update posterior.
		rev, err := EncodeRevision(ev.ClusterTime, b.cfg.EpochBase)
		if err != nil {
			logrus.Errorf("clusterTime inválido no change stream: %v", err)
			continue
		}
		r.Rev = rev
		if r.Created {
			r.CreateRevision = rev
		}
		if rev <= corte {
			continue // já entregue na fase histórica
		}
		if !inRange(r.Name, key, end) {
			b.observeRevision(rev)
			continue
		}
		corte = rev
		b.observeRevision(rev)
		eventos <- recordsToEvents([]*Record{r})
	}

	if err := cs.Err(); err != nil && !errors.Is(err, context.Canceled) {
		logrus.Errorf("Change stream encerrado com erro: %v", err)
		errc <- err
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
				CreateRevision: r.CreateRevision,
				ModRevision:    r.PrevRevision,
			}
		}
		out = append(out, e)
	}
	return out
}
