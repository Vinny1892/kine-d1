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
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"operationType": bson.M{"$in": []string{"insert", "update", "replace"}},
		}}},
	}

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
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
			FullDocument *Record `bson:"fullDocument"`
		}
		if err := cs.Decode(&ev); err != nil {
			logrus.Errorf("Falha ao decodificar evento do change stream: %v", err)
			continue
		}
		r := ev.FullDocument
		// Documentos ainda sem revisão são o intervalo entre o insert e o
		// update que grava o `rev` (ver crud.go). O update subsequente gera
		// outro evento, já com a revisão preenchida.
		if r == nil || r.Rev == 0 {
			continue
		}
		if r.Rev <= corte {
			continue // já entregue na fase histórica
		}
		if !inRange(r.Name, key, end) {
			b.observeRevision(r.Rev)
			continue
		}
		corte = r.Rev
		b.observeRevision(r.Rev)
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
