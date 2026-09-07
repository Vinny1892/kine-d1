package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

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
//
// Existe UM único change stream por processo, compartilhado por todos os
// watchers através do pkg/broadcaster (MW-2). Antes cada Watch abria o seu, o
// que multiplicava conexões — um apiserver tem dezenas de informers, e o M0
// admite 500 conexões no total.

// Códigos de erro do MongoDB que significam "o stream não pode ser retomado
// de onde parou". Ver MW-3.
const (
	errChangeStreamHistoryLost = 286
	errChangeStreamFatalError  = 280
)

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

	// A inscrição acontece ANTES de buscar o histórico. A ordem importa: se
	// fosse o contrário, um evento que ocorresse entre o fim da leitura
	// histórica e a inscrição se perderia. Inscrito primeiro, o que chegar
	// nesse intervalo fica no buffer do canal e é filtrado depois pela
	// revisão de corte.
	ao_vivo, err := b.broadcaster.Subscribe(ctx, b.conectarStream)
	if err != nil {
		errc <- fmt.Errorf("assinar o change stream: %w", err)
		close(eventos)
		close(errc)
		return server.WatchResult{CurrentRevision: rev, CompactRevision: compact, Errorc: errc}
	}

	go b.repassar(ctx, key, end, revision, ao_vivo, eventos, errc)

	return server.WatchResult{
		CurrentRevision: rev,
		CompactRevision: compact,
		Events:          eventos,
		Errorc:          errc,
	}
}

// repassar entrega primeiro o histórico pedido e depois filtra o fluxo ao vivo
// para o range deste watcher.
func (b *Backend) repassar(ctx context.Context, key, end string, revision int64,
	ao_vivo <-chan server.Events, eventos chan []*server.Event, errc chan error) {

	defer close(eventos)
	defer close(errc)

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

	for {
		select {
		case <-ctx.Done():
			return
		case lote, ok := <-ao_vivo:
			if !ok {
				return
			}
			var meus []*server.Event
			for _, e := range lote {
				if e.KV == nil || e.KV.ModRevision <= corte {
					continue // já coberto pela fase histórica
				}
				if !inRange(e.KV.Key, key, end) {
					continue
				}
				corte = e.KV.ModRevision
				meus = append(meus, e)
			}
			if len(meus) > 0 {
				select {
				case eventos <- meus:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// conectarStream é a ConnectFunc do broadcaster: chamada uma única vez, na
// primeira inscrição. Devolve o canal que o loop do stream alimenta.
func (b *Backend) conectarStream() (chan server.Events, error) {
	saida := make(chan server.Events, 100)
	go b.laçoDoStream(saida)
	return saida, nil
}

// laçoDoStream mantém um change stream vivo, reabrindo-o quando cai.
//
// MW-3: um stream que fica para trás da janela do oplog é invalidado, e o
// MongoDB recusa retomá-lo pelo resume token. Quando isso acontece, o buraco é
// preenchido lendo a coleção diretamente entre a última revisão entregue e
// agora, e só então um stream novo é aberto. Sem isso o watch morreria em
// silêncio — e no M0 o oplog é pequeno e não configurável, então basta o kine
// ficar alguns minutos lento para cair nesse caso.
func (b *Backend) laçoDoStream(saida chan server.Events) {
	defer close(saida)

	var token bson.Raw
	espera := time.Second

	for {
		if b.ctx.Err() != nil {
			return
		}

		cs, err := b.abrirStream(token)
		if err != nil {
			if b.ctx.Err() != nil {
				return
			}
			ChangeStreamReconnects.WithLabelValues("falha_ao_abrir").Inc()
			logrus.Errorf("Falha ao abrir o change stream: %v", err)
			if !b.dormir(espera) {
				return
			}
			espera = min(espera*2, 30*time.Second)
			continue
		}
		espera = time.Second

		b.streams.Add(1)
		token = b.consumir(cs, saida, token)
		b.streams.Add(-1)
		cs.Close(context.Background())
	}
}

// consumir lê o stream até ele cair, devolvendo o resume token mais recente.
func (b *Backend) consumir(cs *mongo.ChangeStream, saida chan server.Events, token bson.Raw) bson.Raw {
	for cs.Next(b.ctx) {
		var ev struct {
			ClusterTime  bson.Timestamp `bson:"clusterTime"`
			FullDocument *Record        `bson:"fullDocument"`
		}
		if err := cs.Decode(&ev); err != nil {
			logrus.Errorf("Falha ao decodificar evento do change stream: %v", err)
			continue
		}
		token = cs.ResumeToken()

		r := ev.FullDocument
		if r == nil {
			continue
		}
		// A revisão vem do clusterTime do evento, não do campo `rev` do
		// documento. São o mesmo valor — o clusterTime do insert é o que a
		// escrita grava —, mas o do evento já está disponível aqui e chega na
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
		b.observeRevision(rev)

		select {
		case saida <- recordsToEvents([]*Record{r}):
		case <-b.ctx.Done():
			return token
		}
	}

	err := cs.Err()
	if err == nil || errors.Is(err, context.Canceled) {
		return token
	}

	if historicoPerdido(err) {
		ChangeStreamReconnects.WithLabelValues("historico_perdido").Inc()
		logrus.Warnf("Change stream invalidado (%v): recuperando por leitura direta", err)
		if b.recuperar(saida) {
			return nil // recomeça sem token: a janela já foi coberta
		}
	}
	ChangeStreamReconnects.WithLabelValues("queda").Inc()
	logrus.Errorf("Change stream caiu: %v", err)
	return token
}

// recuperar preenche o intervalo perdido lendo a coleção diretamente, da última
// revisão observada até o fim. Devolve false se não conseguiu.
func (b *Backend) recuperar(saida chan server.Events) bool {
	b.mu.RLock()
	desde := b.currentRev
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(b.ctx, 2*time.Minute)
	defer cancel()

	perdidos, err := b.after(ctx, "", "", desde, 0)
	if err != nil {
		logrus.Errorf("Falha ao recuperar o intervalo perdido do watch: %v", err)
		return false
	}
	if len(perdidos) == 0 {
		return true
	}
	logrus.Infof("Recuperados %d eventos perdidos pela invalidação do change stream", len(perdidos))
	for _, r := range perdidos {
		b.observeRevision(r.Rev)
		select {
		case saida <- recordsToEvents([]*Record{r}):
		case <-b.ctx.Done():
			return false
		}
	}
	return true
}

// abrirStream abre o change stream, retomando pelo token quando houver.
func (b *Backend) abrirStream(token bson.Raw) (*mongo.ChangeStream, error) {
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
	if len(token) > 0 {
		opts.SetResumeAfter(token)
	} else {
		b.mu.RLock()
		rev := b.currentRev
		b.mu.RUnlock()
		if rev > 0 {
			ts := DecodeRevision(rev, b.cfg.EpochBase)
			opts.SetStartAtOperationTime(&ts)
		}
	}
	return b.col.Watch(b.ctx, pipeline, opts)
}

// historicoPerdido diz se o erro significa que o oplog já não cobre o ponto de
// retomada — o caso que exige recuperação por leitura direta.
func historicoPerdido(err error) bool {
	var ce mongo.ServerError
	if errors.As(err, &ce) {
		return ce.HasErrorCode(errChangeStreamHistoryLost) ||
			ce.HasErrorCode(errChangeStreamFatalError)
	}
	return false
}

func (b *Backend) dormir(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-b.ctx.Done():
		return false
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
				Version:        r.Version - 1,
				CreateRevision: r.CreateRevision,
				ModRevision:    r.PrevRevision,
			}
		}
		out = append(out, e)
	}
	return out
}
