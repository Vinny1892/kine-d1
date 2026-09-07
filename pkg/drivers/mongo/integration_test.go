//go:build integration

package mongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Testes de integração contra um MongoDB real. Rodam apenas com a tag
// `integration` e a variável KINE_MONGO_TEST_URI apontando para um cluster:
//
//	go test -tags=integration ./pkg/drivers/mongo/ -v
func testBackend(t *testing.T) (*Backend, context.Context, func()) {
	t.Helper()
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI não definida")
	}
	// Cada execução usa uma coleção própria, para não pisar nas outras.
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	uri += fmt.Sprintf("%skine_database=kine_test&kine_collection=t%d", sep, time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	_, b, err := New(ctx, wg, &drivers.Config{Endpoint: uri})
	if err != nil {
		cancel()
		t.Fatalf("New: %v", err)
	}
	be := b.(*Backend)
	if err := be.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	return be, ctx, func() {
		_ = be.col.Drop(context.Background())
		_ = be.meta.Drop(context.Background())
		cancel()
		wg.Wait()
	}
}

func TestCriarLerAtualizarApagar(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	chave := "/registry/pods/default/nginx"
	valor := []byte("primeiro")

	rev, err := b.Create(ctx, chave, valor, 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rev == 0 {
		t.Fatal("Create devolveu revisão zero")
	}

	_, kv, err := b.Get(ctx, chave, 0, false)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv == nil || string(kv.Value) != "primeiro" {
		t.Fatalf("Get devolveu %v, esperado 'primeiro'", kv)
	}
	if kv.ModRevision != rev {
		t.Errorf("ModRevision=%d, esperado %d", kv.ModRevision, rev)
	}

	// Criar de novo precisa falhar — é o ErrKeyExists do etcd.
	if _, err := b.Create(ctx, chave, []byte("x"), 0); err != server.ErrKeyExists {
		t.Errorf("Create duplicado devolveu %v, esperado ErrKeyExists", err)
	}

	rev2, kv2, ok, err := b.Update(ctx, chave, []byte("segundo"), rev, 0)
	if err != nil || !ok {
		t.Fatalf("Update: err=%v ok=%v", err, ok)
	}
	if rev2 <= rev {
		t.Errorf("revisão não avançou: %d -> %d", rev, rev2)
	}
	if string(kv2.Value) != "segundo" {
		t.Errorf("Update devolveu %q", kv2.Value)
	}

	// A revisão histórica precisa continuar visível.
	_, antigo, err := b.Get(ctx, chave, rev, false)
	if err != nil {
		t.Fatalf("Get histórico: %v", err)
	}
	if antigo == nil || string(antigo.Value) != "primeiro" {
		t.Errorf("Get na revisão %d devolveu %v, esperado 'primeiro'", rev, antigo)
	}

	_, _, ok, err = b.Delete(ctx, chave, rev2)
	if err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}
	_, kv3, err := b.Get(ctx, chave, 0, false)
	if err != nil {
		t.Fatalf("Get após delete: %v", err)
	}
	if kv3 != nil {
		t.Errorf("chave ainda visível após delete: %v", kv3)
	}
}

func TestListEContagem(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("/registry/pods/default/p%02d", i)
		if _, err := b.Create(ctx, k, []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Create %s: %v", k, err)
		}
	}
	// Uma chave fora do prefixo, para provar que o range é respeitado.
	if _, err := b.Create(ctx, "/registry/services/x", []byte("fora"), 0); err != nil {
		t.Fatal(err)
	}

	pref, fim := "/registry/pods/default/", "/registry/pods/default0"

	_, kvs, err := b.List(ctx, pref, fim, 0, 0, false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(kvs) != 10 {
		t.Fatalf("List devolveu %d chaves, esperado 10", len(kvs))
	}
	for i := 1; i < len(kvs); i++ {
		if kvs[i-1].Key >= kvs[i].Key {
			t.Errorf("List fora de ordem: %q antes de %q", kvs[i-1].Key, kvs[i].Key)
		}
	}

	_, n, err := b.Count(ctx, pref, fim, 0)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 10 {
		t.Errorf("Count=%d, esperado 10", n)
	}

	_, lim, err := b.List(ctx, pref, fim, 3, 0, false)
	if err != nil {
		t.Fatalf("List com limite: %v", err)
	}
	if len(lim) != 3 {
		t.Errorf("List com limite devolveu %d, esperado 3", len(lim))
	}

	// keysOnly é o que separa 55ms de 823ms num range grande (MSPIKE-5).
	_, so, err := b.List(ctx, pref, fim, 0, 0, true)
	if err != nil {
		t.Fatalf("List keysOnly: %v", err)
	}
	for _, kv := range so {
		if len(kv.Value) != 0 {
			t.Errorf("keysOnly devolveu valor em %q", kv.Key)
			break
		}
	}
}

func TestRevisoesMonotonicas(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	const n = 20
	revs := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		r, err := b.Create(ctx, fmt.Sprintf("/k/%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		revs = append(revs, r)
	}
	for i := 1; i < len(revs); i++ {
		if revs[i] <= revs[i-1] {
			t.Errorf("revisões não são monotônicas: %d depois de %d", revs[i], revs[i-1])
		}
	}
}

func TestRevisoesConcorrentes(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	const n = 24
	var wg sync.WaitGroup
	revs := make([]int64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			revs[i], errs[i] = b.Create(ctx, fmt.Sprintf("/c/%d", i), []byte("v"), 0)
		}(i)
	}
	wg.Wait()

	vistas := map[int64]bool{}
	for i, err := range errs {
		if err != nil {
			t.Errorf("Create concorrente %d: %v", i, err)
			continue
		}
		if vistas[revs[i]] {
			t.Errorf("revisão duplicada: %d", revs[i])
		}
		vistas[revs[i]] = true
	}
	if len(vistas) != n {
		t.Errorf("%d revisões distintas, esperado %d", len(vistas), n)
	}
}

func TestWatch(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	ctxW, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res := b.Watch(ctxW, "/w/", "/w0", 0)
	if res.Events == nil {
		t.Fatal("Watch não devolveu canal de eventos")
	}
	time.Sleep(2 * time.Second) // deixa o change stream assentar

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := b.Create(ctx, fmt.Sprintf("/w/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	recebidos := 0
	var ultima int64
	prazo := time.After(25 * time.Second)
	for recebidos < n {
		select {
		case lote, ok := <-res.Events:
			if !ok {
				t.Fatalf("canal fechado após %d eventos", recebidos)
			}
			for _, e := range lote {
				if e.KV.ModRevision <= ultima {
					t.Errorf("evento fora de ordem: %d depois de %d", e.KV.ModRevision, ultima)
				}
				ultima = e.KV.ModRevision
				recebidos++
			}
		case err := <-res.Errorc:
			t.Fatalf("erro no watch: %v", err)
		case <-prazo:
			t.Fatalf("timeout: recebeu %d de %d eventos", recebidos, n)
		}
	}
}

func TestWatchComEscritasConcorrentes(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	ctxW, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res := b.Watch(ctxW, "/wc/", "/wc0", 0)
	time.Sleep(time.Second)

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := b.Create(ctx, fmt.Sprintf("/wc/%02d", i), []byte("v"), 0)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Create concorrente: %v", err)
		}
	}

	vistos := 0
	var ultima int64
	for vistos < n {
		select {
		case lote, ok := <-res.Events:
			if !ok {
				t.Fatalf("canal fechado após %d eventos", vistos)
			}
			for _, e := range lote {
				if e.KV.ModRevision <= ultima {
					t.Fatalf("evento fora de ordem: %d depois de %d", e.KV.ModRevision, ultima)
				}
				ultima = e.KV.ModRevision
				vistos++
			}
		case err := <-res.Errorc:
			t.Fatalf("erro no watch: %v", err)
		case <-ctxW.Done():
			t.Fatalf("timeout: recebeu %d de %d eventos", vistos, n)
		}
	}
}

func TestLeaseExpiraComTombstone(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	chave := "/lease/curta"
	if _, err := b.Create(ctx, chave, []byte("temporario"), 1); err != nil {
		t.Fatalf("Create: %v", err)
	}

	prazo := time.Now().Add(10 * time.Second)
	for time.Now().Before(prazo) {
		_, kv, err := b.Get(ctx, chave, 0, false)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if kv == nil {
			var tomb Record
			if err := b.col.FindOne(ctx, bson.M{"name": chave, "deleted": true}).Decode(&tomb); err != nil {
				t.Fatalf("a chave expirou sem tombstone: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("a chave com lease não expirou")
}

func TestCompact(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	chave := "/registry/pods/default/a"
	rev, err := b.Create(ctx, chave, []byte("v0"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		rev, _, _, err = b.Update(ctx, chave, []byte(fmt.Sprintf("v%d", i)), rev, 0)
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
	}

	alvo := rev
	feito, err := b.Compact(ctx, alvo)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if feito != alvo {
		t.Errorf("Compact devolveu %d, esperado %d", feito, alvo)
	}

	// O valor corrente precisa sobreviver à compactação.
	_, kv, err := b.Get(ctx, chave, 0, false)
	if err != nil {
		t.Fatalf("Get após compact: %v", err)
	}
	if kv == nil || string(kv.Value) != "v5" {
		t.Fatalf("após compact, Get devolveu %v, esperado v5", kv)
	}

	// Compactar de novo para a mesma revisão é no-op.
	if _, err := b.Compact(ctx, alvo); err != server.ErrCompacted {
		t.Errorf("Compact repetido devolveu %v, esperado ErrCompacted", err)
	}
}

func TestParseDSN(t *testing.T) {
	cfg, err := ParseDSN("mongodb+srv://u:p@host/meudb?retryWrites=true&kine_collection=c1&kine_epoch_base=123")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if cfg.Database != "meudb" {
		t.Errorf("Database=%q", cfg.Database)
	}
	if cfg.Collection != "c1" {
		t.Errorf("Collection=%q", cfg.Collection)
	}
	if cfg.EpochBase != 123 {
		t.Errorf("EpochBase=%d", cfg.EpochBase)
	}
	if strings.Contains(cfg.URI, "kine_") {
		t.Errorf("parâmetros kine_ vazaram para a URI: %s", cfg.URI)
	}
	if !strings.Contains(cfg.URI, "retryWrites=true") {
		t.Errorf("parâmetro do mongo foi removido: %s", cfg.URI)
	}
	if _, err := ParseDSN("mongodb://h/?kine_desconhecido=1"); err == nil {
		t.Error("parâmetro kine_ desconhecido deveria dar erro")
	}
}

// TestWatchersCompartilhamUmStream cobre o MW-2: vários watchers precisam
// dividir um único change stream, não abrir um cada. Um apiserver tem dezenas
// de informers, e o M0 admite 500 conexões no total.
func TestWatchersCompartilhamUmStream(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	const nWatchers = 8
	ctxW, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	res := make([]server.WatchResult, nWatchers)
	for i := range res {
		res[i] = b.Watch(ctxW, "/s/", "/s0", 0)
		if res[i].Events == nil {
			t.Fatalf("watcher %d não recebeu canal", i)
		}
	}
	time.Sleep(3 * time.Second) // deixa o stream assentar

	// Um único change stream deve estar aberto, independentemente do número
	// de watchers. O broadcaster chama a ConnectFunc uma vez só.
	if n := b.streamsAbertos(); n != 1 {
		t.Errorf("streams abertos = %d, esperado 1 para %d watchers", n, nWatchers)
	}

	const nChaves = 4
	for i := 0; i < nChaves; i++ {
		if _, err := b.Create(ctx, fmt.Sprintf("/s/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	// Todos os watchers precisam ver todos os eventos, em ordem.
	var wg sync.WaitGroup
	falhas := make([]string, nWatchers)
	for i := 0; i < nWatchers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vistos := 0
			var ultima int64
			prazo := time.After(35 * time.Second)
			for vistos < nChaves {
				select {
				case lote, ok := <-res[i].Events:
					if !ok {
						falhas[i] = fmt.Sprintf("canal fechou com %d de %d", vistos, nChaves)
						return
					}
					for _, e := range lote {
						if e.KV.ModRevision <= ultima {
							falhas[i] = fmt.Sprintf("fora de ordem: %d após %d", e.KV.ModRevision, ultima)
							return
						}
						ultima = e.KV.ModRevision
						vistos++
					}
				case <-prazo:
					falhas[i] = fmt.Sprintf("timeout com %d de %d eventos", vistos, nChaves)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for i, f := range falhas {
		if f != "" {
			t.Errorf("watcher %d: %s", i, f)
		}
	}
}

// TestRecuperacaoDeStreamInvalidado cobre o MW-3.
//
// Ressalva importante sobre o alcance deste teste: ele exercita o mecanismo de
// recuperação, não o gatilho. Provocar uma invalidação real exigiria encher a
// janela do oplog, o que não é viável no M0 — isso fica para o MSPIKE-8. O que
// se prova aqui é que, dada uma lacuna entre a última revisão observada e o
// estado atual, a leitura direta a preenche na ordem correta e sem repetir.
func TestRecuperacaoDeStreamInvalidado(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	// Marca o ponto a partir do qual os eventos serão considerados perdidos.
	if _, err := b.Create(ctx, "/r/marco", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	b.mu.RLock()
	desde := b.currentRev
	b.mu.RUnlock()

	const n = 6
	for i := 0; i < n; i++ {
		if _, err := b.Create(ctx, fmt.Sprintf("/r/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	// Rebobina a revisão observada, simulando um stream que ficou para trás.
	b.mu.Lock()
	b.currentRev = desde
	b.mu.Unlock()

	saida := make(chan server.Events, 64)
	if ok := b.recuperar(saida); !ok {
		t.Fatal("recuperar() devolveu false")
	}
	close(saida)

	var revs []int64
	chaves := map[string]bool{}
	for lote := range saida {
		for _, e := range lote {
			revs = append(revs, e.KV.ModRevision)
			chaves[e.KV.Key] = true
		}
	}

	if len(revs) != n {
		t.Fatalf("recuperou %d eventos, esperado %d", len(revs), n)
	}
	for i := 1; i < len(revs); i++ {
		if revs[i] <= revs[i-1] {
			t.Errorf("recuperação fora de ordem: %d após %d", revs[i], revs[i-1])
		}
	}
	for i := 0; i < n; i++ {
		if !chaves[fmt.Sprintf("/r/%d", i)] {
			t.Errorf("chave /r/%d não foi recuperada", i)
		}
	}
	// A revisão observada precisa ter avançado, senão a próxima recuperação
	// repetiria os mesmos eventos.
	b.mu.RLock()
	depois := b.currentRev
	b.mu.RUnlock()
	if depois <= desde {
		t.Errorf("currentRev não avançou após a recuperação: %d -> %d", desde, depois)
	}
}

// TestHistoricoPerdido garante que só os códigos de invalidação disparam a
// recuperação — um erro de rede comum deve apenas reconectar pelo token.
func TestHistoricoPerdido(t *testing.T) {
	casos := []struct {
		nome     string
		err      error
		esperado bool
	}{
		{"nil", nil, false},
		{"erro comum", errors.New("connection reset"), false},
		{"ChangeStreamHistoryLost", mongo.CommandError{Code: errChangeStreamHistoryLost}, true},
		{"ChangeStreamFatalError", mongo.CommandError{Code: errChangeStreamFatalError}, true},
		{"outro código", mongo.CommandError{Code: 11000}, false},
	}
	for _, c := range casos {
		if got := historicoPerdido(c.err); got != c.esperado {
			t.Errorf("%s: historicoPerdido=%v, esperado %v", c.nome, got, c.esperado)
		}
	}
}

// TestInvalidacaoRealDoOplog fecha a lacuna que o TestRecuperacaoDeStreamInvalidado
// deixava: aqui a invalidação é REAL, não simulada.
//
// Pedir um startAtOperationTime anterior à janela do oplog faz o MongoDB
// recusar com ChangeStreamHistoryLost (286) — medido no MSPIKE-8, onde a janela
// do M0 estava em 4,4 h. O que se prova é que o erro chega classificável pelo
// historicoPerdido() e que abrirStream se recupera dele.
func TestInvalidacaoRealDoOplog(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	if _, err := b.Create(ctx, "/inv/a", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	// 30 dias atrás cai fora de qualquer janela de oplog concebível.
	antigo := bson.Timestamp{T: uint32(time.Now().Add(-30 * 24 * time.Hour).Unix()), I: 1}
	cs, err := b.col.Watch(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
	}, options.ChangeStream().SetStartAtOperationTime(&antigo))

	// O erro pode vir na abertura ou na primeira leitura.
	if err == nil {
		cs.TryNext(ctx)
		err = cs.Err()
		cs.Close(context.Background())
	}
	if err == nil {
		t.Skip("o MongoDB aceitou um startAtOperationTime de 30 dias atrás — " +
			"a janela do oplog deve ter crescido; sem invalidação para observar")
	}

	t.Logf("erro devolvido: %v", err)
	if !historicoPerdido(err) {
		t.Errorf("historicoPerdido() não reconheceu a invalidação real — "+
			"a recuperação do MW-3 não dispararia. erro: %v", err)
	}

	// E o driver precisa conseguir abrir um stream novo depois disso.
	b.ctx = ctx
	novo, err := b.abrirStream(nil)
	if err != nil {
		t.Fatalf("abrirStream após invalidação falhou: %v", err)
	}
	novo.Close(context.Background())
}

// TestCompactRepetido cobre a lacuna de o Compact nunca ter rodado por tempo
// real: o apiserver compacta a cada 5 minutos, e se cada ciclo deixar lixo o
// banco cresce sem limite — no M0, até parar o cluster.
func TestCompactRepetido(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	const chaves, geracoes = 10, 6
	revs := map[string]int64{}
	for i := 0; i < chaves; i++ {
		k := fmt.Sprintf("/c/%d", i)
		r, err := b.Create(ctx, k, []byte("g0"), 0)
		if err != nil {
			t.Fatal(err)
		}
		revs[k] = r
	}

	var docs []int64
	for g := 1; g <= geracoes; g++ {
		for i := 0; i < chaves; i++ {
			k := fmt.Sprintf("/c/%d", i)
			r, _, ok, err := b.Update(ctx, k, []byte(fmt.Sprintf("g%d", g)), revs[k], 0)
			if err != nil || !ok {
				t.Fatalf("Update g%d %s: err=%v ok=%v", g, k, err, ok)
			}
			revs[k] = r
		}
		atual, err := b.CurrentRevision(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Compact(ctx, atual); err != nil && err != server.ErrCompacted {
			t.Fatalf("Compact na geração %d: %v", g, err)
		}
		n, err := b.col.CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, n)
		t.Logf("geração %d: %d documentos", g, n)
	}

	// O número de documentos precisa estabilizar: cada ciclo apaga o que o
	// anterior tornou obsoleto. Se crescer sempre, o compact não está limpando.
	primeiro, ultimo := docs[0], docs[len(docs)-1]
	if ultimo > primeiro {
		t.Errorf("o banco cresceu ao longo dos ciclos de compactação: %d -> %d "+
			"(compact não está recuperando espaço)", primeiro, ultimo)
	}
	// E o estado corrente precisa continuar íntegro.
	for i := 0; i < chaves; i++ {
		k := fmt.Sprintf("/c/%d", i)
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		esperado := fmt.Sprintf("g%d", geracoes)
		if kv == nil || string(kv.Value) != esperado {
			t.Errorf("%s = %v, esperado %s", k, kv, esperado)
		}
	}
}

// testBackendNaColecao abre um backend adicional sobre a MESMA coleção, para
// simular dois servidores kine compartilhando um MongoDB.
func testBackendNaColecao(t *testing.T, db, col string) (*Backend, context.Context, func()) {
	t.Helper()
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI não definida")
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	uri += fmt.Sprintf("%skine_database=%s&kine_collection=%s", sep, db, col)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	_, b, err := New(ctx, wg, &drivers.Config{Endpoint: uri})
	if err != nil {
		cancel()
		t.Fatalf("New: %v", err)
	}
	be := b.(*Backend)
	if err := be.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	return be, ctx, func() { cancel(); wg.Wait() }
}

// TestDuasInstancias cobre a promessa que o driver faz ao devolver
// leaderElect=true: vários servidores kine sobre o mesmo MongoDB.
//
// A ordenação aqui não depende de coordenação entre processos — a revisão é o
// clusterTime, que é global ao cluster MongoDB, e o oplog é único. Foi o que
// permitiu remover os mutexes que a primeira correção do watch introduziu.
func TestDuasInstancias(t *testing.T) {
	db := "kine_multi"
	col := fmt.Sprintf("m%d", time.Now().UnixNano())

	a, ctxA, limparA := testBackendNaColecao(t, db, col)
	defer limparA()
	b2, ctxB, limparB := testBackendNaColecao(t, db, col)
	defer limparB()
	defer func() { _ = a.col.Drop(context.Background()); _ = a.meta.Drop(context.Background()) }()

	// As duas instâncias precisam concordar no epoch base, senão as revisões
	// de uma não fazem sentido para a outra.
	if a.cfg.EpochBase != b2.cfg.EpochBase {
		t.Fatalf("epoch base divergente: A=%d B=%d", a.cfg.EpochBase, b2.cfg.EpochBase)
	}

	// Um watch na instância A precisa ver o que a instância B escreve.
	ctxW, cancel := context.WithTimeout(ctxA, 40*time.Second)
	defer cancel()
	res := a.Watch(ctxW, "/multi/", "/multi0", 0)
	time.Sleep(3 * time.Second)

	const n = 6
	var revs []int64
	for i := 0; i < n; i++ {
		escritor, ctx := a, ctxA
		if i%2 == 1 {
			escritor, ctx = b2, ctxB
		}
		r, err := escritor.Create(ctx, fmt.Sprintf("/multi/%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		revs = append(revs, r)
	}

	// Revisões geradas por processos diferentes precisam ser globalmente
	// monotônicas e distintas.
	vistas := map[int64]bool{}
	for i, r := range revs {
		if vistas[r] {
			t.Errorf("revisão %d repetida entre instâncias", r)
		}
		vistas[r] = true
		if i > 0 && r <= revs[i-1] {
			t.Errorf("revisões não monotônicas entre instâncias: %d após %d", r, revs[i-1])
		}
	}

	recebidos, ultima := 0, int64(0)
	prazo := time.After(35 * time.Second)
	for recebidos < n {
		select {
		case lote, ok := <-res.Events:
			if !ok {
				t.Fatalf("canal fechou com %d de %d", recebidos, n)
			}
			for _, e := range lote {
				if e.KV.ModRevision <= ultima {
					t.Errorf("fora de ordem: %d após %d", e.KV.ModRevision, ultima)
				}
				ultima = e.KV.ModRevision
				recebidos++
			}
		case err := <-res.Errorc:
			t.Fatalf("watch: %v", err)
		case <-prazo:
			t.Fatalf("timeout: %d de %d eventos (a instância A não viu as escritas da B)", recebidos, n)
		}
	}

	// Compactação concorrente: só uma pode avançar a revisão, sem corromper.
	atual, err := a.CurrentRevision(ctxA)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	erros := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); _, erros[0] = a.Compact(ctxA, atual) }()
	go func() { defer wg.Done(); _, erros[1] = b2.Compact(ctxB, atual) }()
	wg.Wait()

	sucessos := 0
	for _, e := range erros {
		if e == nil {
			sucessos++
		} else if e != server.ErrCompacted {
			t.Errorf("Compact concorrente devolveu erro inesperado: %v", e)
		}
	}
	if sucessos != 1 {
		t.Errorf("%d instâncias compactaram com sucesso, esperado exatamente 1", sucessos)
	}

	// O estado corrente precisa sobreviver, visto pelas duas.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("/multi/%d", i)
		for nome, inst := range map[string]*Backend{"A": a, "B": b2} {
			ctx := ctxA
			if nome == "B" {
				ctx = ctxB
			}
			_, kv, err := inst.Get(ctx, k, 0, false)
			if err != nil {
				t.Fatalf("Get %s em %s: %v", k, nome, err)
			}
			if kv == nil {
				t.Errorf("instância %s não vê %s após a compactação", nome, k)
			}
		}
	}
}
