//go:build integration

package mongo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"go.mongodb.org/mongo-driver/v2/bson"
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
