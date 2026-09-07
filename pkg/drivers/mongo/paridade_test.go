//go:build integration

package mongo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

// MT-1 — paridade com o backend de referência.
//
// O baseline não é o driver SQLite (cujos testes cobrem só VACUUM), é o
// pkg/drivers/memory: 22 casos, e é o outro backend que implementa
// server.Backend diretamente. Estes testes portam os casos que a suíte do
// mongo ainda não cobria.
//
// Uma diferença estrutural exige adaptação: no memory as revisões são densas
// (1, 2, 3), aqui vêm do clusterTime e saltam (ADR-0001). Onde o teste de
// referência compara com um literal, aqui se compara com a revisão devolvida.

func TestParidadeCreateAfterDelete(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()
	k := "/test/a"

	rev, err := b.Create(ctx, k, []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := b.Delete(ctx, k, rev); err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}
	// Recriar depois de apagar precisa funcionar, e a contagem de versões
	// reinicia — é a semântica do etcd.
	rev2, err := b.Create(ctx, k, []byte("v2"), 0)
	if err != nil {
		t.Fatalf("Create após Delete: %v", err)
	}
	if rev2 <= rev {
		t.Errorf("revisão não avançou: %d -> %d", rev, rev2)
	}
	_, kv, err := b.Get(ctx, k, 0, false)
	if err != nil || kv == nil {
		t.Fatalf("Get: err=%v kv=%v", err, kv)
	}
	if string(kv.Value) != "v2" {
		t.Errorf("valor = %q, esperado v2", kv.Value)
	}
	if kv.Version != 1 {
		t.Errorf("Version = %d após recriar, esperado 1", kv.Version)
	}
}

func TestParidadeVersionIncrement(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()
	k := "/test/a"

	rev, err := b.Create(ctx, k, []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for esperado, valor := range map[int64]string{1: "v1"} {
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if kv.Version != esperado {
			t.Errorf("Version = %d após criar, esperado %d", kv.Version, esperado)
		}
		_ = valor
	}
	for i, esperado := range []int64{2, 3} {
		var err error
		rev, _, _, err = b.Update(ctx, k, []byte(fmt.Sprintf("v%d", i+2)), rev, 0)
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if kv.Version != esperado {
			t.Errorf("Version = %d após %d updates, esperado %d", kv.Version, i+1, esperado)
		}
	}
}

func TestParidadeWatchPrevKV(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()
	k := "/test/a"

	rev, err := b.Create(ctx, k, []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}

	wctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	wr := b.Watch(wctx, "/test/", "/test0", 0)
	time.Sleep(3 * time.Second)

	if _, _, ok, err := b.Update(ctx, k, []byte("v2"), rev, 0); err != nil || !ok {
		t.Fatalf("Update: err=%v ok=%v", err, ok)
	}

	select {
	case eventos := <-wr.Events:
		if len(eventos) != 1 {
			t.Fatalf("%d eventos, esperado 1", len(eventos))
		}
		e := eventos[0]
		if string(e.KV.Value) != "v2" {
			t.Errorf("KV.Value = %q, esperado v2", e.KV.Value)
		}
		if e.PrevKV == nil {
			t.Fatal("PrevKV ausente — o watch não expôs o valor anterior")
		}
		if string(e.PrevKV.Value) != "v1" {
			t.Errorf("PrevKV.Value = %q, esperado v1", e.PrevKV.Value)
		}
		// revisões não são densas aqui: compara com a revisão da criação
		if e.PrevKV.ModRevision != rev {
			t.Errorf("PrevKV.ModRevision = %d, esperado %d", e.PrevKV.ModRevision, rev)
		}
	case err := <-wr.Errorc:
		t.Fatalf("watch: %v", err)
	case <-time.After(35 * time.Second):
		t.Fatal("timeout esperando o evento")
	}
}

func TestParidadeListExcludesDeleted(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()
	pref, fim := "/test/", "/test0"

	var revs []int64
	for i := 0; i < 3; i++ {
		r, err := b.Create(ctx, fmt.Sprintf("/test/k%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatal(err)
		}
		revs = append(revs, r)
	}
	if _, _, ok, err := b.Delete(ctx, "/test/k1", revs[1]); err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}

	_, kvs, err := b.List(ctx, pref, fim, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 {
		t.Fatalf("List devolveu %d, esperado 2 (a apagada não deve aparecer)", len(kvs))
	}
	for _, kv := range kvs {
		if kv.Key == "/test/k1" {
			t.Error("a chave apagada apareceu no List")
		}
	}
	_, n, err := b.Count(ctx, pref, fim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("Count = %d, esperado 2", n)
	}
}

func TestParidadeGetAtRevision(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()
	k := "/test/a"

	r1, err := b.Create(ctx, k, []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	r2, _, _, err := b.Update(ctx, k, []byte("v2"), r1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.Update(ctx, k, []byte("v3"), r2, 0); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		rev      int64
		esperado string
	}{{r1, "v1"}, {r2, "v2"}, {0, "v3"}} {
		_, kv, err := b.Get(ctx, k, c.rev, false)
		if err != nil {
			t.Fatalf("Get na revisão %d: %v", c.rev, err)
		}
		if kv == nil || string(kv.Value) != c.esperado {
			t.Errorf("Get(rev=%d) = %v, esperado %q", c.rev, kv, c.esperado)
		}
	}
}

func TestParidadeCompactDropsTombstones(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	r, err := b.Create(ctx, "/test/morto", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := b.Delete(ctx, "/test/morto", r); err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}
	if _, err := b.Create(ctx, "/test/vivo", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	atual, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Compact(ctx, atual); err != nil && err != server.ErrCompacted {
		t.Fatalf("Compact: %v", err)
	}

	// A tombstone precisa ter sumido, e a chave viva precisa continuar.
	_, kvs, err := b.List(ctx, "/test/", "/test0", 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || kvs[0].Key != "/test/vivo" {
		t.Errorf("após compactar, List = %v, esperado só /test/vivo", kvs)
	}
	_, kv, err := b.Get(ctx, "/test/morto", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if kv != nil {
		t.Errorf("a chave apagada reapareceu após a compactação: %v", kv)
	}
}

func TestParidadeWatchCompacted(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	r, err := b.Create(ctx, "/test/a", []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		r, _, _, err = b.Update(ctx, "/test/a", []byte(fmt.Sprintf("v%d", i+2)), r, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	atual, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Compact(ctx, atual); err != nil && err != server.ErrCompacted {
		t.Fatalf("Compact: %v", err)
	}

	// Um watch a partir de revisão já compactada precisa devolver ErrCompacted,
	// não silêncio — é como o apiserver sabe que precisa relistar.
	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	wr := b.Watch(wctx, "/test/", "/test0", 1)
	select {
	case err := <-wr.Errorc:
		if err != server.ErrCompacted {
			t.Errorf("watch em revisão compactada devolveu %v, esperado ErrCompacted", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("watch em revisão compactada não devolveu erro algum")
	}
}

func TestParidadeCurrentRevisionEDbSize(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	r0, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := b.Create(ctx, "/test/a", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r2 < r1 || r2 <= r0 {
		t.Errorf("CurrentRevision não acompanhou a escrita: %d -> %d (escrita em %d)", r0, r2, r1)
	}

	tam, err := b.DbSize(ctx)
	if err != nil {
		t.Fatalf("DbSize: %v", err)
	}
	if tam <= 0 {
		t.Errorf("DbSize = %d, esperado > 0", tam)
	}
}

func TestParidadeKeysOnly(t *testing.T) {
	b, ctx, limpar := testBackend(t)
	defer limpar()

	if _, err := b.Create(ctx, "/test/a", []byte("conteudo"), 0); err != nil {
		t.Fatal(err)
	}
	_, kv, err := b.Get(ctx, "/test/a", 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil {
		t.Fatal("Get keysOnly não devolveu a chave")
	}
	if len(kv.Value) != 0 {
		t.Errorf("Get keysOnly devolveu valor: %q", kv.Value)
	}
	if kv.Key != "/test/a" {
		t.Errorf("Key = %q", kv.Key)
	}
}
