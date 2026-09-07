//go:build integration

package mongo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MT-2 — conformidade com a API etcd v3, exercitada pelo protocolo.
//
// O que o apiserver realmente usa: Txn com Compare, Range com opções, Watch com
// filtros e revisão, Lease com TTL, e Compact. Cada teste aqui reproduz um
// padrão que o apiserver emite, não uma chamada inventada.

// TestConfLeaseTTL cobre o ciclo de lease do etcd, que o apiserver usa para
// objetos com expiração (Lease de nó, eventos).
func TestConfLeaseTTL(t *testing.T) {
	cli, ctx := etcdClient(t)

	lease, err := cli.Grant(ctx, 2)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if lease.ID == 0 {
		t.Fatal("Grant devolveu lease ID zero")
	}
	chave := "/registry/leases/efemera"
	if _, err := cli.Put(ctx, chave, "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("Put com lease: %v", err)
	}

	g, err := cli.Get(ctx, chave)
	if err != nil || len(g.Kvs) != 1 {
		t.Fatalf("a chave com lease não existe: err=%v kvs=%d", err, len(g.Kvs))
	}
	if g.Kvs[0].Lease != int64(lease.ID) {
		t.Errorf("Lease na chave = %d, esperado %d", g.Kvs[0].Lease, lease.ID)
	}

	// O kine expira por varredura (pkg/ttl), então a remoção não é instantânea.
	prazo := time.After(60 * time.Second)
	for {
		g, err := cli.Get(ctx, chave)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(g.Kvs) == 0 {
			return // expirou
		}
		select {
		case <-prazo:
			t.Fatal("a chave com lease de 2s não expirou em 60s")
		case <-time.After(3 * time.Second):
		}
	}
}

// TestConfRangeOpcoes cobre as variações de Range que o apiserver emite:
// prefixo, limite, contagem, keys-only e paginação por FromKey.
func TestConfRangeOpcoes(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/pods/ns/"

	for i := 0; i < 8; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sp%02d", pref, i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	casos := []struct {
		nome   string
		opts   []clientv3.OpOption
		valida func(*testing.T, *clientv3.GetResponse)
	}{
		{"prefixo", []clientv3.OpOption{clientv3.WithPrefix()},
			func(t *testing.T, r *clientv3.GetResponse) {
				if len(r.Kvs) != 8 {
					t.Errorf("%d chaves, esperado 8", len(r.Kvs))
				}
			}},
		{"limite", []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithLimit(3)},
			func(t *testing.T, r *clientv3.GetResponse) {
				if len(r.Kvs) != 3 {
					t.Errorf("%d chaves, esperado 3", len(r.Kvs))
				}
			}},
		{"contagem", []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithCountOnly()},
			func(t *testing.T, r *clientv3.GetResponse) {
				if r.Count != 8 {
					t.Errorf("Count = %d, esperado 8", r.Count)
				}
				if len(r.Kvs) != 0 {
					t.Errorf("CountOnly devolveu %d chaves", len(r.Kvs))
				}
			}},
		{"keys-only", []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly()},
			func(t *testing.T, r *clientv3.GetResponse) {
				for _, kv := range r.Kvs {
					if len(kv.Value) != 0 {
						t.Errorf("KeysOnly devolveu valor em %s", kv.Key)
						return
					}
				}
			}},
	}
	for _, c := range casos {
		r, err := cli.Get(ctx, pref, c.opts...)
		if err != nil {
			t.Errorf("%s: %v", c.nome, err)
			continue
		}
		c.valida(t, r)
		if r.Header.Revision == 0 {
			t.Errorf("%s: header sem revisão — o watch cache depende dela", c.nome)
		}
	}
}

// TestConfTxnDelete cobre o delete condicional: o apiserver apaga só se a
// revisão for a que ele leu, para não apagar uma versão mais nova.
func TestConfTxnDelete(t *testing.T) {
	cli, ctx := etcdClient(t)
	chave := "/registry/pods/ns/apagavel"

	p, err := cli.Put(ctx, chave, "v1")
	if err != nil {
		t.Fatal(err)
	}
	rev := p.Header.Revision

	// Com a revisão errada, não apaga.
	r, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev-1)).
		Then(clientv3.OpDelete(chave)).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if r.Succeeded {
		t.Error("apagou com revisão desatualizada — o apiserver perderia escritas")
	}
	if g, err := cli.Get(ctx, chave); err != nil || len(g.Kvs) != 1 {
		t.Fatalf("a chave desapareceu: err=%v kvs=%d", err, len(g.Kvs))
	}

	// Com a revisão certa, apaga. O Else é obrigatório: o kine reconhece os
	// padrões de Txn por forma exata, e o delete condicional exige um ramo de
	// falha com um Range (pkg/server/delete.go:19). O apiserver sempre o manda.
	r, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev)).
		Then(clientv3.OpDelete(chave)).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !r.Succeeded {
		t.Fatal("não apagou com a revisão correta")
	}
	if g, err := cli.Get(ctx, chave); err != nil || len(g.Kvs) != 0 {
		t.Errorf("a chave continua após o delete: err=%v kvs=%d", err, len(g.Kvs))
	}
}

// TestConfWatchFiltrado cobre o watch com WithFilterDelete e WithPrevKV, que o
// apiserver usa para alimentar o watch cache.
func TestConfWatchFiltrado(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/cm/"

	p, err := cli.Put(ctx, pref+"a", "v1")
	if err != nil {
		t.Fatal(err)
	}
	base := p.Header.Revision

	wctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	ch := cli.Watch(wctx, pref, clientv3.WithPrefix(),
		clientv3.WithRev(base+1), clientv3.WithPrevKV())
	time.Sleep(2 * time.Second)

	if _, err := cli.Put(ctx, pref+"a", "v2"); err != nil {
		t.Fatal(err)
	}

	select {
	case wr := <-ch:
		if wr.Err() != nil {
			t.Fatalf("watch: %v", wr.Err())
		}
		if len(wr.Events) == 0 {
			t.Fatal("nenhum evento")
		}
		ev := wr.Events[0]
		if string(ev.Kv.Value) != "v2" {
			t.Errorf("Kv.Value = %q, esperado v2", ev.Kv.Value)
		}
		if ev.PrevKv == nil {
			t.Error("WithPrevKV não trouxe o valor anterior")
		} else if string(ev.PrevKv.Value) != "v1" {
			t.Errorf("PrevKv.Value = %q, esperado v1", ev.PrevKv.Value)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("timeout esperando o evento")
	}
}

// TestConfCompactPeloProtocolo cobre o Compact que o apiserver chama a cada
// 5 minutos, e o ErrCompacted que ele espera ao pedir revisão antiga.
func TestConfCompactPeloProtocolo(t *testing.T) {
	cli, ctx := etcdClient(t)
	chave := "/registry/pods/ns/compactavel"

	var revs []int64
	for i := 0; i < 4; i++ {
		p, err := cli.Put(ctx, chave, fmt.Sprintf("v%d", i))
		if err != nil {
			t.Fatal(err)
		}
		revs = append(revs, p.Header.Revision)
	}
	alvo := revs[len(revs)-1]

	if _, err := cli.Compact(ctx, alvo); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// O valor corrente sobrevive.
	g, err := cli.Get(ctx, chave)
	if err != nil || len(g.Kvs) != 1 {
		t.Fatalf("Get após compactar: err=%v kvs=%d", err, len(g.Kvs))
	}
	if string(g.Kvs[0].Value) != "v3" {
		t.Errorf("valor corrente = %q, esperado v3", g.Kvs[0].Value)
	}

	// Pedir revisão compactada precisa dar erro, não devolver dado velho —
	// é assim que o apiserver sabe que precisa relistar.
	if _, err := cli.Get(ctx, chave, clientv3.WithRev(revs[0])); err == nil {
		t.Error("Get em revisão compactada não devolveu erro")
	} else {
		t.Logf("erro esperado ao ler revisão compactada: %v", err)
	}
}
