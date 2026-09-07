//go:build integration

package mongo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestConfLeaseTTL — see docs/implementation-notes.md
func TestConfLeaseTTL(t *testing.T) {
	cli, ctx := etcdClient(t)

	lease, err := cli.Grant(ctx, 2)
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if lease.ID == 0 {
		t.Fatal("Grant returned lease ID zero")
	}
	chave := "/registry/leases/efemera"
	if _, err := cli.Put(ctx, chave, "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("Put com lease: %v", err)
	}

	g, err := cli.Get(ctx, chave)
	if err != nil || len(g.Kvs) != 1 {
		t.Fatalf("a key com lease does not exist: err=%v kvs=%d", err, len(g.Kvs))
	}
	if g.Kvs[0].Lease != int64(lease.ID) {
		t.Errorf("Lease na key = %d, want %d", g.Kvs[0].Lease, lease.ID)
	}

	deadline := time.After(60 * time.Second)
	for {
		g, err := cli.Get(ctx, chave)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(g.Kvs) == 0 {
			return // expirou
		}
		select {
		case <-deadline:
			t.Fatal("a key com lease de 2s did not expire em 60s")
		case <-time.After(3 * time.Second):
		}
	}
}

// TestConfRangeOptions — see docs/implementation-notes.md
func TestConfRangeOptions(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/pods/ns/"

	for i := 0; i < 8; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sp%02d", pref, i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	casos := []struct {
		name   string
		opts   []clientv3.OpOption
		valida func(*testing.T, *clientv3.GetResponse)
	}{
		{"prefixo", []clientv3.OpOption{clientv3.WithPrefix()},
			func(t *testing.T, r *clientv3.GetResponse) {
				if len(r.Kvs) != 8 {
					t.Errorf("%d keys, want 8", len(r.Kvs))
				}
			}},
		{"limite", []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithLimit(3)},
			func(t *testing.T, r *clientv3.GetResponse) {
				if len(r.Kvs) != 3 {
					t.Errorf("%d keys, want 3", len(r.Kvs))
				}
			}},
		{"contagem", []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithCountOnly()},
			func(t *testing.T, r *clientv3.GetResponse) {
				if r.Count != 8 {
					t.Errorf("Count = %d, want 8", r.Count)
				}
				if len(r.Kvs) != 0 {
					t.Errorf("CountOnly returned %d keys", len(r.Kvs))
				}
			}},
		{"keys-only", []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly()},
			func(t *testing.T, r *clientv3.GetResponse) {
				for _, kv := range r.Kvs {
					if len(kv.Value) != 0 {
						t.Errorf("KeysOnly returned value em %s", kv.Key)
						return
					}
				}
			}},
	}
	for _, c := range casos {
		r, err := cli.Get(ctx, pref, c.opts...)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		c.valida(t, r)
		if r.Header.Revision == 0 {
			t.Errorf("%s: header sem revision — o watch cache depende dela", c.name)
		}
	}
}

// TestConfTxnDelete — see docs/implementation-notes.md
func TestConfTxnDelete(t *testing.T) {
	cli, ctx := etcdClient(t)
	chave := "/registry/pods/ns/apagavel"

	p, err := cli.Put(ctx, chave, "v1")
	if err != nil {
		t.Fatal(err)
	}
	rev := p.Header.Revision

	r, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev-1)).
		Then(clientv3.OpDelete(chave)).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if r.Succeeded {
		t.Error("deleted com revision stale — o apiserver perderia writes")
	}
	if g, err := cli.Get(ctx, chave); err != nil || len(g.Kvs) != 1 {
		t.Fatalf("a key vanished: err=%v kvs=%d", err, len(g.Kvs))
	}

	r, err = cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev)).
		Then(clientv3.OpDelete(chave)).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !r.Succeeded {
		t.Fatal("did not delete com a revision correct")
	}
	if g, err := cli.Get(ctx, chave); err != nil || len(g.Kvs) != 0 {
		t.Errorf("a key survived after o delete: err=%v kvs=%d", err, len(g.Kvs))
	}
}

// TestConfFilteredWatch — see docs/implementation-notes.md
func TestConfFilteredWatch(t *testing.T) {
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
			t.Fatal("nenhum event")
		}
		ev := wr.Events[0]
		if string(ev.Kv.Value) != "v2" {
			t.Errorf("Kv.Value = %q, want v2", ev.Kv.Value)
		}
		if ev.PrevKv == nil {
			t.Error("WithPrevKV did not return the previous value")
		} else if string(ev.PrevKv.Value) != "v1" {
			t.Errorf("PrevKv.Value = %q, want v1", ev.PrevKv.Value)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("timeout waiting for o event")
	}
}

// TestConfCompactOverProtocol — see docs/implementation-notes.md
func TestConfCompactOverProtocol(t *testing.T) {
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
	target := revs[len(revs)-1]

	if _, err := cli.Compact(ctx, target); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	g, err := cli.Get(ctx, chave)
	if err != nil || len(g.Kvs) != 1 {
		t.Fatalf("Get after compaction: err=%v kvs=%d", err, len(g.Kvs))
	}
	if string(g.Kvs[0].Value) != "v3" {
		t.Errorf("value current = %q, want v3", g.Kvs[0].Value)
	}

	if _, err := cli.Get(ctx, chave, clientv3.WithRev(revs[0])); err == nil {
		t.Error("Get em revision compacted returned no erro")
	} else {
		t.Logf("erro want ao ler revision compacted: %v", err)
	}
}
