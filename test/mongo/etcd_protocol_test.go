//go:build integration

package mongo_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/endpoint"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func etcdClient(t *testing.T) (*clientv3.Client, context.Context) {
	t.Helper()
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI is not set")
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	uri += fmt.Sprintf("%skine_database=kine_proto&kine_collection=p%d", sep, time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	cfg := endpoint.Config{
		Endpoint:       uri,
		Listener:       fmt.Sprintf("unix://%s/kine-proto.sock", t.TempDir()),
		WaitGroup:      wg,
		NotifyInterval: 5 * time.Second,
	}
	e, err := endpoint.Listen(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("subir kine: %v", err)
	}
	time.Sleep(2 * time.Second) // sem canal de ready

	tls, err := e.TLSConfig.ClientConfig()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   e.Endpoints,
		TLS:         tls,
		DialTimeout: 15 * time.Second,
	})
	if err != nil {
		cancel()
		t.Fatalf("cliente etcd: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
		cancel()
		wg.Wait()
	})
	return cli, ctx
}

// TestTxnCreate — see docs/implementation-notes.md
func TestTxnCreate(t *testing.T) {
	cli, ctx := etcdClient(t)
	chave := "/registry/pods/default/nginx"

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", 0)).
		Then(clientv3.OpPut(chave, "objeto-v1")).
		Commit()
	if err != nil {
		t.Fatalf("create Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("create Txn did not succeed on a missing key")
	}

	resp2, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", 0)).
		Then(clientv3.OpPut(chave, "objeto-duplicado")).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("duplicate create Txn: %v", err)
	}
	if resp2.Succeeded {
		t.Error("create Txn succeeded on an existing key")
	}
	if len(resp2.Responses) == 0 {
		t.Fatal("o ramo Else returned no o value atual")
	}
	rr := resp2.Responses[0].GetResponseRange()
	if len(rr.Kvs) != 1 || string(rr.Kvs[0].Value) != "objeto-v1" {
		t.Errorf("Else returned %v, want objeto-v1", rr.Kvs)
	}
}

// TestTxnUpdate — see docs/implementation-notes.md
func TestTxnUpdate(t *testing.T) {
	cli, ctx := etcdClient(t)
	chave := "/registry/configmaps/default/cm"

	if _, err := cli.Put(ctx, chave, "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	g, err := cli.Get(ctx, chave)
	if err != nil || len(g.Kvs) != 1 {
		t.Fatalf("Get: %v kvs=%d", err, len(g.Kvs))
	}
	rev := g.Kvs[0].ModRevision

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev)).
		Then(clientv3.OpPut(chave, "v2")).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn de update: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("update Txn failed with the correct revision - optimistic concurrency is broken")
	}

	resp2, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev)).
		Then(clientv3.OpPut(chave, "v3")).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn com revision stale: %v", err)
	}
	if resp2.Succeeded {
		t.Error("Txn sucedeu com revision stale — o apiserver perderia writes")
	}
}

// TestRevisionMonotonicOverProtocol — see docs/implementation-notes.md
func TestRevisionMonotonicOverProtocol(t *testing.T) {
	cli, ctx := etcdClient(t)

	var revs []int64
	for i := 0; i < 10; i++ {
		r, err := cli.Put(ctx, fmt.Sprintf("/registry/x/%d", i), "v")
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		revs = append(revs, r.Header.Revision)
	}
	for i := 1; i < len(revs); i++ {
		if revs[i] <= revs[i-1] {
			t.Fatalf("revision did not advance: %d to de %d", revs[i], revs[i-1])
		}
	}
	jumps := 0
	for i := 1; i < len(revs); i++ {
		if revs[i] != revs[i-1]+1 {
			jumps++
		}
	}
	t.Logf("revisions: %v", revs)
	t.Logf("%d de %d incrementos are not densos (want com clusterTime)", jumps, len(revs)-1)
}

// TestWatchOverProtocol — see docs/implementation-notes.md
func TestWatchOverProtocol(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/pods/"

	r, err := cli.Put(ctx, pref+"a", "1")
	if err != nil {
		t.Fatal(err)
	}
	base := r.Header.Revision

	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ch := cli.Watch(wctx, pref, clientv3.WithPrefix(), clientv3.WithRev(base+1))

	time.Sleep(2 * time.Second)
	for i := 0; i < 3; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sb%d", pref, i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	seen := 0
	var lastRev int64
	for seen < 3 {
		select {
		case wr, ok := <-ch:
			if !ok {
				t.Fatalf("canal de watch fechou after %d events", seen)
			}
			if wr.Err() != nil {
				t.Fatalf("watch: %v", wr.Err())
			}
			for _, ev := range wr.Events {
				if ev.Kv.ModRevision <= lastRev {
					t.Errorf("event out of order: %d after %d", ev.Kv.ModRevision, lastRev)
				}
				lastRev = ev.Kv.ModRevision
				seen++
			}
		case <-time.After(25 * time.Second):
			t.Fatalf("timeout: %d de 3 events", seen)
		}
	}
}

// TestListByPrefix — see docs/implementation-notes.md
func TestListByPrefix(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/services/"

	for i := 0; i < 8; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%ssvc%02d", pref, i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cli.Put(ctx, "/registry/outracoisa/z", "v"); err != nil {
		t.Fatal(err)
	}

	resp, err := cli.Get(ctx, pref, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Get com prefixo: %v", err)
	}
	if len(resp.Kvs) != 8 {
		t.Fatalf("LIST returned %d keys, want 8", len(resp.Kvs))
	}
	for i := 1; i < len(resp.Kvs); i++ {
		if string(resp.Kvs[i-1].Key) >= string(resp.Kvs[i].Key) {
			t.Errorf("LIST out of order: %s before de %s", resp.Kvs[i-1].Key, resp.Kvs[i].Key)
		}
	}
	if resp.Header.Revision == 0 {
		t.Error("LIST returned no revision in the header - the watch cache depends on it")
	}

	cresp, err := cli.Get(ctx, pref, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if cresp.Count != 8 {
		t.Errorf("count=%d, want 8", cresp.Count)
	}
}
