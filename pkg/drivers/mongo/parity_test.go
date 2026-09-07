//go:build integration

package mongo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func TestParityCreateAfterDelete(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()
	k := "/test/a"

	rev, err := b.Create(ctx, k, []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := b.Delete(ctx, k, rev); err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}
	rev2, err := b.Create(ctx, k, []byte("v2"), 0)
	if err != nil {
		t.Fatalf("Create after Delete: %v", err)
	}
	if rev2 <= rev {
		t.Errorf("revision did not advance: %d -> %d", rev, rev2)
	}
	_, kv, err := b.Get(ctx, k, 0, false)
	if err != nil || kv == nil {
		t.Fatalf("Get: err=%v kv=%v", err, kv)
	}
	if string(kv.Value) != "v2" {
		t.Errorf("value = %q, want v2", kv.Value)
	}
	if kv.Version != 1 {
		t.Errorf("Version = %d after recriar, want 1", kv.Version)
	}
}

func TestParityVersionIncrement(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()
	k := "/test/a"

	rev, err := b.Create(ctx, k, []byte("v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for want, payload := range map[int64]string{1: "v1"} {
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if kv.Version != want {
			t.Errorf("Version = %d after criar, want %d", kv.Version, want)
		}
		_ = payload
	}
	for i, want := range []int64{2, 3} {
		var err error
		rev, _, _, err = b.Update(ctx, k, []byte(fmt.Sprintf("v%d", i+2)), rev, 0)
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if kv.Version != want {
			t.Errorf("Version = %d after %d updates, want %d", kv.Version, i+1, want)
		}
	}
}

func TestParityWatchPrevKV(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()
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
			t.Fatalf("%d events, want 1", len(eventos))
		}
		e := eventos[0]
		if string(e.KV.Value) != "v2" {
			t.Errorf("KV.Value = %q, want v2", e.KV.Value)
		}
		if e.PrevKV == nil {
			t.Fatal("PrevKV missing - the watch did not expose the previous value")
		}
		if string(e.PrevKV.Value) != "v1" {
			t.Errorf("PrevKV.Value = %q, want v1", e.PrevKV.Value)
		}
		if e.PrevKV.ModRevision != rev {
			t.Errorf("PrevKV.ModRevision = %d, want %d", e.PrevKV.ModRevision, rev)
		}
	case err := <-wr.Errorc:
		t.Fatalf("watch: %v", err)
	case <-time.After(35 * time.Second):
		t.Fatal("timeout waiting for o event")
	}
}

func TestParityListExcludesDeleted(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()
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
		t.Fatalf("List returned %d, want 2 (a deleted must not aparecer)", len(kvs))
	}
	for _, kv := range kvs {
		if kv.Key == "/test/k1" {
			t.Error("a key deleted apareceu no List")
		}
	}
	_, n, err := b.Count(ctx, pref, fim, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}
}

func TestParityGetAtRevision(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()
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
		rev  int64
		want string
	}{{r1, "v1"}, {r2, "v2"}, {0, "v3"}} {
		_, kv, err := b.Get(ctx, k, c.rev, false)
		if err != nil {
			t.Fatalf("Get na revision %d: %v", c.rev, err)
		}
		if kv == nil || string(kv.Value) != c.want {
			t.Errorf("Get(rev=%d) = %v, want %q", c.rev, kv, c.want)
		}
	}
}

func TestParityCompactDropsTombstones(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	r, err := b.Create(ctx, "/test/dead", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := b.Delete(ctx, "/test/dead", r); err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}
	if _, err := b.Create(ctx, "/test/alive", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	atual, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Compact(ctx, atual); err != nil && err != server.ErrCompacted {
		t.Fatalf("Compact: %v", err)
	}

	_, kvs, err := b.List(ctx, "/test/", "/test0", 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || kvs[0].Key != "/test/alive" {
		t.Errorf("after compaction, List = %v, want only /test/alive", kvs)
	}
	_, kv, err := b.Get(ctx, "/test/dead", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if kv != nil {
		t.Errorf("the deleted key reappeared after compaction: %v", kv)
	}
}

func TestParityWatchCompacted(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

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

	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	wr := b.Watch(wctx, "/test/", "/test0", 1)
	select {
	case err := <-wr.Errorc:
		if err != server.ErrCompacted {
			t.Errorf("watch em revision compacted returned %v, want ErrCompacted", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("watch em revision compacted returned no erro algum")
	}
}

func TestParityCurrentRevisionAndDbSize(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

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
		t.Errorf("CurrentRevision did not follow the write: %d -> %d (write at %d)", r0, r2, r1)
	}

	tam, err := b.DbSize(ctx)
	if err != nil {
		t.Fatalf("DbSize: %v", err)
	}
	if tam <= 0 {
		t.Errorf("DbSize = %d, want > 0", tam)
	}
}

func TestParityKeysOnly(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	if _, err := b.Create(ctx, "/test/a", []byte("content"), 0); err != nil {
		t.Fatal(err)
	}
	_, kv, err := b.Get(ctx, "/test/a", 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil {
		t.Fatal("Get keysOnly returned no a key")
	}
	if len(kv.Value) != 0 {
		t.Errorf("Get keysOnly returned value: %q", kv.Value)
	}
	if kv.Key != "/test/a" {
		t.Errorf("Key = %q", kv.Key)
	}
}
