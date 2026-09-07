//go:build integration

package mongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func testBackend(t *testing.T) (*Backend, context.Context, func()) {
	t.Helper()
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI is not set")
	}
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

func TestCreateReadUpdateDelete(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	chave := "/registry/pods/default/nginx"
	payload := []byte("first")

	rev, err := b.Create(ctx, chave, payload, 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rev == 0 {
		t.Fatal("Create returned revision zero")
	}

	_, kv, err := b.Get(ctx, chave, 0, false)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv == nil || string(kv.Value) != "first" {
		t.Fatalf("Get returned %v, want 'first'", kv)
	}
	if kv.ModRevision != rev {
		t.Errorf("ModRevision=%d, want %d", kv.ModRevision, rev)
	}

	if _, err := b.Create(ctx, chave, []byte("x"), 0); err != server.ErrKeyExists {
		t.Errorf("Create duplicado returned %v, want ErrKeyExists", err)
	}

	rev2, kv2, ok, err := b.Update(ctx, chave, []byte("segundo"), rev, 0)
	if err != nil || !ok {
		t.Fatalf("Update: err=%v ok=%v", err, ok)
	}
	if rev2 <= rev {
		t.Errorf("revision did not advance: %d -> %d", rev, rev2)
	}
	if string(kv2.Value) != "segundo" {
		t.Errorf("Update returned %q", kv2.Value)
	}

	_, antigo, err := b.Get(ctx, chave, rev, false)
	if err != nil {
		t.Fatalf("historical Get: %v", err)
	}
	if antigo == nil || string(antigo.Value) != "first" {
		t.Errorf("Get na revision %d returned %v, want 'first'", rev, antigo)
	}

	_, _, ok, err = b.Delete(ctx, chave, rev2)
	if err != nil || !ok {
		t.Fatalf("Delete: err=%v ok=%v", err, ok)
	}
	_, kv3, err := b.Get(ctx, chave, 0, false)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if kv3 != nil {
		t.Errorf("key still visible after delete: %v", kv3)
	}
}

func TestListAndCount(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("/registry/pods/default/p%02d", i)
		if _, err := b.Create(ctx, k, []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Create %s: %v", k, err)
		}
	}
	if _, err := b.Create(ctx, "/registry/services/x", []byte("fora"), 0); err != nil {
		t.Fatal(err)
	}

	pref, fim := "/registry/pods/default/", "/registry/pods/default0"

	_, kvs, err := b.List(ctx, pref, fim, 0, 0, false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(kvs) != 10 {
		t.Fatalf("List returned %d keys, want 10", len(kvs))
	}
	for i := 1; i < len(kvs); i++ {
		if kvs[i-1].Key >= kvs[i].Key {
			t.Errorf("List out of order: %q before de %q", kvs[i-1].Key, kvs[i].Key)
		}
	}

	_, n, err := b.Count(ctx, pref, fim, 0)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 10 {
		t.Errorf("Count=%d, want 10", n)
	}

	_, lim, err := b.List(ctx, pref, fim, 3, 0, false)
	if err != nil {
		t.Fatalf("List com limite: %v", err)
	}
	if len(lim) != 3 {
		t.Errorf("List com limite returned %d, want 3", len(lim))
	}

	_, so, err := b.List(ctx, pref, fim, 0, 0, true)
	if err != nil {
		t.Fatalf("List keysOnly: %v", err)
	}
	for _, kv := range so {
		if len(kv.Value) != 0 {
			t.Errorf("keysOnly returned value em %q", kv.Key)
			break
		}
	}
}

func TestRevisionsAreMonotonic(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

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
			t.Errorf("revisions are not monotonic: %d after %d", revs[i], revs[i-1])
		}
	}
}

func TestConcurrentRevisions(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

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
			t.Errorf("Create concurrent %d: %v", i, err)
			continue
		}
		if vistas[revs[i]] {
			t.Errorf("revision duplicate: %d", revs[i])
		}
		vistas[revs[i]] = true
	}
	if len(vistas) != n {
		t.Errorf("%d revisions distintas, want %d", len(vistas), n)
	}
}

func TestWatch(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	ctxW, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res := b.Watch(ctxW, "/w/", "/w0", 0)
	if res.Events == nil {
		t.Fatal("Watch returned no canal de events")
	}
	time.Sleep(2 * time.Second) // deixa o change stream assentar

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := b.Create(ctx, fmt.Sprintf("/w/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	received := 0
	var lastRev int64
	deadline := time.After(25 * time.Second)
	for received < n {
		select {
		case lote, ok := <-res.Events:
			if !ok {
				t.Fatalf("canal fechado after %d events", received)
			}
			for _, e := range lote {
				if e.KV.ModRevision <= lastRev {
					t.Errorf("event out of order: %d to de %d", e.KV.ModRevision, lastRev)
				}
				lastRev = e.KV.ModRevision
				received++
			}
		case err := <-res.Errorc:
			t.Fatalf("erro no watch: %v", err)
		case <-deadline:
			t.Fatalf("timeout: recebeu %d de %d events", received, n)
		}
	}
}

func TestWatchWithConcurrentWrites(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

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
			t.Fatalf("Create concurrent: %v", err)
		}
	}

	seen := 0
	var lastRev int64
	for seen < n {
		select {
		case lote, ok := <-res.Events:
			if !ok {
				t.Fatalf("canal fechado after %d events", seen)
			}
			for _, e := range lote {
				if e.KV.ModRevision <= lastRev {
					t.Fatalf("event out of order: %d to de %d", e.KV.ModRevision, lastRev)
				}
				lastRev = e.KV.ModRevision
				seen++
			}
		case err := <-res.Errorc:
			t.Fatalf("erro no watch: %v", err)
		case <-ctxW.Done():
			t.Fatalf("timeout: recebeu %d de %d events", seen, n)
		}
	}
}

func TestLeaseExpiryWritesTombstone(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	chave := "/lease/curta"
	if _, err := b.Create(ctx, chave, []byte("temporario"), 1); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, kv, err := b.Get(ctx, chave, 0, false)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if kv == nil {
			var tomb Record
			if err := b.col.FindOne(ctx, bson.M{"name": chave, "deleted": true}).Decode(&tomb); err != nil {
				t.Fatalf("a key expirou sem tombstone: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("a key com lease did not expire")
}

func TestCompact(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

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

	target := rev
	done, err := b.Compact(ctx, target)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if done != target {
		t.Errorf("Compact returned %d, want %d", done, target)
	}

	_, kv, err := b.Get(ctx, chave, 0, false)
	if err != nil {
		t.Fatalf("Get after compact: %v", err)
	}
	if kv == nil || string(kv.Value) != "v5" {
		t.Fatalf("after compact, Get returned %v, want v5", kv)
	}

	if _, err := b.Compact(ctx, target); err != server.ErrCompacted {
		t.Errorf("Compact repetido returned %v, want ErrCompacted", err)
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
		t.Errorf("kine_ parameters leaked into the URI: %s", cfg.URI)
	}
	if !strings.Contains(cfg.URI, "retryWrites=true") {
		t.Errorf("a mongo parameter was stripped: %s", cfg.URI)
	}
	if _, err := ParseDSN("mongodb://h/?kine_desconhecido=1"); err == nil {
		t.Error("an unknown kine_ parameter should error")
	}
}

// TestWatchersShareOneStream — see docs/implementation-notes.md
func TestWatchersShareOneStream(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	const nWatchers = 8
	ctxW, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	res := make([]server.WatchResult, nWatchers)
	for i := range res {
		res[i] = b.Watch(ctxW, "/s/", "/s0", 0)
		if res[i].Events == nil {
			t.Fatalf("watcher %d got no channel", i)
		}
	}
	time.Sleep(3 * time.Second) // deixa o stream assentar

	if n := b.openStreams(); n != 1 {
		t.Errorf("streams abertos = %d, want 1 para %d watchers", n, nWatchers)
	}

	const nChaves = 4
	for i := 0; i < nChaves; i++ {
		if _, err := b.Create(ctx, fmt.Sprintf("/s/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	var wg sync.WaitGroup
	falhas := make([]string, nWatchers)
	for i := 0; i < nWatchers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seen := 0
			var lastRev int64
			deadline := time.After(35 * time.Second)
			for seen < nChaves {
				select {
				case lote, ok := <-res[i].Events:
					if !ok {
						falhas[i] = fmt.Sprintf("canal fechou com %d de %d", seen, nChaves)
						return
					}
					for _, e := range lote {
						if e.KV.ModRevision <= lastRev {
							falhas[i] = fmt.Sprintf("out of order: %d after %d", e.KV.ModRevision, lastRev)
							return
						}
						lastRev = e.KV.ModRevision
						seen++
					}
				case <-deadline:
					falhas[i] = fmt.Sprintf("timeout com %d de %d events", seen, nChaves)
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

// TestInvalidatedStreamBackfill — see docs/implementation-notes.md
func TestInvalidatedStreamBackfill(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	if _, err := b.Create(ctx, "/r/marker", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	b.mu.RLock()
	from_ := b.currentRev
	b.mu.RUnlock()

	const n = 6
	for i := 0; i < n; i++ {
		if _, err := b.Create(ctx, fmt.Sprintf("/r/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	b.mu.Lock()
	b.currentRev = from_
	b.mu.Unlock()

	saida := make(chan server.Events, 64)
	if ok := b.backfill(saida); !ok {
		t.Fatal("backfill() returned false")
	}
	close(saida)

	var revs []int64
	keys := map[string]bool{}
	for lote := range saida {
		for _, e := range lote {
			revs = append(revs, e.KV.ModRevision)
			keys[e.KV.Key] = true
		}
	}

	if len(revs) != n {
		t.Fatalf("recovered %d events, want %d", len(revs), n)
	}
	for i := 1; i < len(revs); i++ {
		if revs[i] <= revs[i-1] {
			t.Errorf("backfill out of order: %d after %d", revs[i], revs[i-1])
		}
	}
	for i := 0; i < n; i++ {
		if !keys[fmt.Sprintf("/r/%d", i)] {
			t.Errorf("key /r/%d was not recuperada", i)
		}
	}
	b.mu.RLock()
	to := b.currentRev
	b.mu.RUnlock()
	if to <= from_ {
		t.Errorf("currentRev did not advance after the backfill: %d -> %d", from_, to)
	}
}

// TestHistoryLostClassification — see docs/implementation-notes.md
func TestHistoryLostClassification(t *testing.T) {
	casos := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"erro comum", errors.New("connection reset"), false},
		{"ChangeStreamHistoryLost", mongo.CommandError{Code: errChangeStreamHistoryLost}, true},
		{"ChangeStreamFatalError", mongo.CommandError{Code: errChangeStreamFatalError}, true},
		{"another code", mongo.CommandError{Code: 11000}, false},
	}
	for _, c := range casos {
		if got := historyLost(c.err); got != c.want {
			t.Errorf("%s: historyLost=%v, want %v", c.name, got, c.want)
		}
	}
}

// TestRealOplogInvalidation — see docs/implementation-notes.md
func TestRealOplogInvalidation(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	if _, err := b.Create(ctx, "/inv/a", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	antigo := bson.Timestamp{T: uint32(time.Now().Add(-30 * 24 * time.Hour).Unix()), I: 1}
	cs, err := b.col.Watch(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"operationType": "insert"}}},
	}, options.ChangeStream().SetStartAtOperationTime(&antigo))

	if err == nil {
		cs.TryNext(ctx)
		err = cs.Err()
		cs.Close(context.Background())
	}
	if err == nil {
		t.Skip("MongoDB accepted a startAtOperationTime from 30 days ago - " +
			"the oplog window must have grown; no invalidation to observe")
	}

	t.Logf("erro devolvido: %v", err)
	if !historyLost(err) {
		t.Errorf("historyLost() did not recognise the real invalidation - "+
			"the MW-3 backfill would never fire. error: %v", err)
	}

	b.ctx = ctx
	novo, err := b.openStream(nil)
	if err != nil {
		t.Fatalf("openStream after invalidation failed: %v", err)
	}
	novo.Close(context.Background())
}

// TestRepeatedCompaction — see docs/implementation-notes.md
func TestRepeatedCompaction(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	const keys, geracoes = 10, 6
	revs := map[string]int64{}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("/c/%d", i)
		r, err := b.Create(ctx, k, []byte("g0"), 0)
		if err != nil {
			t.Fatal(err)
		}
		revs[k] = r
	}

	var docs []int64
	for g := 1; g <= geracoes; g++ {
		for i := 0; i < keys; i++ {
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
			t.Fatalf("Compact na generation %d: %v", g, err)
		}
		n, err := b.col.CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, n)
		t.Logf("generation %d: %d documents", g, n)
	}

	first, last := docs[0], docs[len(docs)-1]
	if last > first {
		t.Errorf("the database grew across compaction cycles: %d -> %d "+
			"(compact is not reclaiming space)", first, last)
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("/c/%d", i)
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		want := fmt.Sprintf("g%d", geracoes)
		if kv == nil || string(kv.Value) != want {
			t.Errorf("%s = %v, want %s", k, kv, want)
		}
	}
}

func testBackendOnCollection(t *testing.T, db, col string) (*Backend, context.Context, func()) {
	t.Helper()
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI is not set")
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

// TestTwoInstances — see docs/implementation-notes.md
func TestTwoInstances(t *testing.T) {
	db := "kine_multi"
	col := fmt.Sprintf("m%d", time.Now().UnixNano())

	a, ctxA, limparA := testBackendOnCollection(t, db, col)
	defer limparA()
	b2, ctxB, limparB := testBackendOnCollection(t, db, col)
	defer limparB()
	defer func() { _ = a.col.Drop(context.Background()); _ = a.meta.Drop(context.Background()) }()

	if a.cfg.EpochBase != b2.cfg.EpochBase {
		t.Fatalf("epoch base diverging: A=%d B=%d", a.cfg.EpochBase, b2.cfg.EpochBase)
	}

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

	vistas := map[int64]bool{}
	for i, r := range revs {
		if vistas[r] {
			t.Errorf("revision %d repetida entre instances", r)
		}
		vistas[r] = true
		if i > 0 && r <= revs[i-1] {
			t.Errorf("revisions not monotonic across instances: %d after %d", r, revs[i-1])
		}
	}

	received, lastRev := 0, int64(0)
	deadline := time.After(35 * time.Second)
	for received < n {
		select {
		case lote, ok := <-res.Events:
			if !ok {
				t.Fatalf("canal fechou com %d de %d", received, n)
			}
			for _, e := range lote {
				if e.KV.ModRevision <= lastRev {
					t.Errorf("out of order: %d after %d", e.KV.ModRevision, lastRev)
				}
				lastRev = e.KV.ModRevision
				received++
			}
		case err := <-res.Errorc:
			t.Fatalf("watch: %v", err)
		case <-deadline:
			t.Fatalf("timeout: %d of %d events (instance A did not see B's writes)", received, n)
		}
	}

	atual, err := a.CurrentRevision(ctxA)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); _, failures[0] = a.Compact(ctxA, atual) }()
	go func() { defer wg.Done(); _, failures[1] = b2.Compact(ctxB, atual) }()
	wg.Wait()

	sucessos := 0
	for _, e := range failures {
		if e == nil {
			sucessos++
		} else if e != server.ErrCompacted {
			t.Errorf("Compact concurrent returned erro inwant: %v", e)
		}
	}
	if sucessos != 1 {
		t.Errorf("%d instances compactionam com sucesso, want exatamente 1", sucessos)
	}

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("/multi/%d", i)
		for name, inst := range map[string]*Backend{"A": a, "B": b2} {
			ctx := ctxA
			if name == "B" {
				ctx = ctxB
			}
			_, kv, err := inst.Get(ctx, k, 0, false)
			if err != nil {
				t.Fatalf("Get %s em %s: %v", k, name, err)
			}
			if kv == nil {
				t.Errorf("instance %s does not see %s after compaction", name, k)
			}
		}
	}
}

// TestMetricsExposed — see docs/implementation-notes.md
func TestMetricsExposed(t *testing.T) {
	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	reg := prometheus.NewRegistry()
	registerMetrics(reg)

	if _, err := b.Create(ctx, "/m/a", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.List(ctx, "/m/", "/m0", 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Count(ctx, "/m/", "/m0", 0); err != nil {
		t.Fatal(err)
	}

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	vistas := map[string]bool{}
	for _, f := range fams {
		vistas[f.GetName()] = true
	}

	essenciais := []string{
		"kine_mongo_ops_total",
		"kine_mongo_op_duration_seconds",
		"kine_mongo_change_stream_lag_seconds",
		"kine_mongo_change_stream_reconnects_total",
		"kine_mongo_storage_bytes",
		"kine_mongo_current_revision",
		"kine_mongo_compacted_revision",
	}
	for _, name := range essenciais {
		if !vistas[name] {
			t.Errorf("metric %s is not registered", name)
		}
	}

	achou := map[string]bool{}
	for _, f := range fams {
		if f.GetName() != "kine_mongo_ops_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			var op, res string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "op":
					op = l.GetValue()
				case "result":
					res = l.GetValue()
				}
			}
			if res == "success" && m.GetCounter().GetValue() > 0 {
				achou[op] = true
			}
		}
	}
	for _, op := range []string{"append", "list", "count"} {
		if !achou[op] {
			t.Errorf("kine_mongo_ops_total sem sucesso registrado para op=%q", op)
		}
	}
}

// TestBackupRestore — see docs/implementation-notes.md
func TestBackupRestore(t *testing.T) {
	if _, err := exec.LookPath("mongodump"); err != nil {
		t.Skip("mongodump is not no PATH")
	}
	if _, err := exec.LookPath("mongorestore"); err != nil {
		t.Skip("mongorestore is not no PATH")
	}
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI is not set")
	}

	b, ctx, cleanup := testBackend(t)
	defer cleanup()

	keys := map[string]string{}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("/backup/k%d", i)
		v := fmt.Sprintf("value-%d", i)
		if _, err := b.Create(ctx, k, []byte(v), 0); err != nil {
			t.Fatal(err)
		}
		keys[k] = v
	}
	epochOriginal := b.cfg.EpochBase
	dbNome := b.cfg.Database
	colNome := b.cfg.Collection

	dir := t.TempDir()
	dump := exec.CommandContext(ctx, "mongodump", "--uri="+uri, "--db="+dbNome, "--out="+dir)
	if saida, err := dump.CombinedOutput(); err != nil {
		t.Fatalf("mongodump: %v\n%s", err, saida)
	}

	for _, want := range []string{colNome + ".bson", colNome + "_meta.bson"} {
		if _, err := os.Stat(filepath.Join(dir, dbNome, want)); err != nil {
			t.Errorf("the dump does not contain %s: %v", want, err)
		}
	}

	if err := b.col.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.meta.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.col.CountDocuments(ctx, bson.M{}); n != 0 {
		t.Fatalf("the collection was not dropped: %d documents", n)
	}

	rest := exec.CommandContext(ctx, "mongorestore", "--uri="+uri, "--drop",
		"--db="+dbNome, filepath.Join(dir, dbNome))
	if saida, err := rest.CombinedOutput(); err != nil {
		t.Fatalf("mongorestore: %v\n%s", err, saida)
	}

	var meta metaDoc
	if err := b.meta.FindOne(ctx, bson.M{"_id": metaID}).Decode(&meta); err != nil {
		t.Fatalf("kine_meta did not come back: %v", err)
	}
	if meta.EpochBase != epochOriginal {
		t.Errorf("epoch base after restore = %d, want %d", meta.EpochBase, epochOriginal)
	}

	for k, v := range keys {
		_, kv, err := b.Get(ctx, k, 0, false)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if kv == nil {
			t.Errorf("%s did not come back do backup", k)
			continue
		}
		if string(kv.Value) != v {
			t.Errorf("%s = %q, want %q", k, kv.Value, v)
		}
	}
}
