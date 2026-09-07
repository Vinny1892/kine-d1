//go:build integration

package mongo_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func pct(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	i := int(float64(len(v)) * q)
	if i >= len(v) {
		i = len(v) - 1
	}
	return v[i]
}

// TestLoadThroughFullStack — see docs/implementation-notes.md
func TestLoadThroughFullStack(t *testing.T) {
	if os.Getenv("KINE_MONGO_CARGA") == "" {
		t.Skip("defina KINE_MONGO_CARGA=1 para rodar o teste de carga")
	}
	cli, ctx := etcdClient(t)

	const (
		objects     = 300
		objSize     = 3 * 1024 // typical k8s object
		concurrency = 12
	)
	payload := make([]byte, objSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	section := func(s string) { t.Logf("--- %s", s) }

	section(fmt.Sprintf("writing %d objects of %d KB, concurrency %d",
		objects, objSize/1024, concurrency))
	lat := make([]float64, 0, objects)
	var mu sync.Mutex
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	inicio := time.Now()
	failures := 0
	for i := 0; i < objects; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			_, err := cli.Put(ctx, fmt.Sprintf("/registry/carga/o%04d", i), string(payload))
			d := time.Since(t0).Seconds() * 1000
			mu.Lock()
			if err != nil {
				failures++
			} else {
				lat = append(lat, d)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	dur := time.Since(inicio)

	t.Logf("  %d writes em %.1fs = %.1f ops/s (%d failures)",
		len(lat), dur.Seconds(), float64(len(lat))/dur.Seconds(), failures)
	t.Logf("  latency p50=%.0fms p95=%.0fms p99=%.0fms",
		pct(lat, .50), pct(lat, .95), pct(lat, .99))

	if failures > 0 {
		t.Errorf("%d writes falharam under load", failures)
	}
	if p99 := pct(lat, .99); p99 > 10000 {
		t.Errorf("p99 de %.0fms excede o RenewDeadline de 10s da leader election", p99)
	}

	section("LIST de tudo que foi escrito")
	t0 := time.Now()
	r, err := cli.Get(ctx, "/registry/carga/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	t.Logf("  %d keys (%.1f MB) em %dms",
		len(r.Kvs), float64(len(r.Kvs)*objSize)/1024/1024, time.Since(t0).Milliseconds())
	if len(r.Kvs) != objects {
		t.Errorf("LIST returned %d, want %d", len(r.Kvs), objects)
	}

	section("LIST keys-only")
	t0 = time.Now()
	rk, err := cli.Get(ctx, "/registry/carga/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("  %d keys em %dms", len(rk.Kvs), time.Since(t0).Milliseconds())
}

// TestChaosWatchSurvivesInterruption — see docs/implementation-notes.md
func TestChaosWatchSurvivesInterruption(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/caos/"

	p, err := cli.Put(ctx, pref+"inicio", "v")
	if err != nil {
		t.Fatal(err)
	}
	base := p.Header.Revision

	wctx1, cancel1 := context.WithCancel(ctx)
	ch1 := cli.Watch(wctx1, pref, clientv3.WithPrefix(), clientv3.WithRev(base+1))
	time.Sleep(2 * time.Second)

	for i := 0; i < 3; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sa%d", pref, i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	var lastSeen int64
	received := 0
	deadline := time.After(30 * time.Second)
	for received < 3 {
		select {
		case wr := <-ch1:
			if wr.Err() != nil {
				t.Fatalf("watch 1: %v", wr.Err())
			}
			for _, ev := range wr.Events {
				lastSeen = ev.Kv.ModRevision
				received++
			}
		case <-deadline:
			t.Fatalf("timeout no watch 1: %d de 3", received)
		}
	}
	cancel1()
	t.Logf("watch 1 cancelled after seeing revision %d", lastSeen)

	for i := 0; i < 4; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sb%d", pref, i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	wctx2, cancel2 := context.WithTimeout(ctx, 40*time.Second)
	defer cancel2()
	ch2 := cli.Watch(wctx2, pref, clientv3.WithPrefix(), clientv3.WithRev(lastSeen+1))

	seen := map[string]bool{}
	var previous int64
	deadline = time.After(35 * time.Second)
	for len(seen) < 4 {
		select {
		case wr := <-ch2:
			if wr.Err() != nil {
				t.Fatalf("watch 2: %v", wr.Err())
			}
			for _, ev := range wr.Events {
				if ev.Kv.ModRevision <= previous {
					t.Errorf("out of order after reconectar: %d after %d", ev.Kv.ModRevision, previous)
				}
				previous = ev.Kv.ModRevision
				seen[string(ev.Kv.Key)] = true
			}
		case <-deadline:
			t.Fatalf("timeout: recovered %d de 4 writes da window gap (seen: %v)",
				len(seen), seen)
		}
	}
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("%sb%d", pref, i)
		if !seen[k] {
			t.Errorf("lost o event de %s na window gap", k)
		}
	}
	t.Logf("watch 2 recovered all %d writes da window", len(seen))
}

// TestChaosWritesDuringReadSpike — see docs/implementation-notes.md
func TestChaosWritesDuringReadSpike(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/misto/"

	for i := 0; i < 100; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sk%03d", pref, i), "content"); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var reads int
	var mu sync.Mutex
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := cli.Get(ctx, pref, clientv3.WithPrefix()); err == nil {
						mu.Lock()
						reads++
						mu.Unlock()
					}
				}
			}
		}()
	}

	lat := []float64{}
	failures := 0
	for i := 0; i < 30; i++ {
		t0 := time.Now()
		_, err := cli.Put(ctx, fmt.Sprintf("%sw%02d", pref, i), "v")
		if err != nil {
			failures++
		} else {
			lat = append(lat, time.Since(t0).Seconds()*1000)
		}
	}
	close(stop)
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	nl := reads
	mu.Unlock()
	t.Logf("%d LISTs completos em paralelo; %d writes, %d failures", nl, len(lat), failures)
	t.Logf("write latency during the read spike: p50=%.0fms p99=%.0fms",
		pct(lat, .50), pct(lat, .99))
	if failures > 0 {
		t.Errorf("%d writes falharam durante o read spike", failures)
	}
}
