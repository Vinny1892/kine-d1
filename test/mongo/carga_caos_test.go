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

// MT-4 — carga: mede ops/s, latência vista pelo cliente etcd (que é o que o
// apiserver sente) e o crescimento em disco.
//
// O MSPIKE-6 mediu o driver do MongoDB direto. Aqui a medição atravessa a
// pilha inteira: gRPC → kine → MongoDB, que é o caminho real.
func TestCargaPelaPilhaCompleta(t *testing.T) {
	if os.Getenv("KINE_MONGO_CARGA") == "" {
		t.Skip("defina KINE_MONGO_CARGA=1 para rodar o teste de carga")
	}
	cli, ctx := etcdClient(t)

	const (
		objetos      = 300
		tamanhoObj   = 3 * 1024 // objeto k8s típico
		concorrencia = 12
	)
	valor := make([]byte, tamanhoObj)
	for i := range valor {
		valor[i] = byte('a' + i%26)
	}

	titulo := func(s string) { t.Logf("--- %s", s) }

	titulo(fmt.Sprintf("escrevendo %d objetos de %d KB, concorrência %d",
		objetos, tamanhoObj/1024, concorrencia))
	lat := make([]float64, 0, objetos)
	var mu sync.Mutex
	sem := make(chan struct{}, concorrencia)
	var wg sync.WaitGroup
	inicio := time.Now()
	erros := 0
	for i := 0; i < objetos; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			t0 := time.Now()
			_, err := cli.Put(ctx, fmt.Sprintf("/registry/carga/o%04d", i), string(valor))
			d := time.Since(t0).Seconds() * 1000
			mu.Lock()
			if err != nil {
				erros++
			} else {
				lat = append(lat, d)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	dur := time.Since(inicio)

	t.Logf("  %d escritas em %.1fs = %.1f ops/s (%d erros)",
		len(lat), dur.Seconds(), float64(len(lat))/dur.Seconds(), erros)
	t.Logf("  latência p50=%.0fms p95=%.0fms p99=%.0fms",
		pct(lat, .50), pct(lat, .95), pct(lat, .99))

	if erros > 0 {
		t.Errorf("%d escritas falharam sob carga", erros)
	}
	// A leader election do k8s tem RenewDeadline de 10s. Um p99 acima disso
	// significa cluster perdendo liderança sob esta carga.
	if p99 := pct(lat, .99); p99 > 10000 {
		t.Errorf("p99 de %.0fms excede o RenewDeadline de 10s da leader election", p99)
	}

	titulo("LIST de tudo que foi escrito")
	t0 := time.Now()
	r, err := cli.Get(ctx, "/registry/carga/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	t.Logf("  %d chaves (%.1f MB) em %dms",
		len(r.Kvs), float64(len(r.Kvs)*tamanhoObj)/1024/1024, time.Since(t0).Milliseconds())
	if len(r.Kvs) != objetos {
		t.Errorf("LIST devolveu %d, esperado %d", len(r.Kvs), objetos)
	}

	titulo("LIST keys-only, que é o caminho que o driver otimiza")
	t0 = time.Now()
	rk, err := cli.Get(ctx, "/registry/carga/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("  %d chaves em %dms", len(rk.Kvs), time.Since(t0).Milliseconds())
}

// MT-5 — caos: o watch precisa sobreviver a interrupções sem perder revisão.
func TestCaosWatchSobreviveAInterrupcao(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/caos/"

	p, err := cli.Put(ctx, pref+"inicio", "v")
	if err != nil {
		t.Fatal(err)
	}
	base := p.Header.Revision

	// Primeiro watch: recebe alguns eventos e é cancelado no meio.
	wctx1, cancel1 := context.WithCancel(ctx)
	ch1 := cli.Watch(wctx1, pref, clientv3.WithPrefix(), clientv3.WithRev(base+1))
	time.Sleep(2 * time.Second)

	for i := 0; i < 3; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sa%d", pref, i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	var ultimaVista int64
	recebidos := 0
	prazo := time.After(30 * time.Second)
	for recebidos < 3 {
		select {
		case wr := <-ch1:
			if wr.Err() != nil {
				t.Fatalf("watch 1: %v", wr.Err())
			}
			for _, ev := range wr.Events {
				ultimaVista = ev.Kv.ModRevision
				recebidos++
			}
		case <-prazo:
			t.Fatalf("timeout no watch 1: %d de 3", recebidos)
		}
	}
	cancel1()
	t.Logf("watch 1 cancelado após ver até a revisão %d", ultimaVista)

	// Escritas enquanto nenhum watch está ativo — a "janela de queda".
	for i := 0; i < 4; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sb%d", pref, i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	// Segundo watch, retomando de onde o primeiro parou. Nada pode ter sido
	// perdido na janela: é assim que o apiserver reconecta.
	wctx2, cancel2 := context.WithTimeout(ctx, 40*time.Second)
	defer cancel2()
	ch2 := cli.Watch(wctx2, pref, clientv3.WithPrefix(), clientv3.WithRev(ultimaVista+1))

	vistos := map[string]bool{}
	var anterior int64
	prazo = time.After(35 * time.Second)
	for len(vistos) < 4 {
		select {
		case wr := <-ch2:
			if wr.Err() != nil {
				t.Fatalf("watch 2: %v", wr.Err())
			}
			for _, ev := range wr.Events {
				if ev.Kv.ModRevision <= anterior {
					t.Errorf("fora de ordem após reconectar: %d após %d", ev.Kv.ModRevision, anterior)
				}
				anterior = ev.Kv.ModRevision
				vistos[string(ev.Kv.Key)] = true
			}
		case <-prazo:
			t.Fatalf("timeout: recuperou %d de 4 escritas da janela de queda (vistos: %v)",
				len(vistos), vistos)
		}
	}
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("%sb%d", pref, i)
		if !vistos[k] {
			t.Errorf("perdeu o evento de %s na janela de queda", k)
		}
	}
	t.Logf("watch 2 recuperou todas as %d escritas da janela", len(vistos))
}

// TestCaosEscritasDurantePicoDeLeitura verifica que leitura pesada não faz
// escrita falhar — o cenário de um informer relistando enquanto o cluster opera.
func TestCaosEscritasDurantePicoDeLeitura(t *testing.T) {
	cli, ctx := etcdClient(t)
	pref := "/registry/misto/"

	for i := 0; i < 100; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("%sk%03d", pref, i), "conteudo"); err != nil {
			t.Fatal(err)
		}
	}

	parar := make(chan struct{})
	var leituras int
	var mu sync.Mutex
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-parar:
					return
				default:
					if _, err := cli.Get(ctx, pref, clientv3.WithPrefix()); err == nil {
						mu.Lock()
						leituras++
						mu.Unlock()
					}
				}
			}
		}()
	}

	// Escritas durante o pico de leitura.
	lat := []float64{}
	erros := 0
	for i := 0; i < 30; i++ {
		t0 := time.Now()
		_, err := cli.Put(ctx, fmt.Sprintf("%sw%02d", pref, i), "v")
		if err != nil {
			erros++
		} else {
			lat = append(lat, time.Since(t0).Seconds()*1000)
		}
	}
	close(parar)
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	nl := leituras
	mu.Unlock()
	t.Logf("%d LISTs completos em paralelo; %d escritas, %d erros", nl, len(lat), erros)
	t.Logf("latência das escritas sob pico de leitura: p50=%.0fms p99=%.0fms",
		pct(lat, .50), pct(lat, .99))
	if erros > 0 {
		t.Errorf("%d escritas falharam durante o pico de leitura", erros)
	}
}
