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

// Exercita o backend pelo protocolo etcd v3 real, do jeito que o kube-apiserver
// faz. É a validação mais próxima do MT-3 que se consegue sem subir um k3s.
//
// O que importa aqui é o Txn com Compare(ModRevision): é assim que o apiserver
// implementa concorrência otimista em toda escrita. Se isso não funcionar, nada
// funciona — e é exatamente o ponto onde a decisão de derivar a revisão do
// clusterTime (ADR-0001) poderia falhar, por não ser uma sequência densa.
func etcdClient(t *testing.T) (*clientv3.Client, context.Context) {
	t.Helper()
	uri := os.Getenv("KINE_MONGO_TEST_URI")
	if uri == "" {
		t.Skip("KINE_MONGO_TEST_URI não definida")
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	uri += fmt.Sprintf("%skine_database=kine_proto&kine_collection=p%d", sep, time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	cfg := endpoint.Config{
		Endpoint:  uri,
		Listener:  fmt.Sprintf("unix://%s/kine-proto.sock", t.TempDir()),
		WaitGroup: wg,
		// Sem isso o kine faz rand.Int63n(0) em pkg/server/watch.go:35 e
		// entra em panic ao abrir o primeiro watch.
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

// TestTxnCriacao reproduz como o apiserver cria um objeto: um Txn que só grava
// se a chave ainda não existir (ModRevision == 0).
func TestTxnCriacao(t *testing.T) {
	cli, ctx := etcdClient(t)
	chave := "/registry/pods/default/nginx"

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", 0)).
		Then(clientv3.OpPut(chave, "objeto-v1")).
		Commit()
	if err != nil {
		t.Fatalf("Txn de criação: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("Txn de criação não sucedeu com a chave inexistente")
	}

	// A segunda criação precisa falhar — é assim que o apiserver detecta
	// AlreadyExists.
	resp2, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", 0)).
		Then(clientv3.OpPut(chave, "objeto-duplicado")).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn de criação duplicada: %v", err)
	}
	if resp2.Succeeded {
		t.Error("Txn de criação sucedeu numa chave que já existe")
	}
	if len(resp2.Responses) == 0 {
		t.Fatal("o ramo Else não devolveu o valor atual")
	}
	rr := resp2.Responses[0].GetResponseRange()
	if len(rr.Kvs) != 1 || string(rr.Kvs[0].Value) != "objeto-v1" {
		t.Errorf("Else devolveu %v, esperado objeto-v1", rr.Kvs)
	}
}

// TestTxnAtualizacao reproduz o update do apiserver: grava só se a
// ModRevision for exatamente a que ele leu.
func TestTxnAtualizacao(t *testing.T) {
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
		t.Fatal("Txn de update falhou com a revisão correta — concorrência otimista quebrada")
	}

	// Repetir com a revisão velha precisa falhar: é o Conflict do apiserver.
	resp2, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(chave), "=", rev)).
		Then(clientv3.OpPut(chave, "v3")).
		Else(clientv3.OpGet(chave)).
		Commit()
	if err != nil {
		t.Fatalf("Txn com revisão velha: %v", err)
	}
	if resp2.Succeeded {
		t.Error("Txn sucedeu com revisão desatualizada — o apiserver perderia escritas")
	}
}

// TestRevisaoMonotonicaNoProtocolo é o teste que valida o ADR-0001 pelo
// protocolo: as revisões que o apiserver enxerga precisam ser estritamente
// crescentes. Elas NÃO são densas — saltam — e é isso que precisa ser tolerado.
func TestRevisaoMonotonicaNoProtocolo(t *testing.T) {
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
			t.Fatalf("revisão não avançou: %d depois de %d", revs[i], revs[i-1])
		}
	}
	saltos := 0
	for i := 1; i < len(revs); i++ {
		if revs[i] != revs[i-1]+1 {
			saltos++
		}
	}
	t.Logf("revisões: %v", revs)
	t.Logf("%d de %d incrementos não são densos (esperado com clusterTime)", saltos, len(revs)-1)
}

// TestWatchProtocolo exercita o watch do jeito que o watch cache do apiserver
// faz: a partir de uma revisão conhecida, esperando eventos em ordem.
func TestWatchProtocolo(t *testing.T) {
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

	vistos := 0
	var ultima int64
	for vistos < 3 {
		select {
		case wr, ok := <-ch:
			if !ok {
				t.Fatalf("canal de watch fechou após %d eventos", vistos)
			}
			if wr.Err() != nil {
				t.Fatalf("watch: %v", wr.Err())
			}
			for _, ev := range wr.Events {
				if ev.Kv.ModRevision <= ultima {
					t.Errorf("evento fora de ordem: %d após %d", ev.Kv.ModRevision, ultima)
				}
				ultima = ev.Kv.ModRevision
				vistos++
			}
		case <-time.After(25 * time.Second):
			t.Fatalf("timeout: %d de 3 eventos", vistos)
		}
	}
}

// TestListPorPrefixo cobre o LIST que o apiserver faz no start de cada informer.
func TestListPorPrefixo(t *testing.T) {
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
		t.Fatalf("LIST devolveu %d chaves, esperado 8", len(resp.Kvs))
	}
	for i := 1; i < len(resp.Kvs); i++ {
		if string(resp.Kvs[i-1].Key) >= string(resp.Kvs[i].Key) {
			t.Errorf("LIST fora de ordem: %s antes de %s", resp.Kvs[i-1].Key, resp.Kvs[i].Key)
		}
	}
	if resp.Header.Revision == 0 {
		t.Error("LIST não trouxe revisão no header — o watch cache depende dela")
	}

	cresp, err := cli.Get(ctx, pref, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if cresp.Count != 8 {
		t.Errorf("count=%d, esperado 8", cresp.Count)
	}
}
