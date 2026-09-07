# Backend MongoDB para o kine

Roda um cluster Kubernetes (k3s) usando **MongoDB** como datastore, no lugar do etcd.

> **Estado:** v0.1 — validado com um k3s real (nó `Ready`, deployment, scale, rolling update, `kubectl exec`/`logs`). Não é para produção crítica; leia [Quando não usar](#quando-não-usar).

---

## Como funciona

O kine traduz a API etcd v3 que o `kube-apiserver` fala para o datastore de baixo. Os drivers SQL (SQLite, PostgreSQL, MySQL) compartilham a interface `server.Dialect`, que devolve `*sql.Rows`. MongoDB não fala SQL, então este driver implementa `server.Backend` diretamente — como fazem os drivers `nats` e `t4`.

Duas decisões moldam o resto, e ambas têm ADR:

| Decisão | Por quê | ADR |
|---|---|---|
| A revisão do etcd é o **`clusterTime`** do MongoDB, não um contador | Um contador sofre `WriteConflict` sob concorrência (8,2 escritas/s) e o Change Stream entrega fora da ordem dele | [ADR-0001](adr/0001-revisao-por-clustertime.md) |
| O watch escuta só `insert` e deriva a revisão do **`clusterTime` do evento** | Esperar o update que grava `rev` fazia eventos desaparecerem e travava o apiserver | [ADR-0002](adr/0002-watch-por-clustertime-do-insert.md) |

O watch usa **Change Streams**, não polling. Os drivers SQL rodam uma query por segundo para sempre; aqui existe **um único stream por processo**, compartilhado por todos os watchers via `pkg/broadcaster`.

---

## Requisitos

- **MongoDB como replica set.** Change Streams e transações exigem isso. O Atlas M0 (free) serve — é um replica set de 3 nós.
- Um usuário com `readWrite` no banco escolhido.

Não funciona em MongoDB standalone.

---

## Uso

```bash
kine --endpoint "mongodb+srv://usuario:senha@cluster.exemplo.mongodb.net/?retryWrites=true&w=majority"
```

E no k3s, apontando para um kine já em execução:

```bash
k3s server --datastore-endpoint="http://127.0.0.1:2399"
```

### Parâmetros da DSN

A connection string é a do MongoDB. Parâmetros extras usam o prefixo `kine_` e são removidos antes de repassar a URI ao driver oficial, que rejeita chaves desconhecidas.

| Parâmetro | Default | Para quê |
|---|---|---|
| `kine_database` | `kine` | Banco onde as coleções vivem. Também aceito no caminho da URI |
| `kine_collection` | `kine` | Coleção do log de revisões |
| `kine_epoch_base` | `1767225600` | Instante a partir do qual as revisões são contadas — **ver abaixo** |
| `kine_connect_timeout` | `30s` | Timeout da conexão inicial |
| `kine_server_selection_timeout` | `30s` | Espera por um servidor elegível |

**Sobre o `kine_epoch_base`:** a revisão é `(clusterTime.T - epochBase) << 20 | clusterTime.I`. Sem a subtração, o deslocamento estoura o `int64` por volta de 2038. O valor é gravado nos metadados na primeira execução e **não pode mudar depois** — alterá-lo reescreveria o significado de toda revisão já entregue ao apiserver. O driver recusa iniciar se a DSN pedir um valor diferente do gravado.

`writeConcern` e `readConcern` são fixados em `majority`: o kine precisa ler a revisão que acabou de gravar. Medido — custa 26,6 ms contra 27,2 ms do write default, diferença indistinguível.

---

## Coleções criadas

| Coleção | Conteúdo |
|---|---|
| `kine` | o log de revisões (uma entrada por mutação) |
| `kine_meta` | epoch base e revisão compactada do cluster |

Índices em `kine`: `(name, rev)`, `(rev)`, `(name, prev_revision)` **único**, `(prev_revision)`. O índice único é o que produz o `ErrKeyExists` do etcd quando dois clientes criam a mesma chave.

---

## Setup no Atlas (free tier)

1. Crie um cluster **M0**. Prefira a região mais próxima de onde o kine vai rodar — a latência de escrita entra direto no caminho da leader election.
2. Crie um usuário de banco com `readWrite`.
3. Em **Network Access**, libere o IP de onde o kine roda.
4. Pegue a connection string em **Connect → Drivers**.

Guarde a credencial fora do repositório. Nos scripts deste projeto ela vive em `~/.config/kine-mongo/env`:

```bash
MONGO_URI='mongodb+srv://usuario:senha@cluster.exemplo.mongodb.net/?retryWrites=true&w=majority'
```

As aspas simples importam: a URI contém `&`, que o shell interpretaria como operador ao dar `source`.

---

## O que foi medido

Contra um Atlas M0 em São Paulo, e um k3s v1.36.4 real em EC2:

| | |
|---|---|
| Latência de escrita (`insert` w=majority) | **26,6 ms** p50 |
| Latência de leitura (`find` por índice) | **21,9 ms** p50 |
| Nó `Ready` | **~4 s** |
| `readyz` do apiserver | **~30 s** |
| Operações num bootstrap completo | 4.866 WATCH · 82 LIST · 4 DELETE · **0 erros** |
| Tamanho de um cluster k3s inteiro | **~1 MB** (842 documentos) |
| Latência vista pelo apiserver, 1 escrita por vez | **~87 ms** p50 |
| Latência vista pelo apiserver, 12 escritas concorrentes | **~884 ms** p50 |
| Projeção nos 512 MB do M0 | **~429 mil documentos** |
| Janela do oplog no M0 | **~4,4 h** |
| Teto real de escrita (M0) | **~92 ops/s** — acima disso a latência explode |
| Teto prático de mutações do cluster | **~30/s** (cada mutação custa ~2 ops) |

Detalhes e como reproduzir em [`spikes/results/`](../spikes/results/).

---

## Capacidade e o que monitorar

O teto do M0 é **~92 operações/s de escrita**. Cada mutação do kine custa ~2 operações (o insert e o update que grava a revisão), então:

| | Mutações/s do cluster |
|---|---|
| Teto absoluto | ~45 |
| **Onde a latência ainda é sadia** | **~30** |
| Cluster médio ocioso | 3,3 |
| Cluster médio em operação | 10 |

Folga de 3× a 9× para um cluster médio — suficiente, não confortável.

**O que estourar o teto faz não é falhar; é ficar lento.** Medido em [MSPIKE-6](../spikes/results/mspike-6.md): zero erros até 6× o teto, mas o p99 de escrita sai de 810 ms para **63 s**, e o change stream fica **44 s atrasado**. Para o Kubernetes isso é pior que um erro: a leader election tem `RenewDeadline` de 10 s, então scheduler e controller-manager perdem a liderança sem que nada no log diga por quê.

Por isso, o que monitorar **não é taxa de erro** — não vai haver erro:

| Sinal | Alerta |
|---|---|
| **Latência de escrita p99** | acima de ~1 s, o cluster está a caminho de perder a liderança |
| **Concorrência de escrita** | é ela que dói, não o volume: 1 escrita por vez custa ~87 ms; 12 simultâneas custam ~884 ms ([MT-4](../spikes/results/mt-1-2-4-5.md)) |
| **Atraso do change stream** | acima de alguns segundos, os informers estão obsoletos |
| Storage usado | o M0 não expande; ao encher, o cluster para |

As métricas expostas pelo driver, todas com prefixo `kine_mongo_`:

| Métrica | |
|---|---|
| `ops_total{op,result}` | contagem por operação |
| `op_duration_seconds{op}` | histograma, buckets de 1 ms a ~16 s |
| **`change_stream_lag_seconds`** | idade do último evento — **a que detecta o modo de falha real** |
| `change_stream_reconnects_total{motivo}` | `queda`, `historico_perdido`, `falha_ao_abrir` |
| `storage_bytes{componente}` | `dados`, `storage`, `indices` |
| `current_revision`, `compacted_revision` | posição do log |

## Quando não usar

Seja honesto sobre o que isso é.

**Não use se:**

- **É produção crítica.** Este driver é novo, não tem uso em campo, e não passou pela suíte de conformidade etcd completa (`MT-2`).
- **O cluster tem muito churn** — CI criando milhares de Jobs, operadores com reconcile agressivo. O teto é ~30 mutações/s antes da latência machucar a leader election ([MSPIKE-6](../spikes/results/mspike-6.md)), e o sintoma de estourar é o cluster travando sem mensagem de erro.
- **A latência importa.** Cada operação do apiserver paga a ida e volta até o MongoDB. Com o banco longe, isso vira dezenas ou centenas de milissegundos, e a leader election tem deadlines de 5-10 s.
- **Você precisa de backup automático no M0.** O free tier não tem; só `mongodump` manual — runbook em [backup-restore.md](backup-restore.md).
- **Não pode usar replica set.** Standalone não serve.

**Faz sentido para:** homelab, borda, dev/staging, clusters pequenos onde "não administrar banco" vale mais que milissegundos, e onde o Atlas já é parte da stack.

### Riscos conhecidos

| Risco | Situação |
|---|---|
| Watch fica para trás da janela do oplog (~4,4 h no M0) e invalida | Tratado: o driver detecta `ChangeStreamHistoryLost` e recupera por leitura direta ([MW-3](../spikes/results/mt3.md)) |
| M0 não expande storage — ao encher, o cluster para | Folga grande (~429 mil documentos), mas **sem alerta ainda** (`MOPS-1` aberto) |
| Teto de 100 ops/s no M0 | **Medido** ([MSPIKE-6](../spikes/results/mspike-6.md)): não gera erro, gera latência. p99 vai a **63 s** a 6× o teto — e a leader election tem deadline de 10 s |
| Change stream fica para trás sob carga | **Medido**: 44 s de atraso a 6× o teto, sem perder evento. Um watch atrasado é um cluster que não vê suas mudanças |
| Revisões não são densas — saltam | Por desenho ([ADR-0001](adr/0001-revisao-por-clustertime.md)). O apiserver trata `resourceVersion` como valor opaco; validado com k3s real |
| Órfão com `rev = 0` se o processo morrer entre insert e update | Inerte, não corrompe estado ([ADR-0002](adr/0002-watch-por-clustertime-do-insert.md)) |

---

## Desenvolvimento

```bash
go build ./...
go test ./...                                    # unitários, sem credencial

export KINE_MONGO_TEST_URI="mongodb+srv://..."   # integração contra Mongo real
go test -tags=integration ./pkg/drivers/mongo/ ./test/mongo/
```

Os testes de integração criam uma coleção própria por execução e a removem no fim. `test/mongo/` exercita o backend pelo **protocolo etcd v3 real** — inclusive `Txn` com `Compare(ModRevision)`, que é como o apiserver faz concorrência otimista.

### Scripts

| Script | Para quê |
|---|---|
| `hack/ec2-mt3.sh criar` / `destruir` | Provisiona uma EC2, roda o teste de k3s completo, destrói |
| `hack/k3s-mongo.sh` | Sobe kine + k3s localmente (precisa Linux de verdade — ver abaixo) |
| `hack/k3s-diag.sh` | Sobe só o k3s contra um kine já em execução, sem matar nada no fim |
| `hack/k3s-limpar.sh` | Derruba tudo do k3s, inclusive instâncias órfãs |
| `spikes/*.py` | As medições, reproduzíveis |

Backup e restore: [docs/backup-restore.md](backup-restore.md) — o M0 não tem backup automático.

**Não tente no WSL2.** O `modprobe iptable_nat` trava em estado `D` (uninterruptible) dentro do kernel do WSL, imune a `kill -9`. Como o carregamento de módulos é serializado, todo `modprobe` seguinte fica preso atrás dele e o k3s espera para sempre — sem gerar erro no log. Use `hack/ec2-mt3.sh` ou uma VM.
