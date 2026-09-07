# kine-mongo — backend MongoDB Atlas para o kine

> **Status:** pesquisa / anteprojeto
> **Data:** 2026-09-06
> **Upstream:** [k3s-io/kine](https://github.com/k3s-io/kine) commit `6fb95f5`, release `v0.17.0`
> **Repositório:** fork em [Vinny1892/kine-mongo](https://github.com/Vinny1892/kine-mongo)
> **Alvo:** Atlas **M0 (free tier)**
> **Histórico:** este projeto avaliou o Cloudflare D1 antes e o [descartou por custo](docs/d1-descartado/README.md)

---

## 1. Objetivo

Implementar um backend MongoDB para o kine, permitindo rodar um cluster k3s cujo "etcd" é um MongoDB Atlas.

**Não existe precedente.** O kine suporta SQLite, PostgreSQL, MySQL, NATS e t4 — nenhum backend de documento. Isso é ao mesmo tempo a oportunidade e o risco deste projeto.

## 2. Por que MongoDB depois do D1

A avaliação do D1 concluiu que **tudo funcionava, menos o preço**: US$ 22/mês por write/s sustentado, sem freio algum. O control plane sozinho já consumia 87% da franquia gratuita, e um operador com `RequeueAfter: 10s` sobre 500 objetos custava US$ 1.037/mês.

O Atlas muda a natureza do risco:

| | Cloudflare D1 | Atlas M0 |
|---|---|---|
| Modelo de cobrança | **por operação** | **por instância** (grátis) |
| Excesso de carga vira | **fatura** | **lentidão** (throttle em 100 ops/s) |
| Teto de gasto | nenhum | **US$ 0, por construção** |
| Latência (do Brasil) | 232 ms (sem região SA) | ~10-30 ms (**região São Paulo**) |
| Storage | 10 GB | **512 MB** |

> **A troca central:** sai um sistema que degrada em dinheiro, entra um que degrada em performance. Para um cluster que precisa ser previsível, é a troca certa.

---

## 3. O problema arquitetural: o kine é SQL

Esta é a diferença mais importante em relação ao D1, e define o tamanho do trabalho.

O ponto de extensão natural do kine é `server.Dialect` (`pkg/server/types.go:43`), e ele devolve **`*sql.Rows`**:

```go
type Dialect interface {
    ListCurrent(ctx context.Context, ...) (*sql.Rows, error)
    List(ctx context.Context, ...) (*sql.Rows, error)
    ...
}
```

Com o D1, isso era uma dádiva: bastava um driver `database/sql` e herdávamos ~90% do código via `pkg/drivers/generic`. **Com MongoDB essa economia desaparece por completo.**

### O caminho obrigatório: `server.Backend` direto

MongoDB precisa implementar a interface de cima, `server.Backend` (`pkg/server/types.go:28`) — como fazem os drivers `nats` (`pkg/drivers/nats/`) e `t4` (`pkg/drivers/t4/`):

```go
type Backend interface {
    Start(ctx) error
    Get(ctx, key, revision, keysOnly) (int64, *KeyValue, error)
    Create(ctx, key, value, lease) (int64, error)
    Delete(ctx, key, revision) (int64, *KeyValue, bool, error)
    List(ctx, key, end, limit, revision, keysOnly) (int64, []*KeyValue, error)
    Count(ctx, key, end, revision) (int64, int64, error)
    Update(ctx, key, value, revision, lease) (int64, *KeyValue, bool, error)
    Watch(ctx, key, end, revision) WatchResult
    DbSize(ctx) (int64, error)
    CurrentRevision(ctx) (int64, error)
    Compact(ctx, revision) (int64, error)
    WaitForSyncTo(revision)
}
```

Isso significa **reimplementar do zero** o que o `pkg/logstructured` e o `pkg/logstructured/sqllog` faziam de graça: semântica de revisões, watch, compactação, gap-fill e broadcast.

**É a "Opção C" que a avaliação do D1 descartou como custo altíssimo.** Aqui ela é a única opção.

### O que o MongoDB devolve em troca

O trabalho extra não é gratuito, mas compra capacidades que o caminho SQL não tinha:

| Recurso | O que substitui | Ganho |
|---|---|---|
| **Change Streams** | o laço de polling de 1 s (`sqllog/sql.go:486`) | watch em **tempo real**, sem polling |
| **TTL indexes** | o `pkg/ttl` e a varredura de leases | expiração pelo banco |
| **Transações ACID** multi-documento | a transação diferida com CAS do D1 | transação de verdade, interativa |
| **`findAndModify`** atômico | `AUTOINCREMENT` | sequência de revisões |

A eliminação do polling é o ganho mais subestimado: além de derrubar a latência de watch de ~1 s para milissegundos, remove a fonte de carga que rodava 86.400 vezes por dia mesmo com o cluster parado.

---

## 4. O desafio técnico central: revisões monotônicas

No kine, a revisão do etcd é o `id` da linha — um `INTEGER PRIMARY KEY AUTOINCREMENT`. O apiserver depende de três propriedades: **monotônica, sem buracos permanentes, e visível em ordem**.

MongoDB não tem AUTOINCREMENT. O padrão é uma coleção de contadores:

```javascript
db.counters.findAndModify({
  query:  { _id: "revision" },
  update: { $inc: { seq: 1 } },
  new:    true
})
```

Isso é atômico, mas cria três problemas que precisam de spike:

1. **Ponto de contenção.** Todo write passa por um único documento. Com transações, esse documento vira o gargalo de serialização.
2. **Buracos.** Se o `$inc` sucede e o insert do documento falha, a revisão foi consumida e some. É o que o gap-fill do kine trata (`Fill`/`IsFill`) — mas aqui o padrão de falha é diferente.
3. **Visibilidade fora de ordem.** Duas escritas concorrentes podem obter as revisões 5 e 6 e o documento 6 ficar visível antes do 5. O `sqllog` já lidava com isso (`sql.go:530`), mas a lógica passa a ser nossa.

> ✅ **Resolvido pelo MSPIKE-4 — e a resposta foi abandonar o contador.**
>
> As três variantes com contador foram medidas sob concorrência 16. A correta (transação com retry) entrega **8,2 escritas/s** com p50 de 870 ms — abaixo do que um cluster de 20 nós precisa. E o problema decisivo é outro: com contador, **o Change Stream entrega as revisões fora de ordem** (13 de 29 eventos), porque a ordem do oplog é a de commit, não a de aquisição do contador.
>
> **A decisão é usar o `clusterTime` do MongoDB como revisão** ([ADR-0001](docs/adr/0001-revision-from-clustertime.md)). Medido: conhecida na escrita via `session.operation_time`, única sob concorrência (40/40 distintas), e o Change Stream entrega **em ordem dela** — zero eventos fora de ordem. A escrita e o evento carregam o mesmo `clusterTime`.
>
> O contador inventa uma ordem que compete com a do banco; o `clusterTime` **é** a ordem do banco. Isso elimina de uma vez o contador, a contenção, as transações no caminho quente, o buffer de reordenação e o gap-fill.

---

## 5. Restrições do M0 (free tier)

| Recurso | Limite | Impacto no kine |
|---|---|---|
| **Storage** | **512 MB** | Teto real de tamanho de cluster |
| **Throughput** | **100 ops/s** | Teto de carga — mas **throttle, não fatura** |
| Documento (medido) | **16 MB** | vs 2 MiB por valor no D1 — deixa de ser risco |
| Transferência | 10 GB in / 10 GB out por 7 dias | ~1,4 GB/dia; `LIST` grande pode apertar |
| Conexões | 500 | Folgado |
| Coleções | 500 | Usamos 2-3 |
| Pausa automática | após 30 dias sem conexão | Irrelevante num cluster ativo |
| Backup automático | ❌ não tem | `mongodump` manual |
| `$where`, map-reduce | ❌ bloqueados | Não usamos |
| Agregação | máx. 50 estágios, `allowDiskUse` ignorado | Atenção no `LIST` |

**Change Streams e transações multi-documento não constam como indisponíveis no M0** — ele é um replica set de 3 nós, então tecnicamente deveriam funcionar. Mas a avaliação do D1 mostrou que **a documentação erra**, e os dois são pilares deste desenho. Por isso são o primeiro spike, e são eliminatórios.

### O teto de 100 ops/s, traduzido

Reaproveitando o modelo de fontes de write do [SPIKE-9 do D1](docs/d1-descartado/spikes/results/spike-9.md):

- Leader election: **2,0 writes/s fixos**
- Lease de kubelet: **0,103 writes/s por nó**

Se cada mutação custar ~2 ops (`findAndModify` + insert):

| Nós | writes/s | ops/s estimado | Cabe em 100? |
|---|---|---|---|
| 10 | 3,0 | ~6 | ✅ folgado |
| 50 | 7,2 | ~14 | ✅ |
| 100 | 12,3 | ~25 | ✅ |

**O throughput não parece ser o limite — o storage de 512 MB é.** E o mais importante: se estourar, o Atlas **desacelera**; não cobra.

---

## 6. Riscos

### 6.0 ✅ ~~Teto de 100 ops/s~~ — MEDIDO (MSPIKE-6)

A premissa que justificou trocar o D1 pelo Mongo: excesso de carga vira lentidão, não fatura. **Confirmada** — zero erros até 6× o teto ([`spikes/results/mspike-6.md`](spikes/results/mspike-6.md)).

Mas o preço da lentidão é alto e não aparece como erro: o p99 de escrita sai de 810 ms (metade do teto) para **63 s** (6× o teto), e o change stream fica **44 s atrasado**. Com `RenewDeadline` de 10 s na leader election, isso derruba scheduler e controller-manager sem gerar uma linha de erro.

Teto real: **~92 ops/s de escrita** ≈ **30 mutações/s** de cluster com latência sadia. Contra 3,3 (ocioso) e 10 (operação) do modelo, é folga de 3× a 9×.

Comparado ao D1: lá o excesso quebrava o orçamento, aqui quebra o cluster. A troca continua valendo — falha visível é melhor que fatura silenciosa — mas "melhor" não é "indolor".

### 6.1 🔴 Change Streams ou transações indisponíveis no M0

Os dois pilares do desenho. Se qualquer um faltar no free tier, o projeto muda de forma: sem Change Streams volta-se ao polling (perdendo a principal vantagem); sem transações é preciso um padrão CAS como o do D1. **Eliminatório para o alvo M0 — não para o projeto**, que cairia para o Flex (teto de US$ 30/mês).

### 6.2 🔴 Reimplementar `server.Backend` do zero

Não há atalho: revisões, watch, compactação, gap-fill e lease. O `pkg/drivers/nats/` é o mapa mais próximo — vale lê-lo inteiro antes de escrever a primeira linha. O risco é subestimar a semântica sutil que o `sqllog` acumulou ao longo de anos de bugs corrigidos.

### 6.3 ✅ ~~Sequência de revisões~~ — RESOLVIDO pelo MSPIKE-4

Contenção, buracos e ordem fora de sequência eram todos consequência do contador. Com `clusterTime` ([ADR-0001](docs/adr/0001-revision-from-clustertime.md)) os três desaparecem.

Sobra uma consequência a validar: **a revisão deixa de ser densa** — salta em vez de incrementar de 1. O apiserver trata `resourceVersion` como valor opaco monotônico, mas isso precisa ser confirmado com um k3s real (`MT-3`). Afeta também o `compactMinRetain`, que conta revisões e passará a ser uma janela de tempo.

### 6.4 🟠 512 MB de storage — medido

Um documento de 6 KB ocupa **13.694 bytes** com índices, então cabem **~39.200 revisões vivas** em 512 MB ([MSPIKE-9/5](spikes/results/mspike-5-9.md)). Para um cluster de 500 pods compactando a cada 5 min a 10 writes/s, estado corrente mais histórico ficam na casa de 3.500 documentos — dentro do teto com folga.

**Mas o M0 não expande: ao encher, para.** Se a compactação atrasar, o cluster para junto. Isso faz do alerta de storage (`MOPS-1`) um requisito de lançamento — a mesma conclusão a que o D1 chegou sobre o alerta de custo, por outro caminho.

### 6.4-b 🟠 O `LIST` é limitado por banda

Medido: `LIST` de 300 chaves leva **823 ms com `value` e 54,6 ms sem** — 15× de diferença. Trocar o `$group` por uma coleção materializada não resolveu (1403 ms): o gargalo nunca foi o plano de execução, é o payload atravessando a rede, a ~5 MB/s.

Isso explica retroativamente o D1, que media 1039 ms para as mesmas 300 chaves — **é característica de qualquer datastore remoto**, não do MongoDB.

Consequência de projeto: **`keysOnly` deixa de ser otimização e vira caminho principal.** É o parâmetro que separa um LIST de 55 ms de um de 823 ms.

### 6.5 ✅ ~~Consistência~~ — RESOLVIDO de graça (MSPIKE-1)

`w=majority` custa **26,6 ms** contra 27,2 ms do write default — latência indistinguível. `readConcern: majority` custa 21,6 ms. O kine pode usar majority no caminho de escrita sem penalidade alguma.

### 6.6 🟡 Compactação e o oplog

O oplog do M0 é pequeno e não configurável. Um Change Stream que fique para trás além da janela do oplog é invalidado e **perde eventos** — para o kine, isso é um watch quebrado. Precisa de estratégia de resume token e de recuperação.

---

## 7. Plano

### Fase 0 — Spikes (bloqueante)

Mesma disciplina da avaliação do D1: medir contra um Atlas real antes de escrever código de produção. Foi ela que pegou o erro de 70× no custo e uma mitigação silenciosamente errada.

### Fase 1 — Fundações do backend
Sequência de revisões, mapeamento de documento, conexão e índices.

### Fase 2 — `server.Backend`
CRUD, List/Count, compactação.

### Fase 3 — Watch via Change Streams
O diferencial do projeto.

### Fase 4 — Conformidade e k3s real

### Fase 5 — Produção

---

## 8. Backlog

#### Épico A — Spikes (bloqueante)

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| MSPIKE-1 | Provisionar cluster M0 em São Paulo + usuário com escopo mínimo | P | Conexão estabelecida, `ping` medido, credenciais fora do repo |
| MSPIKE-2 | **Change Streams funcionam no M0?** | M | Stream aberto recebe eventos de insert/update/delete; latência do evento medida. **Eliminatório** |
| MSPIKE-3 | **Transações multi-documento funcionam no M0?** | M | Transação com 2 coleções commita e faz rollback. **Eliminatório** |
| MSPIKE-4 | Sequência de revisões: `findAndModify` vs `clusterTime` | G | Decisão em ADR, com medição de contenção sob 50 writes concorrentes |
| MSPIKE-5 | Latência p50/p95/p99 de insert/find/transação a partir do Brasil | M | Tabela comparável à do D1 (que mediu 232 ms) |
| MSPIKE-6 | Teto real de 100 ops/s: o que acontece ao estourar? | M | Confirmar que é throttle e não erro; medir o comportamento sob excesso |
| MSPIKE-7 | Tamanho em disco de um cluster k3s simulado | M | Projeção de quantos nós/pods cabem em 512 MB |
| MSPIKE-8 | Janela do oplog e recuperação de Change Stream invalidado | M | Medir a janela; provar que o resume token recupera sem perder evento |
| MSPIKE-9 | Modelo de documento e índices para as queries do kine | M | `LIST` por prefixo e `After` por revisão com índice, sem COLLSCAN |

#### Épico B — Fundações

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| MFND-1 | `pkg/drivers/mongo` com `init()`, parser de DSN e registro do scheme | M | `kine --endpoint "mongodb+srv://..."` sobe |
| MFND-2 | Conexão, pool, `writeConcern`/`readConcern` majority, retry | M | Reconecta após queda; read-your-writes verificado |
| MFND-3 | Setup de coleções e índices, idempotente | M | Rodar duas vezes não quebra |
| MFND-4 | Gerador de revisões conforme o MSPIKE-4 | G | 10.000 revisões concorrentes: monotônicas e sem buraco permanente |
| MFND-5 | Mapeamento `server.KeyValue` ⇄ documento BSON | M | Round-trip byte a byte de value binário |

#### Épico C — `server.Backend`

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| MBK-1 | `Create` / `Update` / `Delete` com semântica de revisão do etcd | G | Criar chave existente devolve `ErrKeyExists` |
| MBK-2 | `Get` — corrente e por revisão histórica | M | Get em revisão compactada devolve `ErrCompacted` |
| MBK-3 | `List` / `Count` por prefixo, com limite e revisão | G | Paridade com o driver SQLite nos mesmos casos |
| MBK-4 | `Compact` + `CurrentRevision` + `DbSize` | G | Compactação concorrente de 2 instâncias não corrompe |
| MBK-5 | Lease/TTL via TTL index | M | Chave com lease expira sem varredura da aplicação |
| MBK-6 | `WaitForSyncTo` e a semântica de progresso | M | Watch não perde evento sob carga |

#### Épico D — Watch

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| MW-1 | `Watch` via Change Stream, com resume token persistido | G | Watch sobrevive a reconexão sem perder evento |
| MW-2 | Broadcast para múltiplos watchers (reusar `pkg/broadcaster`) | M | N watchers, um único Change Stream |
| MW-3 | Recuperação de stream invalidado (oplog rollover) | G | Detecta, recupera por leitura direta e retoma |
| MW-4 | Ordenação e gap-fill de revisões fora de ordem | G | Eventos entregues em ordem sequencial estrita |

#### Épico E — Conformidade

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| MT-1 | Suíte do kine contra o driver mongo | G | Mesma cobertura do SQLite, ou skips justificados |
| MT-2 | Conformidade etcd (watch, lease, compact, txn) | G | Sem regressão contra o baseline |
| MT-3 | **k3s real: subir cluster, deploy, rolling update** | G | Cluster `Ready`, `kubectl get`/`logs`/`exec` funcionam |
| MT-4 | Carga: medir ops/s, latência do apiserver e crescimento em disco | M | Projeção de teto em 512 MB |
| MT-5 | Caos: queda de rede, failover do replica set, throttle de 100 ops/s | M | Recupera sem perder revisão nem travar watch |

#### Épico F — Produção

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| MOPS-1 | Métricas Prometheus (ops/s, latência, lag do change stream, disco) | M | Dashboard com alerta de storage |
| MOPS-2 | README + guia de setup | M | Alguém de fora sobe um cluster seguindo só o doc |
| MOPS-3 | ADRs das decisões | P | Um arquivo por decisão em `docs/adr/` |
| MOPS-4 | Backup/restore via `mongodump` | M | Runbook testado ponta a ponta |
| MOPS-5 | CI (lint, testes, build multi-arch) | M | Pipeline verde no PR |
| MOPS-6 | Documentar limites e quando **não** usar | P | Seção honesta de "não use isto se..." |

**Estimativa:** P = até 1 dia · M = 2-3 dias · G = 1 semana ou mais.

---

## 9. Próximo passo

**MSPIKE-1 a MSPIKE-3.** Os dois últimos são eliminatórios: sem Change Streams e sem transações no M0, o desenho muda de forma. Nada de código de produção antes de os três fecharem.

---

## 10. Fontes

- [k3s-io/kine](https://github.com/k3s-io/kine)
- [Atlas — limitações do free tier (M0)](https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/)
- [MongoDB — Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Atlas — pricing](https://www.mongodb.com/pricing)
- [Avaliação do Cloudflare D1](docs/d1-descartado/README.md) — descartado por custo
