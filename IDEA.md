# kine-d1 — backend Cloudflare D1 para o kine

> **Status:** pesquisa / anteprojeto
> **Data:** 2026-09-06
> **Upstream analisado:** [k3s-io/kine](https://github.com/k3s-io/kine) commit `6fb95f5` (2026-08-12, "Bump to go1.26 / etcd 3.7 / kubernetes 1.37"), último release `v0.17.0`
> **Repositório:** fork de `k3s-io/kine` em [Vinny1892/kine-d1](https://github.com/Vinny1892/kine-d1) — `origin` = fork, `upstream` = k3s-io/kine

---

## 1. Objetivo

Implementar um novo *driver* (backend) para o kine que use o **Cloudflare D1** como armazenamento, permitindo rodar um cluster k3s cujo "etcd" é um banco SQLite serverless na borda da Cloudflare.

O kine já faz exatamente essa tradução para outros bancos (SQLite, PostgreSQL, MySQL, NATS, t4). A pergunta deste documento é: **o D1 consegue cumprir o contrato que o kine exige?** A resposta curta é *sim, com ressalvas sérias* — e as ressalvas estão todas na seção 5.

---

## 2. Como o kine funciona hoje (o que precisamos implementar)

O kine expõe uma API gRPC compatível com etcd v3 e traduz as operações para um log de revisões em SQL. O fluxo é:

```
kube-apiserver
      │ gRPC (protocolo etcd v3)
      ▼
pkg/server          ← implementa KV, Watch, Lease, Maintenance do etcd
      │
      ▼
pkg/logstructured   ← traduz KV do etcd para um log append-only de revisões
      │
      ▼
pkg/logstructured/sqllog  ← compactação, polling, gap-fill, broadcast de watch
      │
      ▼
server.Dialect      ← interface SQL (pkg/server/types.go:43)
      │
      ▼
pkg/drivers/generic ← SQL genérico compartilhado (SQLite/PG/MySQL)
      │
      ▼
database/sql + driver específico
```

### 2.1 O ponto de extensão

Um driver é uma função registrada num mapa global (`pkg/drivers/registry.go:11`):

```go
type Constructor func(ctx context.Context, wg *sync.WaitGroup, cfg *Config) (leaderElect bool, backend server.Backend, err error)
```

O registro acontece num `init()` associando o *scheme* da DSN (`pkg/drivers/sqlite/sqlite.go:161`), e o pacote é importado por efeito colateral em `pkg/endpoint/init.go`. Ou seja: adicionar `d1://...` custa uma linha em `init.go` e um pacote novo em `pkg/drivers/d1/`.

`leaderElect` deve ser **`true`** para D1 — é o valor usado por todos os backends compartilhados por rede (pgsql, mysql, nats, t4). Isso faz o k3s eleger um líder e evitar que todos os servidores compactem ao mesmo tempo.

### 2.2 A tabela única

Todo o estado do cluster vive numa tabela só (`pkg/drivers/sqlite/sqlite.go:25`):

```sql
CREATE TABLE kine (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,  -- a "revisão" do etcd
    name            TEXT,      -- a chave, ex: /registry/pods/default/nginx
    created         INTEGER,
    deleted         INTEGER,
    create_revision INTEGER,
    prev_revision   INTEGER,
    lease           INTEGER,
    value           BLOB,      -- objeto k8s serializado (protobuf)
    old_value       BLOB       -- cópia do valor anterior, para o PrevKV do watch
);
```

Mais 6 índices, incluindo `UNIQUE (name, prev_revision)` — é essa constraint que produz o `ErrKeyExists` do etcd quando dois clientes tentam criar a mesma chave.

**O `id` é a revisão do etcd.** Ele precisa ser monotônico e sem buracos permanentes; quando um buraco aparece, o kine insere uma linha `gap-N` para tapá-lo (`Fill`/`IsFill`).

### 2.3 O que o `Dialect` exige (`pkg/server/types.go:43`)

18 métodos. Os 15 primeiros são consultas/comandos SQL que o `pkg/drivers/generic` já implementa genericamente — herdamos de graça. Os que exigem atenção:

| Método | Situação no D1 |
|---|---|
| `Insert` | OK via `meta.last_row_id` (ver 4.2) |
| `GetSize` | **Precisa de implementação própria** — o SQLite usa `PRAGMA page_count`, indisponível no D1 |
| `PostCompact` | Não aplicável (era `wal_checkpoint`) — vira no-op |
| `BeginTx` → `server.Transaction` | **Bloqueador principal** — D1 não tem transações interativas (ver 5.1) |

### 2.4 Os dois laços de fundo que dominam o custo

1. **Polling de watch** (`pkg/logstructured/sqllog/sql.go:477`): um `time.NewTicker(time.Second)` (linha 486) que executa `After(rev, limit)` — ou seja, **no mínimo 1 query por segundo, para sempre**, mesmo com o cluster completamente ocioso. Escritas disparam o poll antes do tick via canal `notify`.

2. **Compactação** (`sql.go:234`): abre uma transação `LevelSerializable`, lê a revisão atual, lê a revisão compactada, apaga linhas antigas e grava a nova revisão compactada — tudo atômico.

Esses dois laços são exatamente onde o D1 aperta.

---

## 3. O que é o Cloudflare D1

SQLite serverless da Cloudflare, com replicação de leitura global opcional. Acesso por:

- **Workers binding** (`env.DB.prepare(...)`) — dentro de um Worker.
- **REST API v4** — `POST /accounts/{account_id}/d1/database/{database_id}/query` e `.../raw`, com body `{"sql": "...", "params": [...]}` ou `{"batch": [...]}`.

A resposta traz `results` e um bloco `meta` com `last_row_id`, `changes`, `rows_read`, `rows_written`, `duration`, `size_after`, `served_by_primary`, `served_by_region`. O endpoint `/raw` devolve `{columns: [...], rows: [[...]]}` em vez de array de objetos — mais barato de transferir e de decodificar, e é o que devemos usar.

### 3.1 Limites relevantes ([docs](https://developers.cloudflare.com/d1/platform/limits/))

| Limite | Valor | Impacto no kine |
|---|---|---|
| Tamanho do banco | 10 GB (Paid) / 500 MB (Free) | Teto de tamanho do cluster |
| **Tamanho máximo de uma linha / BLOB / string** | **2 MB** | **Crítico** — `value` + `old_value` na mesma linha |
| Statement SQL | 100 KB | OK se usarmos *bound params* (nunca inline) |
| Parâmetros por query | 100 | OK — o kine usa no máximo 9 |
| Colunas por tabela | 100 | OK — usamos 9 |
| Duração máxima da query | 30 s | Compactação em lote pode encostar |
| Conexões simultâneas por Worker | 6 | Relevante só na arquitetura B |
| Time Travel (PITR) | 30 dias (Paid) / 7 (Free) | **Vantagem** — backup do cluster de graça |
| Bancos por conta | 50.000 (Paid) | Irrelevante |

Cada banco D1 é **single-threaded** e processa queries sequencialmente. Escritas sempre vão ao *primary*.

### 3.2 Preço ([docs](https://developers.cloudflare.com/d1/platform/pricing/))

| Métrica | Workers Paid | Workers Free |
|---|---|---|
| Linhas lidas | 25 bi/mês incluídas, depois $0,001/milhão | 5 mi/dia |
| Linhas escritas | 50 mi/mês incluídas, depois $1,00/milhão | 100 mil/dia |
| Armazenamento | 5 GB incluídos, depois $0,75/GB-mês | 5 GB |

`rows_read` conta **linhas escaneadas**, não retornadas — por isso os índices do kine importam muito.

### 3.3 O que o D1 **não** tem

- `BEGIN TRANSACTION` / `SAVEPOINT` — rejeitado com erro explícito ("please use the state.storage.transaction() API instead"). Só existe `batch`, que é atômico mas **não interativo** (não dá para ler um resultado e decidir o próximo statement dentro da mesma transação).
- `PRAGMA` arbitrário, `VACUUM`, `ATTACH`.
- Conexões persistentes — cada query é um round-trip HTTP.

---

## 4. Arquiteturas possíveis

### Opção A — driver `database/sql` sobre a REST API (recomendada para o MVP)

Escrever um `database/sql/driver` que traduz `QueryContext`/`ExecContext` em chamadas HTTP para `/raw`, e plugá-lo no `generic.OpenDB`, que já aceita um `driver.Connector` (`pkg/drivers/generic/generic.go:157`).

```
kine ──▶ pkg/drivers/d1 ──▶ pkg/drivers/generic ──▶ database/sql ──▶ driver d1 ──HTTPS──▶ api.cloudflare.com/.../d1/.../raw
```

**A favor:** reaproveita ~90% do código (todo o SQL do `generic`, o `sqllog`, métricas, retry). É o mesmo formato dos drivers existentes, então um PR upstream é plausível.
**Contra:** herda o *rate limit* da API v4 (ver 5.2) e uma latência de round-trip por query.

### Opção B — Worker proxy com D1 binding

Um Worker nosso, autenticado por token, expõe um endpoint mínimo; o driver Go fala com ele em vez da API v4.

**A favor:** contorna o rate limit da API v4; permite implementar um endpoint de "transação compilada" (recebe o roteiro do compact inteiro e resolve dentro do Worker com `batch`); permite usar a **Sessions API** com *bookmarks* para consistência sequencial em réplicas de leitura; pode comprimir e paginar do lado do servidor.
**Contra:** mais uma peça para operar e versionar; teto de 1.000 queries D1 por invocação de Worker; não é *upstreamable*.

### Opção C — `server.Backend` direto (sem o `generic`)

Como fazem os drivers `nats` e `t4`. Liberdade total, mas reimplementa revisões, watch, compactação e gap-fill do zero. **Descartada** — custo altíssimo sem ganho claro.

### Recomendação

**Começar pela A**, com o driver isolado atrás de uma interface `d1.Transport`, de modo que a **B** entre depois como uma implementação alternativa de transporte (`d1://...?proxy=https://meu-worker...`) sem reescrever o driver. A decisão A-vs-B depende inteiramente do resultado do spike de rate limit (task `SPIKE-2`).

### 4.1 Forma da DSN

```
d1://<account_id>/<database_id>?token=$CF_API_TOKEN&compress=s2&consistency=session
```

Com suporte a variáveis de ambiente (`CLOUDFLARE_API_TOKEN`) para não colocar segredo em linha de comando — o token dá acesso de escrita ao banco inteiro do cluster.

### 4.2 Decisões técnicas já resolvidas pela leitura do código

- **`LastInsertID = true`.** O `generic` tem dois caminhos de insert (`generic.go:481`): `RETURNING id` ou `LastInsertId()`. O D1 devolve `meta.last_row_id`, então basta o driver expor isso em `sql.Result` e ligar a flag, exatamente como o SQLite faz (`sqlite.go:86`). Evita depender de `RETURNING`.
- **`paramCharacter = "?"`, `numbered = false`** — dialeto SQLite.
- **`CompactSQL`** — o mesmo do SQLite (`sqlite.go:91`) serve, é SQLite puro.
- **`PostCompactSQL = nil`** — sem WAL para checkpoint.
- **`Migrate`** (`generic.go:100`) tenta ler a tabela legada `key_value`; o erro de tabela inexistente é engolido pelo próprio código, então é inofensivo.
- **Setup do schema** — o `setup` do SQLite (`sqlite.go:123`) executa `PRAGMA` e `VACUUM`; a versão D1 deve rodar só os `CREATE TABLE`/`CREATE INDEX`, idealmente num único `batch`.

---

## 5. Riscos e problemas em aberto

Ordenados por gravidade. Cada um vira um *spike* na seção 6.

### 5.1 🔴 Ausência de transações interativas

O `sqllog` abre transações serializáveis em dois lugares (`sql.go:91` e `sql.go:234`) e faz **leituras e escritas intercaladas** dentro delas — o padrão do compact é: ler revisão atual → ler revisão compactada → comparar com o valor esperado → apagar linhas → gravar nova revisão compactada → commit. O D1 não suporta isso.

**Mitigação proposta:** implementar `server.Transaction` como *transação diferida com CAS*:
- Leituras dentro da transação passam direto (autocommit) e retornam na hora.
- Escritas são acumuladas num buffer.
- No `Commit()`, o buffer é enviado como um `batch` atômico, precedido por um statement de *compare-and-swap* que falha se a `compact_rev_key` mudou desde a leitura — algo na linha de `UPDATE kine SET prev_revision = ? WHERE name = 'compact_rev_key' AND prev_revision = ?`, com verificação de `changes = 1` no `meta` da resposta.
- `Rollback()` descarta o buffer.

Isso é **suficiente para o único uso real de transação no kine** (a compactação), mas é uma semântica mais fraca que `Serializable`. Precisa ser validado com o teste de compactação concorrente. Com `leaderElect = true` a janela de concorrência já é pequena, mas não nula (rolling upgrade do plano de controle).

### 5.2 🔴 Rate limit da API v4

A API v4 da Cloudflare tem limite global de **1.200 requisições por 5 minutos por usuário** (≈4 req/s), cumulativo entre dashboard, API key e token. O kine sozinho já faz **1 req/s de polling ocioso**, mais uma por escrita — e um cluster k3s ocioso escreve constantemente (renovação de `Lease` de nó a cada ~10s, `Endpoint`, eventos).

Não está documentado se os endpoints `/query` e `/raw` do D1 caem nesse limite global — o [changelog de 2025-05-30](https://developers.cloudflare.com/changelog/2025-05-30-d1-rest-api-latency/) diz que eles passaram a ser servidos na borda, *bypassando os data centers centrais*, o que sugere um caminho diferente do plano de controle. **Isso precisa ser medido antes de qualquer linha de código de produção** — se o limite se aplicar, a Opção A morre e a B é obrigatória.

### 5.3 🟠 Limite de 2 MB por linha vs. `value` + `old_value`

O k8s limita objetos a ~1,5 MB. A linha do kine guarda `value` **e** `old_value` (o valor anterior, para o `PrevKV` do watch). Dois objetos grandes na mesma linha estouram os 2 MB do D1. Se os `params` da API precisarem de base64 (ver 5.4), some +33%.

**Mitigações:** compressão (o driver `nats` já usa `klauspost/compress/s2` por motivo idêntico — ver `pkg/drivers/nats/codec.go:9`); e/ou mover `old_value` para uma tabela satélite. A compressão é a mitigação de menor atrito e resolve os dois problemas de uma vez (tamanho de linha e custo de armazenamento).

### 5.4 🟠 BLOBs sobre JSON

O corpo da REST API é JSON e o schema documenta `params` como array de strings. Os valores do kine são protobuf binário arbitrário. Não há como passar bytes crus.

**Opções a testar no spike:** (a) coluna `value` como `TEXT` com base64 — simples, +33% de tamanho; (b) manter `BLOB` e usar `unhex(?)` no `INSERT` / `hex(value)` no `SELECT` — +100% no *transporte* mas mantém o BLOB no disco; (c) verificar se a API aceita array de inteiros como os bindings de Worker aceitam `ArrayBuffer`. A escolha muda o schema, então é bloqueante para o resto.

### 5.5 🟠 Latência

Cada query é um round-trip HTTPS até a borda e, para escritas, até o *primary*. Onde o etcd local responde em <10 ms, aqui falamos de dezenas a centenas de ms. O apiserver funciona (k3s com PostgreSQL remoto já vive nesse regime), mas:
- `kubectl` fica visivelmente mais lento;
- o watch tem latência mínima de ~1 s por causa do ticker de polling;
- listagens grandes (`/registry/pods` inteiro no start do apiserver) podem encostar no timeout de 30 s.

Vale medir e considerar tornar o intervalo de polling configurável.

### 5.6 🟡 Réplicas de leitura e consistência

Se ligarmos read replication, uma leitura pode não enxergar a própria escrita — inaceitável para o kine, que depende de ler a revisão que acabou de gravar. A **Sessions API** resolve com *bookmarks* (consistência sequencial), mas precisamos confirmar se ela está disponível pela REST API ou só pelo binding de Worker (a doc é focada em Workers). Se for só binding, é mais um argumento para a Opção B. **Default seguro no MVP: desligar réplicas / forçar `first-primary`.**

### 5.7 🟡 Custo operacional real

Estimativa grosseira para um cluster pequeno e ocioso: o polling ocioso lê poucas linhas por query (as subqueries `MAX(id)` e `MAX(prev_revision)` usam índice), algo como 5 linhas × 86.400 polls/dia ≈ 430 mil linhas lidas/dia — irrisório contra os 25 bilhões incluídos. **O custo não está nas linhas lidas, está no número de requisições e nas escritas.** O plano Free (100 mil escritas/dia) é apertado mas talvez suficiente para um cluster de laboratório; precisa ser medido (task `SPIKE-4`).

### 5.8 🟡 Pool de conexões sem conexões

`generic.OpenDB` configura `MaxIdleConns`/`MaxOpenConns` e chama `db.Ping()` três vezes. No nosso driver "conexão" é um objeto HTTP stateless — `MaxOpenConns` na prática vira o limite de concorrência de requisições. Precisamos escolher defaults sensatos e implementar `Ping` como uma query trivial (`SELECT 1`).

---

## 6. Viabilidade em um cluster médio — as contas

**Definição de "cluster médio" usada aqui:** 10 nós, ~500 pods, ~50 deployments/services, operação normal (deploys diários, sem churn extremo).

### 6.1 Quantas escritas um cluster desses gera?

O que escreve no etcd/kine de forma **constante**, mesmo com o cluster parado:

| Fonte | Frequência | Writes/s (10 nós) |
|---|---|---|
| `Lease` de nó (kubelet renova a cada 10 s) | 1 por nó / 10 s | 1,00 |
| Leader election dos controllers (`RetryPeriod` 2 s) — no k3s são ~4 | 1 por componente / 2 s | 2,00 |
| `NodeStatus` completo (a cada 5 min com NodeLease) | 1 por nó / 300 s | 0,03 |
| Events | variável | ~0,20 |
| Pod status / EndpointSlices em estado estável | ~0 | ~0,10 |
| **Total ocioso** | | **≈ 3,3 writes/s** |

Sob operação normal (deploys, jobs, scaling), a média sobe para **~10 writes/s**, com picos de 30-50/s durante um rollout.

### 6.2 Rows written — o detalhe que quase todo mundo esquece

O kine é um **log append-only**: cada mutação insere uma linha nova. E a compactação depois **apaga** essa linha (`CompactSQL`, `pkg/drivers/sqlite/sqlite.go:91`). No D1, `DELETE` também conta como `rows_written`. Portanto:

> **rows_written ≈ 2 × número de mutações**

| Cenário | Writes/s | Rows written/dia | Rows written/mês | Custo D1 (Paid, 50 mi/mês incluídos) |
|---|---|---|---|---|
| Médio, ocioso | 3,3 | 570 mil | **17,1 mi** | **$0** — cabe no incluído (2,9× de folga) |
| **Médio, operação normal** | **10** | **1,73 mi** | **52,6 mi** | **≈ $2,60/mês** (2,6 mi excedentes) |
| Grande (50 nós / 2.500 pods) | 30 | 5,18 mi | 158 mi | ≈ $108/mês |

### 6.3 Rows read — não é problema

O laço de polling (`pkg/logstructured/sqllog/sql.go:486`) roda 1×/s. A query `After` usa `MAX(id)` sobre o rowid (O(1)), `MAX(prev_revision)` sobre o índice `kine_name_index`, e um range scan em `id > ?`. Ociosa, lê **~5 linhas por poll**:

- 86.400 polls/dia × 5 = **432 mil rows read/dia** ≈ 13 mi/mês
- Franquia incluída: **25 bilhões/mês** → estamos usando **0,05%**

Mesmo somando os `LIST` do apiserver (~1.500 linhas para listar 500 pods, e raros porque o watch cache absorve), rows read fica ordens de grandeza abaixo do incluído. **Custo de leitura ≈ $0.**

### 6.4 Armazenamento — folgado

500 pods × ~8 KB + ~2.000 outros objetos × ~2 KB ≈ **8 MB** de estado corrente. Com histórico entre compactações e a coluna `old_value` duplicando valores, algo como **30-50 MB**. Teto do D1: **10 GB**. Folga de ~200×.

Ressalva: o D1 não permite `VACUUM`. O SQLite reusa páginas livres depois dos `DELETE`, então o banco não cresce indefinidamente — mas o arquivo **nunca encolhe**. Um pico pontual (um Job criando 100 mil objetos) deixa o banco permanentemente grande. Com 10 GB de teto e 50 MB de uso normal isso é acadêmico, mas precisa entrar no monitoramento.

### 6.5 O gargalo real nº 1 — requisições HTTP

| Fonte | req/s |
|---|---|
| Polling do watch | 1,0 |
| Escritas | 3,3 (ocioso) a 10 (normal) |
| Leituras do apiserver | 1-5 |
| **Total** | **≈ 6-16 req/s**, picos de 30-60 |

E aqui está a condição eliminatória: a API v4 da Cloudflare limita a **1.200 requisições por 5 minutos = 4 req/s**. Se esse limite valer para `/query` e `/raw`, **o D1 via REST direta não sustenta nem um cluster de 3 nós** — vai ficar preso em 429 permanente.

Via **Worker proxy** (Opção B) o problema some: 16 req/s = 41 mi requisições/mês × $0,30/mi ≈ **$12/mês**, sem teto rígido.

**Por isso o `SPIKE-2` é o primeiro item do backlog.** Ele não decide se o projeto é viável — decide se a arquitetura é A ou B.

### 6.6 O gargalo real nº 2 — latência (este não tem contorno)

| Backend | Latência de escrita p50 |
|---|---|
| SQLite local | < 1 ms |
| etcd local | 1-10 ms |
| PostgreSQL na mesma região | 5-20 ms |
| **D1 via HTTPS (Brasil → primary nos EUA)** | **~150-350 ms** |

Toda escrita vai ao *primary*, que é single-threaded. Consequências, em ordem de gravidade:

1. **Flapping de leader election** — este é o modo de falha mais provável. Os controllers usam `RetryPeriod` de 2 s e `RenewDeadline` de 10 s. Com 300 ms está tudo bem; mas um pico de latência, um 429 ou uma degradação de rota faz o controller-manager e o scheduler **perderem a liderança e reiniciarem**. Precisa de tuning dos parâmetros de leader election e de alerta dedicado.
2. **Watch com ~1 s de atraso** pelo ticker de polling — rollouts e readiness ficam visivelmente mais lentos.
3. **`kubectl` lento** — cada `apply` paga ~300 ms extras; um `get pods` pode levar 0,5-1 s.
4. **Throughput não é o problema.** Se o D1 sustenta ~50-200 writes/s por banco, os 3-10 writes/s do cluster médio cabem com folga de uma ordem de grandeza.

### 6.7 Veredito

> **Sim, um cluster médio (10 nós / 500 pods) roda em D1 — e por menos de US$ 5/mês.** O limite não é capacidade nem custo; é latência e o rate limit da API.

Traduzindo:

| | |
|---|---|
| ✅ **Capacidade** | Cabe com folga: 3% do armazenamento, 0,05% das leituras, ~100% das escritas incluídas |
| ✅ **Custo** | ~$0-3/mês no plano Paid. Mais barato que qualquer Postgres gerenciado |
| ⚠️ **Latência** | 150-350 ms por escrita. Cluster **funcional, porém lento**, com risco de flapping de leader election |
| 🔴 **Rate limit** | Eliminatório se a API v4 se aplicar. Contornável com Worker proxy (Opção B) |
| 🔴 **Transações** | Precisa da transação diferida com CAS (seção 5.1) — solucionável, mas é o maior trabalho de engenharia |

**Onde faz sentido:** clusters de borda, homelab, dev/staging, control planes de baixa mutação, cenários onde "não administrar banco nenhum" e o Time Travel de 30 dias valem mais que os milissegundos.

**Onde não faz sentido:** produção crítica, clusters com muito churn (CI rodando milhares de Jobs), qualquer coisa com SLA agressivo de latência do apiserver. Para isso, etcd ou Postgres na mesma região.

**Limite prático estimado:** até ~20-30 nós e ~15 writes/s sustentados. Acima disso o custo de escrita cresce linear e a latência começa a machucar o plano de controle.

> ⚠️ Todos os números desta seção são **estimativas derivadas do código e da documentação**, não medições. Os spikes `SPIKE-2` (rate limit), `SPIKE-4` (latência) e a task `TEST-4` (carga) existem justamente para confirmá-los ou derrubá-los.

---

## 7. Plano de execução

### Fase 0 — Spikes (nada é decidido antes disso)

Experimentos pequenos e descartáveis contra um banco D1 real. **São bloqueantes**: os resultados definem schema, arquitetura e viabilidade.

### Fase 1 — Driver `database/sql` para D1

Pacote isolado e testável fora do kine.

### Fase 2 — Driver kine `d1://`

Integração com `pkg/drivers/generic`, schema, transação diferida.

### Fase 3 — Conformidade

Bateria de testes do kine + validação com um k3s real.

### Fase 4 — Produção

Observabilidade, documentação, custo, backup/restore via Time Travel.

### Fase 5 — Opcional

Worker proxy (Opção B), Sessions API, upstream.

---

## 8. Backlog para o GitHub Projects

Formato: `[ÉPICO] ID — título` · *estimativa* · **critério de aceite**.

#### Épico A — Spikes (bloqueante)

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| SPIKE-1 | Provisionar banco D1 de testes + token com escopo mínimo | P | Banco criado, token guardado fora do repo, `curl` no `/raw` retornando 200 documentado no README |
| SPIKE-2 | **Medir o rate limit real de `/query` e `/raw`** | M | Relatório com req/s sustentável, comportamento no 429 e resposta definitiva sobre o limite de 1.200/5min. **Decide Opção A vs B** |
| SPIKE-3 | Definir a representação de BLOB (base64 vs `unhex()` vs array de ints) | M | Benchmark das 3 opções com payloads de 1 KB / 100 KB / 1,4 MB; decisão registrada em ADR |
| SPIKE-4 | Medir latência p50/p95/p99 de `INSERT` e `SELECT` a partir do Brasil | M | Tabela de latências por tipo de query; comparação com SQLite local |
| SPIKE-5 | Validar `meta.last_row_id`, `changes` e `RETURNING` no D1 | P | Confirmação de que `LastInsertID = true` funciona; comportamento do `changes` em `UPDATE ... WHERE` sem match |
| SPIKE-6 | Validar `batch` atômico e o padrão CAS para a compactação | M | Prova de que um `batch` com CAS falho não aplica os demais statements |
| SPIKE-7 | Verificar disponibilidade da Sessions API / bookmarks pela REST API | P | Sim/não documentado; se não, registrar como requisito da Opção B |
| SPIKE-8 | Criar a tabela `kine` + 6 índices no D1 e validar `UNIQUE (name, prev_revision)` | P | Schema aplicado; violação de unicidade produz erro identificável e mapeável para `server.ErrKeyExists` |

#### Épico B — Driver `database/sql`

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| DRV-1 | Esqueleto do pacote + interface `Transport` (REST hoje, Worker depois) | M | `driver.Driver`, `driver.Connector`, `driver.Conn` compilando |
| DRV-2 | Cliente HTTP: auth, timeouts, retry com backoff, tratamento de 429/5xx | M | Retries com jitter; 429 respeita `Retry-After`; contexto cancela a requisição em voo |
| DRV-3 | `QueryContext` sobre `/raw` + `driver.Rows` com tipagem de colunas | G | `SELECT` devolve tipos corretos para INTEGER/TEXT/BLOB/NULL |
| DRV-4 | `ExecContext` + `driver.Result` (`LastInsertId`, `RowsAffected` a partir do `meta`) | M | Valores batem com os do SQLite local nos mesmos statements |
| DRV-5 | Codec de BLOB conforme decisão do SPIKE-3 | M | Round-trip byte a byte de payload binário de 1,4 MB |
| DRV-6 | Compressão opcional de `value`/`old_value` (s2), com header de versão | M | Linha < 2 MB para objeto k8s de 1,5 MB; leitura de linha não comprimida continua funcionando |
| DRV-7 | `Ping` e mapeamento de erros D1 → erros Go tipados | P | `Ping` falha rápido com token inválido, com mensagem clara |
| DRV-8 | Testes unitários com servidor HTTP falso + testes de integração opt-in | G | `go test ./...` verde sem credenciais; `-tags=integration` roda contra D1 real |

#### Épico C — Driver kine `d1://`

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| KINE-1 | `pkg/drivers/d1` com `init()`, parser de DSN e registro do scheme | M | `kine --endpoint "d1://acct/db?token=..."` sobe |
| KINE-2 | Setup do schema (CREATE TABLE + índices) idempotente via `batch` | M | Rodar duas vezes não quebra; sem `PRAGMA`/`VACUUM` |
| KINE-3 | Ligar `generic.OpenDB`, `LastInsertID`, `CompactSQL`, `PostCompact` no-op | M | `Dialect` satisfeito; smoke test de put/get/delete passa |
| KINE-4 | `GetSize` próprio (via `meta.size_after` ou REST `GET /d1/database/{id}`) | P | `etcdctl endpoint status` reporta tamanho plausível |
| KINE-5 | **Transação diferida com CAS** implementando `server.Transaction` | G | Compactação concorrente de 2 instâncias não corrompe `compact_rev_key` |
| KINE-6 | Mapear erro de unicidade → `server.ErrKeyExists` e `ErrCode` para métricas | M | Criar chave duplicada devolve o erro etcd correto |
| KINE-7 | Defaults do pool de conexões e `FillRetryDuration` calibrados para HTTP | P | Sem thrashing de gap-fill sob carga |
| KINE-8 | Intervalo de polling configurável por DSN | P | `?poll-interval=2s` reduz o número de requisições proporcionalmente |

#### Épico D — Testes e conformidade

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| TEST-1 | Rodar a suíte existente do kine contra o driver D1 | G | Mesma cobertura do driver SQLite, ou lista explícita de skips justificados |
| TEST-2 | Teste de conformidade etcd (watch, lease, compact, txn) | G | Nenhuma regressão contra o baseline do SQLite |
| TEST-3 | k3s real apontando para kine+D1: subir cluster, deploy, rolling update | G | Cluster `Ready`, deployment escala, `kubectl get`/`logs`/`exec` funcionam |
| TEST-4 | Teste de carga: N nós / M pods, medir latência do apiserver e custo em linhas | M | Relatório com req/s, p99 e projeção de custo mensal |
| TEST-5 | Teste de caos: perda de rede, 429, 5xx da Cloudflare | M | kine se recupera sem perder revisões nem travar watches |
| TEST-6 | Validar restore via Time Travel do D1 | M | Runbook de restauração testado ponta a ponta |

#### Épico E — Produção

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| OPS-1 | Métricas Prometheus específicas do D1 (rows_read/written, latência, 429) | M | Dashboard Grafana com custo estimado em tempo real |
| OPS-2 | README + guia de setup (criar banco, token, DSN, limites) | M | Alguém de fora sobe um cluster seguindo só o doc |
| OPS-3 | ADRs das decisões (arquitetura, BLOB, transação) | P | Um arquivo por decisão em `docs/adr/` |
| OPS-4 | Modelo de custo documentado com os números do TEST-4 | P | Planilha/tabela de custo por porte de cluster |
| OPS-5 | CI (lint, testes unitários, build multi-arch) | M | Pipeline verde no PR |
| OPS-6 | Documentar limites e quando **não** usar D1 | P | Seção honesta de "não use isto se..." |

#### Épico F — Opcional / futuro

| ID | Título | Est. | Critério de aceite |
|---|---|---|---|
| NEXT-1 | Worker proxy com D1 binding (Opção B) | G | Mesmo `Transport`, sem rate limit da API v4 |
| NEXT-2 | Endpoint de transação compilada no Worker | M | Compactação numa única chamada |
| NEXT-3 | Sessions API + bookmarks para réplicas de leitura | G | Leitura de réplica sem violar read-your-writes |
| NEXT-4 | Proposta de upstream para k3s-io/kine | M | PR aberto com testes e documentação |

**Legenda de estimativa:** P = até 1 dia · M = 2-3 dias · G = 1 semana ou mais.

---

## 9. Próximo passo

Executar o **Épico A** inteiro antes de escrever qualquer código de produção. Em particular, `SPIKE-2` (rate limit) e `SPIKE-3` (BLOB) são os que podem inverter decisões estruturais.

---

## 10. Fontes

- [k3s-io/kine](https://github.com/k3s-io/kine) — fork em [Vinny1892/kine-d1](https://github.com/Vinny1892/kine-d1)
- [D1 — Limits](https://developers.cloudflare.com/d1/platform/limits/)
- [D1 — Pricing](https://developers.cloudflare.com/d1/platform/pricing/)
- [D1 — Global read replication](https://developers.cloudflare.com/d1/best-practices/read-replication/)
- [D1 — Worker API (`batch`)](https://developers.cloudflare.com/d1/worker-api/d1-database/)
- [Cloudflare API — D1 query](https://developers.cloudflare.com/api/resources/d1/subresources/database/methods/query/)
- [Cloudflare Fundamentals — API limits](https://developers.cloudflare.com/fundamentals/api/reference/limits/)
- [Changelog — D1 REST API latency (2025-05-30)](https://developers.cloudflare.com/changelog/2025-05-30-d1-rest-api-latency/)
- [Blog — Sequential consistency without borders](https://blog.cloudflare.com/d1-read-replication-beta/)
- Drivers Go existentes para D1 (todos pré-release, avaliados e descartados como base): [peterheb/cfd1](https://github.com/peterheb/cfd1), [SyneHQ/d1_go_sql](https://github.com/SyneHQ/d1_go_sql)
