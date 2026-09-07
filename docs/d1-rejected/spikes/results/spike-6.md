# SPIKE-6 — Batch atômico e o padrão CAS da compactação

**Status:** ✅ concluído · **Data:** 2026-09-06 · **Decisão:** [ADR-0002](../../adr-0002-deferred-transaction.md)

## A pergunta

O D1 rejeita `BEGIN TRANSACTION`/`SAVEPOINT`. Só existe `batch`: atômico, mas **não interativo**. O compact do kine (`pkg/logstructured/sqllog/sql.go:234`) precisa de:

```
ler revisão atual → ler compact_rev → COMPARAR com o esperado
→ apagar linhas → gravar novo compact_rev     (tudo atômico)
```

A comparação é o nó. Um `UPDATE ... WHERE cond` que não casa afeta 0 linhas e **não gera erro** — o batch seguiria em frente e apagaria dados com base numa premissa falsa.

## Resultados

### 1. O batch é atômico — confirmado

Batch de 3 inserts com o do meio violando a PK:

```
batch success=False   UNIQUE constraint failed: kine_sim.id
linhas antes=5  depois=5
linhas do batch que sobraram: 0
```

Nada vazou. Atomicidade real, não "melhor esforço".

### 2. Mecanismos de guarda

| Mecanismo | CAS correto | CAS errado | Serve? |
|---|---|---|---|
| `UPDATE ... WHERE` (ingênuo) | passa | **passa** | ✗ |
| `INSERT` + `CHECK` constraint | passa | **ABORTA** | ✓ |
| `abs(-9223372036854775808)` | passa | **ABORTA** | ✓ |

O ingênuo falha exatamente como previsto: um CAS errado **não** aborta nada. Se o desenho tivesse ido para código sem este spike, a compactação apagaria dados sob premissa falsa e ninguém veria o erro.

A guarda escolhida:

```sql
INSERT INTO cas_guard(ok)
SELECT 0 WHERE (SELECT prev_revision FROM kine WHERE name='compact_rev_key') != ?
```

Com `CREATE TABLE cas_guard(ok INTEGER PRIMARY KEY CHECK (ok = 1))`. Se o CAS casa, o `SELECT` não devolve linha e nada acontece. Se não casa, tenta inserir `0`, viola o `CHECK` e **aborta o batch inteiro**.

Verificado: `cas_guard` **nunca acumula lixo** — 0 linhas depois de CAS bem-sucedido e depois de CAS falho. Ela existe só como gatilho de erro.

### 3. Ciclo de compact completo

```
estado inicial: 20 linhas, compact_rev=0
a) CAS correto  -> success=True   linhas=11  compact_rev=10
b) CAS errado   -> success=False  linhas=11  compact_rev=10
   erro: CHECK constraint failed: ok = 1: SQLITE_CONSTRAINT
```

O CAS falho não apagou nada e não moveu a revisão. **É exatamente o critério de aceite do spike.**

### 4. Concorrência

Duas instâncias leem `compact_rev=0` e disparam compactações simultâneas para alvos diferentes:

```
alvo=15: ✗ abortou (CHECK constraint failed)
alvo=25: ✓ venceu
vencedores: 1   compact_rev final: 25
```

Exatamente uma vence. É o critério de aceite do `KINE-5`.

### 5. O batch devolve `meta` por statement

```
[0] guarda   changes=0  last_row_id=21  rows_read= 1
[1] DELETE   changes=9  last_row_id=21  rows_written=9
[2] UPDATE   changes=1  last_row_id=21  rows_written=1
[3] INSERT   changes=1  last_row_id=22  rows_written=2
```

`changes` e `last_row_id` corretos e individualizados. Isso resolve o `RowsAffected` e o `LastInsertId` dentro de um batch.

## Veredito

> **A transação diferida com CAS é viável.** Batch atômico ✓ · guarda que aborta ✓ · compact protegido ✓ · seguro sob concorrência ✓

## Achado que simplifica o KINE-5

`Tx.Compact()` retorna `RowsAffected`, que numa transação diferida só existe depois do commit. Parecia um problema de desenho. Não é: o valor é usado **num único lugar** (`sql.go:281`), num `logrus.Infof` que roda **depois** do `t.MustCommit()`. Nenhuma lógica depende dele.

Ou seja, `Tx.Compact()` pode retornar 0 e deixar o driver logar o número real no commit. Impacto puramente cosmético.

## Reproduzir

```bash
./spikes/spike-6-batch-cas.py
```
