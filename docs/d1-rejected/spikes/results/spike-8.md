# SPIKE-8 — O schema real do kine no D1

**Status:** ✅ concluído · **Data:** 2026-09-06

Aplica o schema de `pkg/drivers/sqlite/sqlite.go:25` (mais a `cas_guard` do [ADR-0002](../../adr-0002-deferred-transaction.md)) e roda as queries reais do `generic` com `hex()`/`unhex()`.

## Resultados

**Schema aplica num único `batch` e é idempotente.** 8 statements, 324 ms na primeira aplicação e 300 ms na segunda, sem erro. Os 6 índices são criados normalmente — nada do schema do kine precisou ser adaptado, além de remover `PRAGMA` e `VACUUM`.

**O `INSERT` do `generic` funciona com `unhex()`**, devolvendo `last_row_id` correto, e o `value` faz round-trip byte a byte.

**A constraint de unicidade funciona e é identificável:**

```
UNIQUE constraint failed: kine.name, kine.prev_revision:
SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_UNIQUE)
```

Marcadores utilizáveis para mapear em `server.ErrKeyExists` (`KINE-6`): `UNIQUE constraint failed`, `kine_name_prev_revision_uindex`, `SQLITE_CONSTRAINT`. Preferir o nome do índice, que é o mais específico.

**As queries reais do `generic` funcionam com `hex()`** — `ListCurrent` e `After` retornaram as linhas certas com as colunas na ordem esperada.

**O poll ocioso lê 1 linha** e roda em 0,29 ms no servidor. Projeção: 86.400 linhas lidas/dia só de polling — nada contra a franquia.

## Achados que viram requisito de implementação

1. **`hex(NULL)` devolve `''`**, indistinguível de BLOB vazio. Detalhado no [SPIKE-5](spike-5.md); exige `CASE WHEN col IS NULL THEN NULL ELSE hex(col) END`.
2. **`hex(value)` sem alias renomeia a coluna** para `hex(value)`. O kine usa `Scan` posicional, então não quebra — mas usar `AS value` mantém paridade com o upstream.
3. **Cada INSERT custa 8 `rows_written`** por causa dos 6 índices. É a origem da correção do modelo de custo no [SPIKE-4](spike-4.md).

## Reproduzir

```bash
./spikes/spike-8-schema.py
```
