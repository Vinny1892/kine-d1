# SPIKE-5 — `meta.last_row_id`, `changes` e `RETURNING`

**Status:** ✅ concluído · **Data:** 2026-09-06

## Resultados

**`meta.last_row_id` funciona e é sequencial.** Três inserts devolveram 1, 2, 3, com `changes=1` cada. Isso confirma que o driver pode usar `generic.LastInsertID = true` (`pkg/drivers/generic/generic.go:481`), exatamente como o driver SQLite do upstream — sem depender de `RETURNING`.

**`RETURNING` também é suportado.** `INSERT ... RETURNING id` devolveu `[[4]]` com a coluna nomeada. Os dois caminhos do `generic` funcionam; ficamos no `LastInsertID` por ser o mais simples.

**`changes = 0` não é erro.** Confirmado nos dois casos:

| Operação | success | changes |
|---|---|---|
| `UPDATE` que casa | true | 1 |
| `UPDATE` que não casa | **true** | 0 |
| `DELETE` que não casa | **true** | 0 |

É a evidência independente do que motivou o [ADR-0002](../../docs/adr/0002-transacao-diferida.md): um CAS baseado em `WHERE` não aborta nada, por isso a guarda precisa gerar erro de verdade.

## Achado: `hex(NULL)` é indistinguível de `hex(x'')`

| name | `hex(value)` | `typeof(value)` | `CASE WHEN value IS NULL THEN NULL ELSE hex(value) END` |
|---|---|---|---|
| nulo | `''` | `null` | `None` |
| vazio | `''` | `blob` | `''` |

Os dois devolvem string vazia. O kine grava `NULL` em `value` no `compact_rev_key` e nos registros de gap-fill, então **o driver precisa preservar essa distinção** — senão um `NULL` volta como `[]byte{}`.

Solução adotada: envolver as colunas BLOB em `CASE WHEN col IS NULL THEN NULL ELSE hex(col) END` em vez de `hex(col)` puro. Vale para `DRV-3` e `KINE-3`.

Detalhe menor: `hex(value)` sem alias produz uma coluna chamada `hex(value)`. O kine usa `Scan` posicional, então não quebra, mas o alias `AS value` mantém o SQL legível e a paridade com o upstream.
