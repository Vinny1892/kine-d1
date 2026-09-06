# MSPIKE-2 e MSPIKE-3 — Os eliminatórios do alvo M0

**Status:** ✅ ambos passaram · **Data:** 2026-09-06

A documentação não afirma que Change Streams e transações faltam no M0, mas a avaliação do D1 pegou a doc da Cloudflare errada sobre limites de tamanho — então medimos.

## MSPIKE-2 — Change Streams funcionam ✓

- Stream abre, com resume token disponível de imediato.
- `insert` / `update` / `delete` chegam **na ordem**.
- Latência do evento: **28-34 ms** (o primeiro, 190 ms, inclui o setup do stream).
- O evento traz `clusterTime`, `fullDocument`, `documentKey` e o resume token.

**Resume token sobrevive a reconexão.** Fechando o stream, escrevendo dois documentos e reabrindo com `resume_after`, os dois eventos da janela de queda foram recuperados: nenhum evento perdido.

Isso elimina o laço de polling de 1 s (`sqllog/sql.go:486`) e, com ele, as 86.400 queries/dia que o cluster fazia parado.

## MSPIKE-3 — Transações funcionam ✓ (mas não servem para o caminho quente)

Commit e rollback funcionam, inclusive revertendo o `$inc` do contador. Mas a medição de concorrência revelou o problema que motivou o MSPIKE-4:

```
6 transações concorrentes: 2 commitaram, 4 falharam (WriteConflict)
latência da transação: p50 76,3 ms  vs  26,6 ms de um insert simples
```

**Transações contra um documento contador único não escalam.** Ver [MSPIKE-4](mspike-4.md) — que é onde o desenho mudou.

## Veredito

**O alvo M0 está de pé.** Os dois pilares existem. Mas as transações ficam reservadas para operações raras (compactação), não para o caminho de escrita.
