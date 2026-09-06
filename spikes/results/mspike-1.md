# MSPIKE-1 — Cluster M0: identidade, limites e linha de base

**Status:** ✅ concluído · **Data:** 2026-09-06

| | |
|---|---|
| MongoDB | **8.0.32** |
| Topologia | replica set `atlas-5uuxgo-shard-0`, **3 nós** |
| `maxBsonObjectSize` | **16.777.216** (16 MB) |
| `maxWriteBatchSize` | 100.000 |
| Região (inferida) | **São Paulo ou muito próximo** — ping p50 21,4 ms |

## Latência — contra os 232 ms do D1

| Operação | p50 | p95 | p99 |
|---|---|---|---|
| `ping` | 21,6 ms | 28,5 ms | 55,9 ms |
| `insert_one` (6 KB) | 27,2 ms | 37,4 ms | 264,6 ms |
| `find_one` por índice | 21,9 ms | 43,2 ms | 376,2 ms |
| **`insert` w=majority** | **26,6 ms** | 32,4 ms | 34,6 ms |
| **`find` readConcern=majority** | **21,6 ms** | 23,6 ms | 24,2 ms |

**8,7× mais rápido que o D1** no insert (26,6 ms contra 232 ms). A região São Paulo faz o que nenhuma otimização faria no D1, que não tem presença na América do Sul.

## Dois riscos herdados que morreram aqui

**Consistência (risco 6.5) — resolvido de graça.** `w=majority` custa 26,6 ms contra 27,2 ms do write default: **latência indistinguível**. O kine pode usar majority no caminho de escrita sem penalidade.

**Tamanho de valor — deixa de existir.** O documento pode ter 16 MB; o D1 travava em 2 MiB por valor e 4 MiB por linha, o que forçou o ADR-0001 (hex + `unhex()`) e uma margem de 28% no pior caso. Aqui um objeto k8s de 1,5 MB com `old_value` junto ocupa menos de 20% do limite.

## Reproduzir

```bash
spikes/mspike-1-setup.py
```
