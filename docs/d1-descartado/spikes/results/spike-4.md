# SPIKE-4 — Latência e custo das queries reais do kine

**Status:** ✅ concluído · **Data:** 2026-09-06

Mede as queries reais de `pkg/drivers/generic/generic.go` (com `hex()`/`unhex()` do [ADR-0001](../../adr-0001-representacao-de-blob.md)) contra um D1 com 300 chaves e objetos de 6 KB — tamanho típico de objeto k8s.

## Latência por tipo de query

| Query | p50 | p95 | p99 | `rows_read` | `rows_written` |
|---|---|---|---|---|---|
| INSERT (Create/Update) | 232 ms | 331 ms | 554 ms | 3 | **8** |
| Get de 1 chave | 210 ms | 223 ms | 229 ms | 5 | 0 |
| **List de prefixo (300 chaves)** | **1039 ms** | 1164 ms | 1164 ms | **1304** | 0 |
| After — poll COM eventos | 1023 ms | 1057 ms | 1057 ms | 979 | 0 |
| After — poll OCIOSO | 203 ms | 212 ms | 212 ms | **1** | 0 |
| CurrentRevision | 203 ms | 212 ms | 233 ms | 1 | 0 |
| CompactRevision | 203 ms | 215 ms | 220 ms | 1 | 0 |

**O piso de ~205 ms é o custo de existir a rede.** Nenhuma query trivial fica abaixo disso.

**O `LIST` é o ponto fora da curva: 1 segundo para 300 chaves.** O apiserver faz `LIST` no startup de cada informer, então o boot do plano de controle paga esse pedágio várias vezes. Com 500-1000 chaves isso vira 2-3 s por listagem. É o número que mais deve preocupar no `TEST-3`.

**O poll ocioso é barato:** 203 ms e **1 linha lida**. O laço de 1×/s não é problema nem de custo nem de carga.

## Correção do modelo de custo — o erro do fator 8

O IDEA.md estimava `rows_written ≈ 2 × mutações` (1 insert + 1 delete na compactação). **Está errado.** Medido:

```
INSERT com os 6 índices do kine ....... 8 rows_written
INSERT em tabela sem índices .......... 1 rows_written
DELETE ................................ 1 rows_written
UPDATE ................................ 3 rows_written
```

Cada `INSERT` custa **8** linhas escritas: a linha, os 6 índices e o `sqlite_sequence` do AUTOINCREMENT. O D1 cobra por isso.

### Custo real

| Cenário | writes/s | rows_written/mês | Custo/mês |
|---|---|---|---|
| Médio, ocioso | 3,3 | 77,0 mi | **US$ 27** |
| **Médio, operação normal** | **10** | **233,3 mi** | **US$ 183** |
| Grande (50 nós) | 30 | 699,8 mi | US$ 650 |

Contra a estimativa anterior de "US$ 0 a 3/mês" — **um erro de cerca de 70×**.

`rows_read` continua irrelevante: o polling ocioso consome 2,6 mi/mês, ou **0,01%** da franquia de 25 bilhões.

### Oportunidade de otimização

Como o custo é dominado pelos índices, **remover índices é a alavanca mais direta**. Sair de 6 para 3 índices levaria o insert de 8 para 5 `rows_written` — cerca de **37% de economia**. O trade-off é `rows_read` maior nas queries que perderem índice, mas leitura é praticamente de graça aqui. Merece uma task própria.

## Reproduzir

```bash
./spikes/spike-4-latencia.py [n_amostras]
```
