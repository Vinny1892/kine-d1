# MSPIKE-9 + MSPIKE-5 — Modelo de documento, índices e latência

**Status:** ✅ concluído · **Data:** 2026-09-06

## Schema

```javascript
// coleção kine — o log de revisões
{ _id: ObjectId, rev: int64,        // rev = clusterTime (ADR-0001)
  name: str, created: bool, deleted: bool,
  create_revision: int64, prev_revision: int64, lease: int64,
  value: BinData, old_value: BinData, expires_at: Date|null }

// índices
{ name: 1, rev: -1 }                  // Get da última revisão
{ rev: 1 }                            // After / watch
{ name: 1, prev_revision: 1 } UNIQUE  // detecção de chave duplicada
{ prev_revision: 1 }                  // compactação
{ expires_at: 1 } TTL                 // lease
```

`DuplicateKeyError` (code 11000) no índice `name_prev_uniq` é mapeável direto para `server.ErrKeyExists`. Confirmado.

## Uso de índice

| Query | Plano |
|---|---|
| Get: última revisão de uma chave | ✅ IXSCAN |
| After: revisões > X | ✅ IXSCAN |
| List: range de prefixo | ✅ IXSCAN |
| Compact: por `prev_revision` | ✅ IXSCAN |
| **ListCurrent via `$group`** | ❌ **COLLSCAN**, 880 ms |

## Latência

| Query | p50 | p95 |
|---|---|---|
| Get de 1 chave | **23,4 ms** | 25,1 ms |
| After — ocioso | **23,2 ms** | 24,1 ms |
| CurrentRevision | 23,3 ms | 30,0 ms |
| Count por prefixo | 23,3 ms | 24,1 ms |
| escrita (insert + set rev) | 54,9 ms | 54,3 ms |
| **ListCurrent (300 chaves, com value)** | **823 ms** | 1175 ms |
| **ListCurrent (300 chaves, sem value)** | **54,6 ms** | 83,8 ms |

## O achado: o `LIST` é limitado por banda, não por índice

Medindo a mesma query com payloads diferentes:

| Volume | p50 | Throughput |
|---|---|---|
| 300 × 1 KB = 0,29 MB | 61 ms | 4,8 MB/s |
| 300 × 6 KB = 1,76 MB | 269 ms | 6,5 MB/s |
| 300 × 20 KB = 5,86 MB | 3498 ms | 1,7 MB/s |

E a comparação decisiva: **sem `value` são 54,6 ms; com `value` são 823 ms — 15×**.

Ou seja: trocar `$group` por uma coleção materializada de estado corrente **não resolveu** (ficou em 1403 ms). O gargalo nunca foi o plano de execução, é o payload atravessando a rede.

Isso também explica retroativamente o D1, que media 1039 ms para as mesmas 300 chaves. **É uma característica de qualquer datastore remoto, não do MongoDB.**

Consequências:
1. **`keysOnly` deixa de ser otimização e vira caminho principal.** O `server.Backend` recebe esse parâmetro; usá-lo bem é o que separa um LIST de 55 ms de um de 823 ms.
2. Uma coleção materializada de estado corrente ainda vale — não pela latência, mas por evitar o COLLSCAN do `$group` e reduzir `rows` escaneadas.
3. O limite de transferência do M0 (10 GB/7 dias ≈ 1,4 GB/dia) merece atenção: um LIST completo de um cluster de 500 pods move ~3 MB.

## Escrita: o custo do `clusterTime`

A revisão só existe **depois** do insert (`session.operation_time`), então gravá-la no documento exige uma segunda operação:

| Estratégia | p50 |
|---|---|
| só insert (revisão não gravada) | **30,7 ms** |
| insert + update da revisão | 59,5 ms |
| insert + update em transação | 80,9 ms |

O caminho sem transação custa ~29 ms extras. Ainda é **4× melhor que os 232 ms do D1**, mas é o ponto óbvio de otimização do Épico B.

> Alternativa a explorar no `MFND-4`: não gravar `rev` no documento e manter o índice revisão→documento em memória, populado pelo próprio Change Stream que o watch já consome. Reduziria a escrita a uma operação (30,7 ms), ao custo de precisar reconstruir o índice no start.

## Custo por mutação e teto

Medido com o ciclo completo (log histórico + coleção corrente):

```
40 mutações em 3,8s = 10,4 mutações/s
3 ops por mutação (insert + update rev + upsert corrente)
teto de 100 ops/s do M0 -> ~33 mutações/s
```

O modelo de fontes de write herdado do D1 estimava 3,3 writes/s ocioso e 10 em operação normal. **Cabe, com ~3× de folga.**

## Storage — o teto real (MSPIKE-7)

```
documento de 6 KB, com índices: 13.694 bytes
512 MB / 13.694 = ~39.200 documentos
```

Como o kine guarda histórico até compactar, esse é o total de **revisões vivas**. Para um cluster de 500 pods compactando a cada 5 minutos a 10 writes/s, o estado corrente mais o histórico ficam na casa de 3.500 documentos — bem dentro do teto.

**Mas o M0 não expande: ao encher, para.** Se a compactação atrasar, o cluster para junto. Isso torna o alerta de storage (`MOPS-1`) um requisito, não um conforto — a mesma conclusão que o D1 teve sobre o alerta de custo, por um caminho diferente.
