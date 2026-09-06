# MSPIKE-4 — Como gerar revisões: o desenho mudou

**Status:** ✅ concluído · **Data:** 2026-09-06 · **Decisão:** [ADR-0001](../../docs/adr/0001-revisao-por-clustertime.md)

## O problema

No kine a revisão do etcd é o `id` da linha (`AUTOINCREMENT`). O apiserver depende de três propriedades: **monotônica**, **sem buracos permanentes**, **visível em ordem**. MongoDB não tem AUTOINCREMENT.

## Estratégias com contador — todas ruins

Concorrência 16, 80 escritas:

| Estratégia | Sucesso | Taxa | p50 | Problema |
|---|---|---|---|---|
| A· transação, sem retry | **15/80** | 41/s | 91 ms | 81% de falha por `WriteConflict` |
| B· transação + retry exponencial | **49/80** | 8,2/s | **870 ms** | ainda falha, e a latência colapsa |
| C· contador atômico, sem transação | 80/80 | 37/s | 174 ms | funciona, mas 6,5× mais lento que um insert |

Todas passam por um único documento contador, que vira ponto de serialização. A "correta" (B) entrega **8,2 escritas/s** — abaixo do que um cluster de 20 nós precisa.

## E o problema pior: o Change Stream não entrega em ordem de revisão

Com o contador, as revisões chegam no watch **fora de ordem**:

```
revisões vistas: [5, 13, 14, 9, 11, 1, 15, 7, 4, 2, 3, 12, 6, 10]
eventos fora de ordem: 13 de 29
```

Faz sentido: a ordem do oplog é a ordem de **commit**, não a de aquisição do contador. Duas escritas pegam 5 e 6, e a 6 commita primeiro.

Para o kine isso é fatal — o watch precisa de ordem estrita. Exigiria um buffer de reordenação, reintroduzindo latência e complexidade justamente onde o Change Stream deveria ter simplificado.

## A solução: `clusterTime` como revisão

Em vez de inventar uma sequência, **usar a que o MongoDB já mantém**. Medido:

| Pergunta | Resultado |
|---|---|
| A revisão é conhecida **no momento da escrita**? | ✅ `session.operation_time` após o insert |
| É única sob concorrência? | ✅ 40 escritas concorrentes, **40 revisões distintas**, 0 duplicadas |
| O Change Stream entrega em ordem dela? | ✅ **0 eventos fora de ordem** em 40 |
| O `clusterTime` da escrita casa com o do evento? | ✅ **idênticos** — `Timestamp(1788730614, 7)` nos dois |

Codificação para `int64`: `(ts.time << 32) | ts.inc`.

**Isso elimina de uma vez:** o contador, a contenção, as transações no caminho quente, a reordenação no watch e o gap-fill. A ordem do watch passa a ser a ordem das revisões **por construção**, não por esforço.

## O que fica em aberto

1. **A revisão deixa de ser densa.** Salta em vez de incrementar de 1. O etcd real incrementa; o apiserver trata `resourceVersion` como valor opaco monotônico, mas isso precisa ser validado com um k3s de verdade (`MT-3`) — e afeta `compactMinRetain`, que conta revisões.
2. **Vida útil da codificação.** `time << 32` chega a ~7,68 × 10¹⁸ hoje; o teto do `int64` é 9,22 × 10¹⁸. A codificação estoura por volta de **2038**. Usar um epoch-base do cluster (`(time - base) << 20 | inc`) resolve com folga de séculos.
3. **Transações continuam úteis** para compactação e outras operações raras — só não para o caminho de escrita.

## Reproduzir

```bash
spikes/mspike-4-revisoes.py [concorrencia] [total]
```
