# ADR-0001 — A revisão do etcd vem do `clusterTime`, não de um contador

**Status:** aceito · **Data:** 2026-09-06 · **Evidência:** [MSPIKE-4](../../spikes/results/mspike-4.md)

## Contexto

No kine a revisão do etcd é o `id` da linha, um `INTEGER PRIMARY KEY AUTOINCREMENT`. O apiserver depende de três propriedades: monotônica, sem buracos permanentes, e **visível em ordem**. MongoDB não tem AUTOINCREMENT.

A solução idiomática é um documento contador com `findAndModify` + `$inc`, opcionalmente dentro de uma transação para amarrar a revisão ao documento escrito.

## Medições que descartaram o contador

Concorrência 16, 80 escritas:

| Estratégia | Sucesso | Taxa | p50 |
|---|---|---|---|
| Transação, sem retry | 15/80 | 41/s | 91 ms |
| Transação + retry exponencial | 49/80 | **8,2/s** | **870 ms** |
| Contador atômico, sem transação | 80/80 | 37/s | 174 ms |

A variante correta entrega **8,2 escritas/s** — abaixo do necessário para um cluster de 20 nós. Todas serializam no mesmo documento.

E o problema decisivo é outro: com contador, **o Change Stream entrega as revisões fora de ordem** — 13 de 29 eventos. A ordem do oplog é a de commit, não a de aquisição do contador. Duas escritas pegam 5 e 6, e a 6 commita primeiro. Para o kine, watch fora de ordem é watch quebrado.

## Decisão

**Usar o `clusterTime` do MongoDB como revisão**, codificado em `int64`.

- Na escrita: `session.operation_time` após a operação.
- No watch: o campo `clusterTime` do evento do Change Stream.
- Codificação: `(ts.time - epochBase) << 20 | ts.inc`.

## Justificativa

Medido contra o cluster real:

- **Conhecida na escrita.** `session.operation_time` devolve o timestamp logo após o insert — a API síncrona do `server.Backend` continua possível.
- **Única sob concorrência.** 40 escritas concorrentes produziram 40 revisões distintas, zero duplicadas.
- **O Change Stream entrega em ordem dela.** Zero eventos fora de ordem em 40 — contra 13 de 29 com contador.
- **A escrita e o evento carregam a mesma revisão.** `Timestamp(1788730614, 7)` idêntico nos dois lados.

A diferença de fundo: o contador inventa uma ordem que compete com a do banco. O `clusterTime` **é** a ordem do banco. A ordenação do watch deixa de ser um problema a resolver e passa a ser uma propriedade da escolha.

Isso elimina de uma vez o contador, a contenção, as transações no caminho quente, o buffer de reordenação e o gap-fill.

## Consequências

- **A revisão deixa de ser densa.** Salta em vez de incrementar de 1. O apiserver trata `resourceVersion` como valor opaco monotônico, mas isso **precisa ser validado com um k3s real** (`MT-3`) antes de considerar a decisão firme. Afeta também `compactMinRetain`, que conta revisões — passará a ser uma janela de tempo.
- **A codificação precisa de epoch-base.** `time << 32` estoura o `int64` por volta de 2038. Com `(time - base) << 20` sobram séculos, ao custo de 20 bits para o `inc` (1,05 mi de operações por segundo — folgado contra o teto de 100 ops/s do M0). O `epochBase` vira metadado do cluster e **não pode mudar depois de gravado**.
- **Transações continuam no projeto**, mas para compactação e operações raras — não para escrita.
- **`Fill`/`IsFill` (gap-fill) deixam de ser necessários.** Não há sequência densa para ter buracos.
- Duas escritas no mesmo `inc` do mesmo segundo são impossíveis pelo próprio oplog, mas o driver deve falhar alto se detectar revisão duplicada, em vez de seguir.
