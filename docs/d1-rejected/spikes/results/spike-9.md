# SPIKE-9 — Teto de cluster e o custo do pior caso

**Status:** ✅ concluído · **Data:** 2026-09-06 · **Decide:** se o projeto continua

## Por que este spike existe

O SPIKE-2 mostrou que capacidade não é o limite (286 writes/s sem saturar). O SPIKE-4 mostrou que o custo por INSERT é 8× maior do que eu estimava. Junte os dois e a pergunta que sobra é: **até onde dá para crescer antes de a conta ficar absurda — e o que acontece se um operador mal escrito começar a martelar o datastore?**

## Custo real por mutação, ponta a ponta

Medido com 400 mutações reais seguidas de um ciclo de compactação:

```
escrita ......   7,75 rows_written/mutação
compactação ..   0,63 rows_written/mutação
TOTAL            8,38 rows_written/mutação
```

## A métrica âncora

```
1 write/s sustentado = 21.727.440 rows_written/mês
franquia de 50 mi/mês cobre .......... 2,30 writes/s
cada write/s ADICIONAL custa ......... US$ 21,73/mês
```

> **US$ 22/mês por write/s sustentado.** É o único número que precisa ser lembrado.

## De onde vêm os writes

| Fonte | Taxa | Custo |
|---|---|---|
| Leader election dos controllers (~4 leases, renovação a cada 2 s) | **2,00 writes/s, fixo** | consome 87% da franquia |
| Lease de nó (kubelet, 1 write / 10 s) | 0,103 writes/s **por nó** | **US$ 2,24/mês por nó** |
| Node status completo | 1 write / 5 min por nó | desprezível |

**O control plane sozinho, sem nenhum nó e sem nenhuma carga, já consome 87% da franquia gratuita.** Isso define o piso.

## Cluster ocioso

| Nós | writes/s | US$/mês |
|---|---|---|
| 1 | 2,10 | **0** |
| 3 | 2,31 | **0** |
| 5 | 2,52 | 5 |
| 10 | 3,03 | 16 |
| 20 | 4,06 | 38 |
| 50 | 7,15 | 105 |
| 100 | 12,30 | 217 |

## O pior caso: operador em hot loop

Um reconcile que sempre grava status dispara o próprio watch, que dispara outro reconcile. O loop só é limitado pelo round-trip de escrita.

**Descoberta contraintuitiva: a lentidão do D1 é uma proteção acidental.** A 232 ms por write, cada worker em hot loop satura em **4,3 writes/s**. Num etcd local o mesmo operador faria milhares de writes/s. Aqui a latência age como rate limiter — mas converte volume em dinheiro.

| Workers em loop | writes/s | US$/mês | US$/ano |
|---|---|---|---|
| 1 | 4,3 | **44** | 524 |
| 2 | 8,6 | 137 | 1.648 |
| 5 | 21,6 | 418 | 5.020 |
| 10 | 43,1 | **887** | 10.640 |
| 20 | 86,2 | 1.823 | 21.880 |

Um único operador com `MaxConcurrentReconciles: 10` em hot loop custa **mais que o cluster inteiro**.

## O pior caso realista: requeue periódico

Nem precisa ser bug. Um operador que só reconcilia periodicamente:

| Objetos | Requeue | writes/s | US$/mês |
|---|---|---|---|
| 50 | 30 s | 1,7 | 0 |
| 100 | 30 s | 3,3 | 22 |
| 100 | 10 s | 10,0 | 167 |
| 500 | 30 s | 16,7 | 312 |
| 500 | 10 s | 50,0 | **1.037** |
| 1000 | 10 s | 100,0 | 2.123 |

`RequeueAfter: 10 * time.Second` sobre 500 objetos — um padrão que ninguém consideraria abusivo — custa **mil dólares por mês**.

## Teto por orçamento

| Orçamento | writes/s | Nós (ocioso) | Folga p/ carga real* |
|---|---|---|---|
| US$ 0 | 2,3 | 3 | **−0,7** |
| US$ 25 | 3,5 | 14 | 0,4 |
| US$ 50 | 4,6 | 25 | 1,6 |
| US$ 100 | 6,9 | 48 | 3,9 |
| US$ 200 | 11,5 | 92 | 8,5 |
| US$ 500 | 25,3 | 226 | 22,3 |

\* o que sobra para deploys, jobs, operadores e events depois de pagar 10 nós ociosos.

**A coluna da folga é a que importa.** Com US$ 50/mês você banca 25 nós ociosos — mas só 1,6 writes/s de atividade real. Nós são baratos; **atividade é cara**.

## Veredito

O gargalo do projeto **não é técnico, é financeiro** — e não tem freio automático. O kine não tem rate limit, o D1 não tem hard cap configurável. Um operador em loop simplesmente gera a conta.

**Onde o projeto se sustenta:** cluster pequeno (3-20 nós), carga previsível, sem operadores de terceiros. Faixa de US$ 0 a 40/mês. É homelab, borda, dev/staging, control plane de baixa mutação.

**Onde o projeto não se sustenta:** qualquer cluster que rode operadores que você não escreveu, CI criando Jobs, ou centenas de CRs com reconcile periódico. Não porque quebra — porque a fatura cresce sem aviso.

## O que muda o cálculo

- **KINE-9 (reduzir índices)**: −37% → US$ 13,70 por write/s. Move o teto em ~1,6×.
- **Alerta de `rows_written` (OPS-1)**: não reduz custo, mas transforma uma fatura surpresa num incidente detectável. Passa de "bom ter" a **requisito**.

## Reproduzir

```bash
./spikes/spike-9-carga.py [n_mutacoes]
```
