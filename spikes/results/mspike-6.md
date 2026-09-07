# MSPIKE-6 — O que acontece ao estourar o teto de 100 ops/s do M0

**Status:** ✅ concluído · **Data:** 2026-09-07

Era a última afirmação grande do projeto sem medição. O pivot do Cloudflare D1 para o MongoDB se apoiou em uma premissa: **excesso de carga precisa virar lentidão, não fatura.** No D1 um operador em hot loop gerava US$ 887/mês sem freio algum.

## A premissa se confirma — zero erros

Até **6× o teto documentado**, nenhuma operação falhou:

| Taxa alvo | ops/s reais | Erros | p50 | p99 |
|---|---|---|---|---|
| 50/s (escrita) | 47 | **0** | 34 ms | 810 ms |
| 100/s (no teto) | 60 | **0** | 39 ms | 2.928 ms |
| 250/s (2,5×) | 70 | **0** | 995 ms | 22.388 ms |
| 600/s (6×) | 92 | **0** | 1.032 ms | **63.836 ms** |
| 100/s (leitura) | 100 | **0** | 26 ms | 110 ms |
| 600/s (leitura) | 160 | **0** | 1.009 ms | 20.048 ms |

Nenhum código de erro, nenhuma exceção de throttle. O Atlas **enfileira** em vez de rejeitar. E a recuperação é limpa: cinco segundos depois do pico de 6×, o p50 voltou a 30 ms.

**O teto real de escrita é ~92 ops/s** — coerente com os 100 documentados. Leitura escala melhor, chegando a 160 ops/s.

## Mas o throttle tem um custo que erro nenhum revelaria

Duas coisas que a ausência de erros esconde:

### 1. A latência explode muito antes do teto

O p99 sai de 810 ms (metade do teto) para **63 segundos** (6× o teto). Já **no teto documentado** o p99 é de 2,9 s.

Para o Kubernetes isso é fatal de um jeito que um erro não seria: a leader election tem `RenewDeadline` de 10 s. Uma escrita que leva 63 s significa **scheduler e controller-manager perdendo a liderança** — exatamente o modo de falha que derrubou as primeiras tentativas de MT-3. O cluster não recebe erro; ele simplesmente para de funcionar.

### 2. O Change Stream fica para trás

```
eventos recebidos: 10.000 (todos)
erros no stream: nenhum
último evento recebido há 43,9 s
```

O stream não quebrou nem perdeu evento — mas ficou **44 segundos atrasado**. Para o apiserver, um watch atrasado 44 s é um cluster que não vê suas próprias mudanças por 44 s: reconciliação para, informers ficam obsoletos, e a leader election já perdeu a corrida.

Com a janela de oplog de 4,4 h ([MSPIKE-8](mspike-8.md)), 44 s não chega perto de invalidar. Mas mostra o mecanismo: sob carga sustentada acima do teto, o atraso cresce, e um atraso suficientemente longo cai no cenário de invalidação.

## Comparação com o D1, que é o que motivou tudo

| | Cloudflare D1 | Atlas M0 |
|---|---|---|
| Excesso de carga vira | **fatura** (US$ 22/mês por write/s) | **latência** |
| Teto | nenhum — a conta cresce | ~92 ops/s de escrita |
| Como você descobre | na fatura, no fim do mês | no cluster, na hora |
| Recuperação | irrelevante (o dinheiro já foi) | imediata ao cessar a carga |

A premissa está certa: **é melhor um sistema que degrada visivelmente do que um que cobra silenciosamente.** Mas "melhor" não é "indolor" — o D1 quebrava o orçamento, o M0 quebra o cluster.

## Tradução para capacidade de cluster

Cada mutação do kine custa ~2 operações no MongoDB (o insert e o update que grava `rev`). Então:

| | Escritas/s do cluster |
|---|---|
| Teto absoluto (~92 ops/s) | ~45 mutações/s |
| Onde a latência ainda é sadia (~60 ops/s) | **~30 mutações/s** |
| Cluster médio ocioso (modelo do SPIKE-9) | 3,3 |
| Cluster médio em operação | 10 |

**Folga de 3× a 9×** para um cluster médio. Suficiente, mas não confortável: um operador com reconcile agressivo consome essa folga rápido — e o sintoma será leader election flapando, não uma mensagem de erro.

## Consequência operacional

Isso promove o `MOPS-1` (métricas) de "bom ter" para **necessário**, mas com o alvo corrigido: o que importa monitorar **não é erro** — não vai haver nenhum. É **latência de escrita p99** e **atraso do change stream**. Se o p99 passar de ~1 s, o cluster está a caminho de perder a liderança.

## Reproduzir

```bash
spikes/mspike-6-throttle.py [multiplicador]
```
