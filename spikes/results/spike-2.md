# SPIKE-2 — Rate limit real de `/query` e `/raw`

**Status:** ✅ concluído · **Data:** 2026-09-06 · **Decide:** Opção A vs Opção B

## A pergunta

O limite global da API v4 da Cloudflare — **1.200 requisições / 5 min por usuário (~4 req/s)** — se aplica aos endpoints de query do D1? Um cluster k3s precisa de 6-16 req/s. Se o limite valesse, a Opção A (REST direta) estaria morta e o Worker proxy seria obrigatório.

## Método

Escalonar a taxa e acumular muito mais de 1.200 requisições dentro de uma janela de 5 minutos, observando 429. Leituras com `SELECT 1` (`rows_read` = 0); escritas com `INSERT` numa tabela descartável.

## Resultado: nenhum 429. Em lugar nenhum.

### Leitura (`SELECT 1`)

| Fase | Taxa alvo | Taxa real | n | Acumulado | Status | p50 | p95 |
|---|---|---|---|---|---|---|---|
| A | 5/s | 5,0/s | 100 | 100 | 100× 200 | 255 ms | 292 ms |
| B | 10/s | 9,9/s | 200 | 300 | 200× 200 | 262 ms | 345 ms |
| C | 20/s | 19,6/s | 400 | 700 | 400× 200 | 245 ms | 321 ms |
| D | 40/s | 39,7/s | 1200 | **1900** | 1200× 200 | 260 ms | 357 ms |

**1.900 requisições em 88 segundos** — 5,4× o limite documentado, dentro da janela de 5 min. Zero 429.

### Escrita (`INSERT`)

| Fase | Taxa alvo | Taxa real | n | Status | p50 | p95 |
|---|---|---|---|---|---|---|
| A | 5/s | 5,0/s | 100 | 100× 200 | 304 ms | 399 ms |
| B | 10/s | 9,9/s | 200 | 200× 200 | 287 ms | 386 ms |
| C | 20/s | 19,7/s | 400 | 400× 200 | 282 ms | 425 ms |
| D | 40/s | 39,6/s | 1200 | 1200× 200 | 284 ms | 410 ms |

### Saturação de escrita — procurando o teto

| Taxa alvo | Taxa real | n | Status | p50 | p95 |
|---|---|---|---|---|---|
| 80/s | 78,1/s | 1200 | 1200× 200 | 299 ms | 426 ms |
| 160/s | 155,6/s | 2400 | 2400× 200 | 296 ms | 434 ms |
| **300/s** | **285,9/s** | 4500 | 4500× 200 | 347 ms | 617 ms |

**286 writes/s sustentados**, sem 429 e sem erro. A latência subiu só de 284 ms para 347 ms — o joelho da curva **não foi alcançado**.

## Verificação de integridade

Depois de 10.000 inserts concorrentes:

```
SELECT COUNT(*), MAX(id) FROM t_rl  ->  [10000, 10000]
```

Contagem exata, `MAX(id)` idêntico à contagem. **Nenhuma escrita perdida e nenhum buraco na sequência de IDs**, mesmo com centenas de inserts concorrentes por segundo.

Isso é mais importante do que parece: no kine o `id` é a revisão do etcd, e buracos disparam o mecanismo de gap-fill (`Fill`/`IsFill`). O AUTOINCREMENT do D1 sob concorrência alta se comportou perfeitamente — o gap-fill deve ser raro na prática.

## Veredito

> **O limite de 1.200/5min NÃO se aplica aos endpoints de query do D1.** A Opção A (REST direta) é viável e fica confirmada como a arquitetura do MVP.

Faz sentido com o [changelog de 2025-05-30](https://developers.cloudflare.com/changelog/2025-05-30-d1-rest-api-latency/): `/query` e `/raw` passaram a ser servidos na borda, fora dos data centers centrais que impõem o limite do plano de controle. O limite continua valendo para os endpoints de controle (criar/apagar banco) — que o kine usa uma vez no setup.

### Consequências

1. **Risco 5.2 do IDEA.md está eliminado** — era o único 🔴 com potencial de matar o projeto.
2. **Throughput deixa de ser preocupação.** 286 writes/s contra os ~10 writes/s de um cluster médio: **28× de folga**. A estimativa do IDEA.md ("se o D1 sustenta 50-200 writes/s") era conservadora.
3. **O Worker proxy (NEXT-1) deixa de ser contingência** e vira otimização opcional — justificável só por batching e Sessions API, não por rate limit.
4. **Sobra um único gargalo real: a latência** (~260 ms de piso, SPIKE-1). E esse não tem contorno.

## Ressalvas honestas

- Medido a partir de **uma** máquina e **um** IP. Pode haver limites por conta que só apareçam com mais origens em paralelo, ou limites de longo prazo (por hora/dia) que uma janela de 88 s não detecta.
- Não houve teste de **duração longa**. Um cluster real martela o endpoint por semanas; convém revalidar no TEST-4.
- O plano da conta influencia. Confirmar em qual plano estes números foram obtidos antes de generalizar.

## Reproduzir

```bash
./spikes/spike-2-ratelimit.py                    # leitura
./spikes/spike-2-ratelimit.py --write            # escrita
./spikes/spike-2-ratelimit.py --endpoint query   # o outro endpoint
```
