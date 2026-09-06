# Cloudflare D1 — avaliado e descartado

**Período:** 2026-09-06 · **Resultado:** descartado por custo

Este diretório guarda a avaliação completa do Cloudflare D1 como backend do kine. O trabalho foi encerrado depois que a medição de custo mostrou que o modelo não fecha. Fica preservado por dois motivos: a decisão é rastreável, e o método dos spikes se reaproveita direto na avaliação do MongoDB.

## Por que foi descartado

Tudo funcionava. O problema era o preço.

| | |
|---|---|
| Arquitetura | ✅ REST direta viável — o limite de 1.200 req/5min **não** se aplica a `/query` e `/raw` |
| Capacidade | ✅ 286 writes/s sustentados, sem saturar. 28× o necessário |
| Schema | ✅ hex + `unhex()` → BLOB nativo, com 28% de margem no pior caso |
| Transações | ✅ transação diferida com CAS validada, inclusive sob concorrência |
| Latência | ⚠️ ~232 ms por escrita (Brasil → ENAM). Sem contorno: não há região sul-americana |
| **Custo** | 🔴 **US$ 22/mês por write/s sustentado, sem freio** |

O golpe fatal está no [SPIKE-9](spikes/results/spike-9.md):

- O control plane **sozinho**, sem nós e sem carga, consome 87% da franquia gratuita — a leader election renova ~4 leases a cada 2 s.
- Cada nó adiciona US$ 2,24/mês só de lease de kubelet.
- Um operador com `RequeueAfter: 10s` sobre 500 objetos — padrão que ninguém chamaria de abusivo — custa **US$ 1.037/mês**.
- **Não há freio automático:** o kine não tem rate limit e o D1 não tem hard cap configurável.

A faixa em que o projeto se sustentava era estreita demais: 3 a 20 nós, carga previsível, sem operadores de terceiros.

## Os erros que a medição corrigiu

Vale registrar, porque são o argumento a favor de medir antes de construir:

1. **Modelo de custo errado por ~70×.** A estimativa inicial dizia "US$ 0 a 3/mês"; o real era US$ 27 a 183/mês. A causa: cada `INSERT` escreve também os 6 índices e o `sqlite_sequence` — 8 `rows_written`, não 1.
2. **A documentação da Cloudflare está errada** sobre o limite de tamanho. Ela afirma 2.000.000 bytes para "string, BLOB ou linha". O real são dois limites distintos: 2 MiB por valor e 4 MiB por linha.
3. **A mitigação de transação que eu havia proposto era silenciosamente errada.** `UPDATE ... WHERE cas = ?` com valor errado afeta 0 linhas e **retorna sucesso** — apagaria dados sob premissa falsa, sem sinal. A guarda correta precisa gerar erro de verdade.

## O que continua valendo para o MongoDB

- A leitura do kine em [IDEA.md](IDEA.md) seções 2 e 3 — como o kine funciona e onde se pluga um driver.
- O modelo de fontes de write num cluster k3s (leader election fixa + lease por nó), no [SPIKE-9](spikes/results/spike-9.md).
- O método: provisionar, medir contra o serviço real, e desconfiar da documentação.

## Conteúdo

| | |
|---|---|
| [IDEA.md](IDEA.md) | o anteprojeto completo, com todas as seções corrigidas pelas medições |
| [spikes/](spikes/) | 9 spikes com scripts reproduzíveis e resultados |
| [adr-0001](adr-0001-representacao-de-blob.md) | representação de BLOB sobre JSON |
| [adr-0002](adr-0002-transacao-diferida.md) | transação diferida com CAS |
