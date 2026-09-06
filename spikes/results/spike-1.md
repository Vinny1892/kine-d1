# SPIKE-1 — Provisionar banco D1 de testes + token com escopo mínimo

**Status:** ✅ concluído · **Data:** 2026-09-06

## Resultado

| Item | Valor |
|---|---|
| Banco | `kine-d1-spike` |
| Região do primary | **ENAM** (Eastern North America) |
| `read_replication.mode` | `disabled` (default — é o que queremos no MVP) |
| `file_size` vazio | 12.288 bytes |
| Escopo do token | `Account · D1 · Edit` — suficiente para criar banco, consultar e ler metadados |

O `/raw` respondeu 200 e o bound param fez round-trip. Critério de aceite cumprido.

## Achado colateral: latência (adianta o SPIKE-4)

Medido de **Brasil → ENAM**, conexão reutilizada (keep-alive), RTT ponta a ponta em ms:

| Operação | p50 | p95 |
|---|---|---|
| `SELECT 1` (leitura trivial) | **274** | 330 |
| `CREATE TABLE` (DDL) | 268 | 286 |
| **`INSERT` (escrita real)** | **304** | **395** |
| `SELECT ... WHERE id = ?` (indexado) | 258 | 276 |

Sem keep-alive (uma conexão nova por requisição, como faz `curl`): p50 **342**, p95 **611**, max **1041**. O handshake TLS custa ~70 ms — o driver Go precisa de pool com keep-alive, não é opcional.

### Por que isso importa

O IDEA.md estimava 150-350 ms para escrita. O medido fica no **topo dessa faixa** (p50 304 ms), e esse é o **piso**: tabela vazia, query trivial, sem concorrência.

Note que `SELECT 1` custa 274 ms. Ou seja, **não é o custo da query, é o custo de existir a rede** — toda operação do apiserver paga ~260 ms de pedágio, independente do que faça.

O `meta.duration` reportado pelo D1 foi de **0,46 ms**. Todo o resto — 99,8% do tempo — é rede e roteamento.

## Consequências para o projeto

1. **Confirma o risco de flapping de leader election.** Uma renovação de lease é uma escrita de ~300 ms. Com `RetryPeriod` de 2 s ainda cabe, mas a margem é bem menor do que num etcd local, e o D1 é single-threaded: sob concorrência a fila cresce.
2. **Keep-alive é requisito, não otimização** — vale ~70 ms por requisição (DRV-2).
3. **A latência não melhora com tuning nosso.** Os location hints do D1 não incluem a América do Sul; ENAM é o mais próximo do Brasil. Confirmar no SPIKE-4.

## Reproduzir

```bash
./spikes/spike-1-setup.sh
```
