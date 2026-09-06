# SPIKE-3 — Representação de BLOB sobre a REST API JSON do D1

**Status:** ✅ concluído · **Data:** 2026-09-06 · **Decisão:** [ADR-0001](../../docs/adr/0001-representacao-de-blob.md)

## A pergunta

O corpo da REST API é JSON e `params` é documentado como array de strings, mas os valores do kine são protobuf binário arbitrário (bytes 0x00-0xFF). Como passar bytes crus por um canal que não os aceita? A escolha muda o schema, então bloqueia `DRV-5` e `KINE-2`.

## Estratégias testadas

Payload de bytes aleatórios — pior caso, sem compressibilidade.

| Estratégia | 1 KB | 100 KB | 1,4 MB | Transporte | No banco | `typeof` |
|---|---|---|---|---|---|---|
| base64 → TEXT | ✓ | ✓ | ✓ | **1,33×** | 1,33× | `text` |
| **hex + `unhex()` → BLOB** | ✓ | ✓ | ✓ | 2,00× | **1,00×** | `blob` |
| array de ints → BLOB | ✓ | ✓ | ✓ | 4,58× | 1,00× | `blob` |
| base64 → `CAST AS BLOB` | ✓ | ✓ | ✓ | 1,33× | 1,33× | `blob` |
| string UTF-8 crua | ✗ | ✗ | ✗ | — | — | — |

Quatro fazem round-trip byte a byte. A UTF-8 crua falha na codificação, como esperado — bytes arbitrários não são UTF-8 válido.

**`CAST(? AS BLOB)` é uma armadilha:** o `typeof` vira `blob`, mas `length()` devolveu 1.911.468 para um payload de 1.433.600 bytes. Ele apenas reinterpreta a string base64 como bytes — **não decodifica nada**. Custa o mesmo armazenamento do TEXT, com aparência enganosa de BLOB nativo.

### Latência (insert / select)

| Estratégia | 1 KB | 100 KB | 1,4 MB |
|---|---|---|---|
| base64 → TEXT | 225 / 212 ms | 485 / 393 ms | 1358 / 690 ms |
| hex + `unhex()` | 226 / 206 ms | 538 / 460 ms | 1601 / 951 ms |

No tamanho que importa de verdade (objetos k8s típicos, 5-10 KB) a diferença é **ruído**. Ela só aparece em payloads grandes, que são a exceção.

## O achado principal: a documentação está errada sobre o limite de 2 MB

A doc da Cloudflare diz "any single string, BLOB, **or table row** is capped at 2.000.000 bytes". O comportamento real são **dois limites distintos**, e ambos são potências de 2:

| Limite | Valor real | Evidência |
|---|---|---|
| **Por valor individual** | **2 MiB** (2.097.152) | 1,9 MB aceito · 2,1 MB → `SQLITE_TOOBIG` |
| **Por linha somada** | **4 MiB** (4.194.304) | 3,99 MB aceito · 4,61 MB rejeitado |

Os seis pontos de dados coletados batem com esses dois valores **sem uma única exceção**:

```
valor:  1.945.600 < 2.097.152  aceito       2.150.400 > 2.097.152  rejeitado
linha:  4.147.200 < 4.194.304  aceito       4.608.000 > 4.194.304  rejeitado
        3.993.600 < 4.194.304  aceito       5.836.800 > 4.194.304  rejeitado
```

### O que isso faz com o risco 5.3 do IDEA.md

O risco estava **superestimado**. O caso real do kine — `value` e `old_value` cada um no teto de 1,5 MB do k8s, 3,0 MB de linha — foi testado diretamente:

```
2 × 1,5 MB = 3,0 MB por linha:  ✓ aceito
leitura de volta: 1.536.000 bytes em 1235ms — ✓ byte a byte idêntico
```

Cabe, com 28% de folga sobre o limite de linha.

### Mas isso condena o base64

Com base64 (+33%), um objeto de 1,5 MB vira 2.048.000 bytes por valor — **2,3% abaixo** do limite de 2 MiB. E a linha vai a 4.096.000 — **2,3% abaixo** do limite de 4 MiB.

Funcionaria. Por 48 KB de margem. Isso não é margem, é sorte.

## Decisão

**hex + `unhex()`**, com compressão por cima (`DRV-6`). Detalhes e alternativas descartadas em [ADR-0001](../../docs/adr/0001-representacao-de-blob.md).

## Reproduzir

```bash
./spikes/spike-3-blob.py
```
