# ADR-0001 — Representação de BLOB sobre a REST API do D1

**Status:** aceito · **Data:** 2026-09-06 · **Evidência:** [SPIKE-3](../../spikes/results/spike-3.md)

## Contexto

O kine guarda em `kine.value` e `kine.old_value` os objetos do Kubernetes serializados em protobuf — bytes arbitrários de 0x00 a 0xFF. A REST API do D1 transporta parâmetros em JSON, que não representa bytes crus: `params` é documentado como array de strings, e qualquer sequência que não seja UTF-8 válido é rejeitada na codificação.

Precisamos de uma codificação de transporte. Ela define o tipo da coluna, portanto trava o schema (`KINE-2`) e o codec do driver (`DRV-5`).

## Restrições medidas

Do SPIKE-3, contra um banco D1 real:

- **2 MiB por valor individual** (2.097.152 bytes) — acima disso, `SQLITE_TOOBIG`.
- **4 MiB por linha somada** (4.194.304 bytes).
- A documentação da Cloudflare afirma 2.000.000 bytes para "string, BLOB **ou linha**". A parte da linha está errada: o limite real de linha é o dobro.
- O Kubernetes limita objetos a ~1,5 MB, então o pior caso do kine é `value` + `old_value` = 3,0 MB numa linha.

## Opções

| | Transporte | Armazenamento | Margem no pior caso |
|---|---|---|---|
| base64 → TEXT | 1,33× | 1,33× | **2,3%** |
| **hex + `unhex()` → BLOB** | 2,00× | **1,00×** | **28%** |
| array de ints | 4,58× | 1,00× | 28% |
| base64 → `CAST AS BLOB` | 1,33× | 1,33× | 2,3% |

## Decisão

**Codificar em hexadecimal no transporte e gravar BLOB nativo, via `unhex(?)` na escrita e `hex(v)` na leitura.** Por cima disso, compressão (`DRV-6`) na camada do driver.

```sql
INSERT INTO kine(..., value, old_value) VALUES(..., unhex(?), unhex(?))
SELECT ..., hex(value), hex(old_value) FROM kine ...
```

## Justificativa

**Armazenamento 1:1 é o que compra margem.** No pior caso o kine grava 3,0 MB numa linha de teto 4 MiB — 28% de folga. Com base64 a mesma linha ficaria a 2,3% do limite. Uma margem de 48 KB contra um limite rígido que devolve erro em vez de degradar não é uma margem; é uma falha esperando o objeto certo.

**O custo de transporte é irrelevante onde ele acontece.** Os 2× do hex contra os 1,33× do base64 pesam sobre a rede, que já é o gargalo do projeto (~260 ms de piso). Mas objetos k8s típicos têm 5-10 KB, e nessa faixa a diferença medida foi **ruído** (226 ms contra 225 ms em 1 KB). A diferença só aparece em 1,4 MB — 1601 ms contra 1358 ms — que é o caso raro.

**A compressão inverte a comparação de qualquer jeito.** Protobuf de objetos k8s comprime bem; com s2 o payload típico cai a uma fração, e os 2× do hex passam a incidir sobre um valor bem menor que o original. A combinação hex+compressão vence o base64 puro nos dois eixos ao mesmo tempo.

**`CAST(? AS BLOB)` foi descartado por ser enganoso:** produz `typeof = blob` sem decodificar nada, guardando a string base64 com o custo do base64 e a aparência de BLOB nativo. É o pior dos dois mundos, difícil de perceber em revisão.

**Array de ints foi descartado** por 4,58× de transporte sem nenhuma vantagem sobre o hex.

## Consequências

- O schema usa `value BLOB` e `old_value BLOB` — igual ao driver SQLite do upstream, o que mantém o SQL de `pkg/drivers/generic` intacto.
- O driver precisa reescrever as queries que tocam essas colunas para envolver `unhex()`/`hex()`. Isso **não** é transparente no nível de `database/sql`: exige interceptar no dialeto D1 (`KINE-3`), não apenas no codec.
- Objetos acima de ~2 MiB comprimidos continuam impossíveis. Está acima do limite do k8s, mas o driver deve devolver um erro claro em vez de deixar vazar `SQLITE_TOOBIG`.
- Vale um teste de regressão com `value` e `old_value` no teto de 1,5 MB, para que a margem de 28% não seja corroída sem ninguém notar (`TEST-1`).
