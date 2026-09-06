# ADR-0002 — Transação diferida com CAS no lugar de `BEGIN TRANSACTION`

**Status:** aceito · **Data:** 2026-09-06 · **Evidência:** [SPIKE-6](../../spikes/results/spike-6.md)

## Contexto

`server.Transaction` (`pkg/server/types.go:65`) exige `BeginTx`/`Commit`/`Rollback` com leituras e escritas intercaladas. O D1 rejeita `BEGIN TRANSACTION` e `SAVEPOINT` com erro explícito; oferece apenas `batch`, que é atômico mas **não interativo** — não dá para ler um resultado e decidir o próximo statement dentro da mesma transação.

O kine usa transações em dois lugares, ambos no `sqllog`:
- `sql.go:91` — `compactStart`, desduplicação de `compact_rev_key` na inicialização.
- `sql.go:234` — o ciclo de compactação, o caso que importa.

O padrão do compact é: ler revisão atual → ler `compact_rev` → **comparar com o valor esperado** → apagar linhas → gravar novo `compact_rev`. A comparação existe para detectar que outra instância compactou nesse meio-tempo.

## Decisão

Implementar `server.Transaction` como **transação diferida com compare-and-swap**:

- **Leituras** (`CurrentRevision`, `GetCompactRevision`) passam direto, em autocommit, e retornam na hora.
- **Escritas** (`Compact`, `SetCompactRevision`, `DeleteRevision`) são acumuladas num buffer.
- **`Commit()`** envia o buffer como um único `batch`, precedido de um statement de guarda que aborta tudo se o estado mudou desde a leitura.
- **`Rollback()`** descarta o buffer — nenhuma chamada de rede.

A guarda:

```sql
CREATE TABLE cas_guard(ok INTEGER PRIMARY KEY CHECK (ok = 1));

INSERT INTO cas_guard(ok)
SELECT 0 WHERE (SELECT prev_revision FROM kine WHERE name='compact_rev_key') != ?;
```

CAS casa → o `SELECT` não devolve linha, nada acontece. CAS não casa → tenta inserir `0`, viola o `CHECK` e **aborta o batch inteiro**.

## Justificativa

**O caminho ingênuo é silenciosamente errado.** `UPDATE ... WHERE prev_revision = ?` com valor errado afeta 0 linhas e retorna sucesso. O SPIKE-6 mediu: CAS correto passa, CAS errado **também passa**. Sem uma guarda que gere erro de verdade, a compactação apagaria dados sob premissa falsa sem sinal nenhum.

**A tabela de guarda não acumula lixo.** Ela nunca recebe uma linha: insere `0`, que sempre viola `CHECK(ok = 1)`. Quando o CAS casa, o `SELECT` não produz linha nenhuma. Verificado nos dois caminhos.

**Escolhida sobre o `abs()` overflow.** `abs(-9223372036854775808)` também aborta e dispensa tabela auxiliar, mas é um truque obscuro que depende de detalhe de implementação do SQLite e exigiria um parágrafo de comentário para qualquer um entender. A tabela de guarda é auto-explicativa e custa uma linha no schema.

**A semântica é mais fraca que `Serializable`, e isso é aceitável aqui.** As leituras não ficam num snapshot. Mas o único invariante que o compact precisa é "ninguém mexeu no `compact_rev_key` entre a minha leitura e a minha escrita", e é exatamente isso que o CAS garante. Validado com duas instâncias concorrentes: exatamente uma vence.

## Consequências

- `BeginTx` ignora `sql.TxOptions{Isolation: LevelSerializable}`. Documentar, porque a discrepância entre o que o chamador pede e o que o driver entrega é sutil.
- **O erro de CHECK precisa virar `server.ErrCompacted`.** Quando a instância perdedora aborta, o `sqllog` já trata `ErrCompacted` como situação normal (`sql.go:250`), não como falha. Sem essa tradução, um cluster multi-servidor logaria erro a cada ciclo de compactação. Vai para o `KINE-6`.
- `Tx.Compact()` retorna `RowsAffected`, que só existe depois do commit. Não é problema: o valor alimenta um único `logrus.Infof` (`sql.go:281`) que roda **depois** do `MustCommit()`, e nenhuma lógica depende dele. Retornar 0 e deixar o driver logar o número real do batch. Impacto cosmético.
- `Rollback()` sem commit não faz chamada de rede — mais barato que uma transação real.
- Um `Commit()` com buffer vazio deve ser no-op, sem round-trip.
- A tabela `cas_guard` entra no schema do `KINE-2`.
