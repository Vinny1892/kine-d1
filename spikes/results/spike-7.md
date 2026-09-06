# SPIKE-7 — Sessions API pela REST API

**Status:** ✅ concluído · **Data:** 2026-09-06 · **Resposta: não disponível**

## A pergunta

A Sessions API do D1 dá consistência sequencial em réplicas de leitura, via *bookmarks* que carregam um ponto no tempo entre queries. O kine depende de ler a revisão que acabou de gravar, então sem isso as réplicas de leitura são inutilizáveis. A doc da Cloudflare descreve a Sessions API em termos do binding de Worker (`withSession(bookmark)`) — ela existe pela REST?

## Resultado

**Não há veículo para bookmark na REST API.**

- `read_replication` do banco: `{"mode": "disabled"}` (default).
- O `meta` da resposta traz `served_by_primary` e `served_by_region`, mas **nenhum campo de bookmark ou commit token**.
- Nenhum header de resposta relacionado a D1 ou bookmark.

Tentativas de passar a sessão:

| Tentativa | Resultado |
|---|---|
| `{"sql":…, "session": "first-primary"}` | aceito, **sem erro** |
| `{"sql":…, "session_id": "first-unconstrained"}` | aceito, **sem erro** |
| header `x-cf-d1-session` | aceito, sem header de volta |
| header `cf-d1-session-commit-token` | aceito, sem header de volta |

**Atenção: "aceito sem erro" aqui é o pior resultado possível.** Campos desconhecidos no body são **ignorados em silêncio**. Uma implementação que passasse um bookmark inventado pareceria funcionar e não teria efeito nenhum — falha silenciosa, do tipo que só aparece em produção sob replicação.

## Consequências

1. **Manter `read_replication` desligado no MVP.** É o default, e é a única configuração segura sem bookmarks.
2. Se réplicas de leitura virarem requisito (latência global), isso **exige** o Worker proxy (`NEXT-1`), onde o binding dá acesso à Sessions API. Passa a ser a justificativa mais forte para a Opção B — mais forte que batching.
3. O driver não deve enviar campos não documentados no body, justamente porque o silêncio esconde o erro.
