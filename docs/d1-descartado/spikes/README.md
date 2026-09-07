# Spikes

Experimentos contra um banco **Cloudflare D1 real**, para validar as premissas do
[IDEA.md](../IDEA.md) antes de escrever código de produção. São descartáveis por
natureza: existem para produzir uma decisão, não para virar biblioteca.

## Pré-requisitos

`curl` e `jq`. Nada de wrangler — os spikes falam direto com a REST API, que é
exatamente o caminho que o driver vai usar.

## Credenciais

As credenciais ficam **fora do repositório**, em `~/.config/kine-d1/env`.
O `.gitignore` já bloqueia `*.env` na árvore, mas o arquivo nem sequer mora aqui.

### 1. Criar o token

Em <https://dash.cloudflare.com/profile/api-tokens> → **Create Token** →
**Create Custom Token**:

| Campo | Valor |
|---|---|
| Permissions | `Account` · `D1` · **Edit** |
| Account Resources | `Include` · a sua conta específica |
| TTL | opcional, mas recomendado para um token de spike |

Só isso. `D1 · Edit` é o escopo mínimo: cria bancos, executa queries e lê
metadados. Nenhuma outra permissão é necessária, e nenhuma outra deve ser dada —
esse token acaba virando a credencial do cluster.

### 2. Pegar o Account ID

Aparece na barra lateral direita de qualquer domínio no dashboard, ou em
**Workers & Pages** → **Overview**. É um hex de 32 caracteres.

### 3. Gravar o arquivo

```bash
mkdir -p ~/.config/kine-d1
cat > ~/.config/kine-d1/env <<'ENV'
CLOUDFLARE_ACCOUNT_ID=<seu account id>
CLOUDFLARE_API_TOKEN=<seu token>
ENV
chmod 600 ~/.config/kine-d1/env
```

O `D1_DATABASE_ID` é acrescentado automaticamente pelo `spike-1-setup.sh`.

Para usar outro caminho, exporte `KINE_D1_ENV=/caminho/para/env`.

## Rodando

```bash
./spikes/spike-1-setup.sh            # provisiona e valida (idempotente)
./spikes/spike-1-setup.sh meu-banco  # com outro nome de banco
```

## Convenções

- Nenhum script imprime o token. Se algum imprimir, é bug.
- Tudo é idempotente: rodar duas vezes não quebra nada.
- Resultados que embasam decisão vão para um ADR em `docs/adr/`, não ficam aqui.

## Estado

| Spike | O que responde | Status |
|---|---|---|
| SPIKE-1 | Banco provisionado e `/raw` acessível | ✅ [concluído](results/spike-1.md) |
| SPIKE-2 | **Rate limit real de `/query` e `/raw`** — decide REST direta vs Worker proxy | ✅ [concluído](results/spike-2.md) — sem limite; Opção A confirmada |
| SPIKE-3 | Representação de BLOB sobre JSON | ✅ [concluído](results/spike-3.md) — hex+unhex ([ADR-0001](../adr-0001-representacao-de-blob.md)) |
| SPIKE-4 | Latência p50/p95/p99 a partir do Brasil | ✅ [concluído](results/spike-4.md) |
| SPIKE-5 | `meta.last_row_id`, `changes`, `RETURNING` | ✅ [concluído](results/spike-5.md) |
| SPIKE-6 | `batch` atômico e o padrão CAS | ✅ [concluído](results/spike-6.md) — viável ([ADR-0002](../adr-0002-transacao-diferida.md)) |
| SPIKE-7 | Sessions API / bookmarks pela REST API | ✅ [concluído](results/spike-7.md) |
| SPIKE-8 | Schema do kine no D1 e a constraint de unicidade | ✅ [concluído](results/spike-8.md) |
| SPIKE-9 | **Teto de cluster e custo do pior caso** | ✅ [concluído](results/spike-9.md) — US$ 22/mês por write/s |
