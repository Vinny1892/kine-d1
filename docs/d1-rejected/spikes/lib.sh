#!/usr/bin/env bash
# Biblioteca comum dos spikes do kine-d1.
# Nunca imprime o token. Carregue com:  source spikes/lib.sh

set -euo pipefail

ENV_FILE="${KINE_D1_ENV:-$HOME/.config/kine-d1/env}"
API="https://api.cloudflare.com/client/v4"

die() { printf '\033[31merro:\033[0m %s\n' "$*" >&2; exit 1; }
info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }

load_env() {
  [ -f "$ENV_FILE" ] || die "credenciais não encontradas em $ENV_FILE — veja spikes/README.md"
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a

  # Aceita os nomes usados pelo dashboard da Cloudflare como alias, para não
  # obrigar a reescrever o arquivo de credenciais.
  : "${CLOUDFLARE_API_TOKEN:=${ApiToken:-${CF_API_TOKEN:-}}}"
  : "${CLOUDFLARE_ACCOUNT_ID:=${AccountId:-${CF_ACCOUNT_ID:-}}}"
  export CLOUDFLARE_API_TOKEN CLOUDFLARE_ACCOUNT_ID

  [ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ] || die "account id não definido em $ENV_FILE (aceita CLOUDFLARE_ACCOUNT_ID ou AccountId)"
  [ -n "${CLOUDFLARE_API_TOKEN:-}" ]  || die "token não definido em $ENV_FILE (aceita CLOUDFLARE_API_TOKEN ou ApiToken)"
}

# cf <método> <caminho> [body-json]
# Caminho é relativo a /accounts/<id>. Ecoa a resposta crua.
cf() {
  # 'path' é variável especial do zsh (vinculada a $PATH) — nunca usar como local aqui.
  local method="$1" route="$2" body="${3:-}"
  local url="$API/accounts/$CLOUDFLARE_ACCOUNT_ID$route"
  if [ -n "$body" ]; then
    curl -sS -X "$method" "$url" \
      -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
      -H "Content-Type: application/json" \
      --data "$body"
  else
    curl -sS -X "$method" "$url" \
      -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"
  fi
}

# d1_raw <sql> [params-json-array] — executa contra o banco do spike via /raw
d1_raw() {
  local sql="$1" params="${2:-[]}"
  [ -n "${D1_DATABASE_ID:-}" ] || die "D1_DATABASE_ID não definido — rode spikes/spike-1-setup.sh"
  cf POST "/d1/database/$D1_DATABASE_ID/raw" \
    "$(jq -nc --arg sql "$sql" --argjson params "$params" '{sql:$sql, params:$params}')"
}

# checa se a resposta da API teve success:true, senão morre mostrando os erros
assert_ok() {
  local resp="$1" ctx="${2:-requisição}"
  if [ "$(printf '%s' "$resp" | jq -r '.success // false')" != "true" ]; then
    printf '%s' "$resp" | jq '.errors' >&2
    die "$ctx falhou"
  fi
}
