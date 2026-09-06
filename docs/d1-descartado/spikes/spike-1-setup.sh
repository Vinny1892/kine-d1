#!/usr/bin/env bash
# SPIKE-1 — provisiona o banco D1 de testes e valida o acesso via /raw.
#
# Idempotente: se o banco já existir, reaproveita.
# Uso:  ./spikes/spike-1-setup.sh [nome-do-banco]

cd "$(dirname "$0")/.."
source spikes/lib.sh
load_env

DB_NAME="${1:-kine-d1-spike}"

info "conta: ${CLOUDFLARE_ACCOUNT_ID:0:8}… (token com ${#CLOUDFLARE_API_TOKEN} chars)"

# --- 1. o token funciona e tem o escopo certo? -----------------------------
info "verificando o token contra /d1/database"
resp="$(cf GET "/d1/database?per_page=1")"
if [ "$(printf '%s' "$resp" | jq -r '.success // false')" != "true" ]; then
  printf '%s' "$resp" | jq '.errors' >&2
  die "o token não consegue listar bancos D1 — confira a permissão 'Account · D1 · Edit'"
fi
ok "token válido, com acesso a D1"

# --- 2. criar (ou reaproveitar) o banco ------------------------------------
existing="$(cf GET "/d1/database?name=$DB_NAME" | jq -r --arg n "$DB_NAME" \
  '.result[]? | select(.name==$n) | .uuid' | head -1)"

if [ -n "$existing" ]; then
  DB_ID="$existing"
  ok "banco '$DB_NAME' já existe: $DB_ID"
else
  info "criando o banco '$DB_NAME'"
  resp="$(cf POST "/d1/database" "$(jq -nc --arg n "$DB_NAME" '{name:$n}')")"
  assert_ok "$resp" "criação do banco"
  DB_ID="$(printf '%s' "$resp" | jq -r '.result.uuid')"
  ok "banco criado: $DB_ID"
fi

# --- 3. persistir o database id no arquivo de env --------------------------
if ! grep -q '^D1_DATABASE_ID=' "$ENV_FILE" 2>/dev/null; then
  printf 'D1_DATABASE_ID=%s\n' "$DB_ID" >> "$ENV_FILE"
  ok "D1_DATABASE_ID gravado em $ENV_FILE"
else
  sed -i "s|^D1_DATABASE_ID=.*|D1_DATABASE_ID=$DB_ID|" "$ENV_FILE"
  ok "D1_DATABASE_ID atualizado em $ENV_FILE"
fi
export D1_DATABASE_ID="$DB_ID"

# --- 4. smoke test do /raw (o critério de aceite do SPIKE-1) ---------------
info "smoke test: POST /d1/database/\$id/raw"
resp="$(d1_raw "SELECT 1 AS um, 'kine' AS quem, ? AS eco" '["ping"]')"
assert_ok "$resp" "query /raw"

echo
printf '%s' "$resp" | jq '{
  columns: .result[0].results.columns,
  rows:    .result[0].results.rows,
  meta:    (.result[0].meta | {duration, rows_read, rows_written, served_by_region, served_by_primary})
}'
echo

got="$(printf '%s' "$resp" | jq -r '.result[0].results.rows[0][2]')"
[ "$got" = "ping" ] || die "bound param não fez round-trip: esperado 'ping', veio '$got'"
ok "/raw respondeu 200 e o bound param fez round-trip"

# --- 5. informações do banco, para referência ------------------------------
info "metadados do banco"
cf GET "/d1/database/$DB_ID" | jq '.result | {name, uuid, version, file_size, num_tables, read_replication}'

echo
ok "SPIKE-1 concluído — banco '$DB_NAME' pronto para os demais spikes"
