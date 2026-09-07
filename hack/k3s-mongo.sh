#!/usr/bin/env bash
# MT-3 — sobe um k3s real com o backend MongoDB.
#
# Arquitetura: o k3s NÃO usa o kine embutido dele. Rodamos o nosso kine
# standalone em 127.0.0.1:2399 e apontamos o k3s para lá; o driver `remote`
# do k3s (pkg/drivers/remote/remote.go) trata esse endereço como se fosse um
# etcd externo. Assim o que está sob teste é o nosso backend.
#
#   kube-apiserver -> k3s (driver remote) -> nosso kine -> MongoDB Atlas
#
# Uso:  sudo ./hack/k3s-mongo.sh
#
# Requer MONGO_URI no ambiente ou em ~/.config/kine-mongo/env.

set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${KINE_MONGO_ENV:-${SUDO_USER:+/home/$SUDO_USER}/.config/kine-mongo/env}"
KINE_ADDR="127.0.0.1:2399"
LOG_DIR="${LOG_DIR:-/tmp/kine-mongo-k3s}"

info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[31merro:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "rode com sudo — o k3s precisa de root"

# Um k3s sobrevivente de execução anterior segura o lock e a porta 6443, e a
# falha resultante ("runtime core not ready") não aponta para a causa.
if pgrep -f "k3s server" >/dev/null 2>&1; then
  die "já existe um k3s rodando — derrube com: sudo systemctl stop k3s; sudo k3s-killall.sh"
fi

if [ -z "${MONGO_URI:-}" ] && [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
fi
[ -n "${MONGO_URI:-}" ] || die "MONGO_URI não definida (nem em $ENV_FILE)"

mkdir -p "$LOG_DIR"
[ -n "${SUDO_USER:-}" ] && chown "$SUDO_USER" "$LOG_DIR"

# --- 1. compila o kine com o driver mongo ---------------------------------
# achar_go localiza o compilador. O sudo reseta o PATH (secure_path), e
# gerenciadores de versão como mise ou asdf instalam o go dentro do home do
# usuário — para o root, `go` simplesmente não existe. Os shims do mise
# também não servem: são links para o próprio mise, que depende do hook de
# shell. Só o caminho absoluto da instalação funciona sob sudo.
# Requer a versão do go.mod: um mise com várias versões instaladas costuma
# ter uma antiga primeiro na ordem do glob, e compilar com ela falha.
achar_go() {
  local minima
  minima="$(awk '/^go /{print $2; exit}' "$RAIZ/go.mod" 2>/dev/null)"
  minima="${minima:-1.26}"

  local candidatos=() c
  [ -n "${GO:-}" ] && candidatos+=("$GO")
  command -v go >/dev/null 2>&1 && candidatos+=("$(command -v go)")
  local home_usr="${SUDO_USER:+/home/$SUDO_USER}"
  for c in "$home_usr"/.local/share/mise/installs/go/*/bin/go \
           "$home_usr"/.asdf/installs/golang/*/go/bin/go \
           /usr/local/go/bin/go /usr/lib/go/bin/go /snap/bin/go; do
    [ -x "$c" ] && candidatos+=("$c")
  done

  # escolhe a maior versão que satisfaça o go.mod
  local melhor="" melhor_v=""
  for c in "${candidatos[@]}"; do
    [ -x "$c" ] || continue
    local v
    v="$("$c" version 2>/dev/null | awk '{print $3}' | sed 's/^go//')"
    [ -n "$v" ] || continue
    # v >= minima ?
    [ "$(printf '%s\n%s\n' "$minima" "$v" | sort -V | head -1)" = "$minima" ] || continue
    if [ -z "$melhor_v" ] || \
       [ "$(printf '%s\n%s\n' "$melhor_v" "$v" | sort -V | tail -1)" = "$v" ]; then
      melhor="$c"; melhor_v="$v"
    fi
  done
  echo "$melhor"
}

info "compilando o kine"
GOBIN_REAL="$(achar_go)"
[ -n "$GOBIN_REAL" ] || die "não encontrei um go que satisfaça o go.mod ($(awk '/^go /{print $2; exit}' go.mod)) — exporte GO=/caminho/para/go"
info "usando $GOBIN_REAL ($("$GOBIN_REAL" version 2>/dev/null | awk '{print $3}'))"

# Compila como o usuário original para não deixar artefatos root-owned no
# repositório nem no cache de módulos.
if [ -n "${SUDO_USER:-}" ]; then
  sudo -u "$SUDO_USER" env HOME="/home/$SUDO_USER" \
    "$GOBIN_REAL" build -C "$RAIZ" -o "$LOG_DIR/kine" . || die "falha ao compilar"
else
  "$GOBIN_REAL" build -C "$RAIZ" -o "$LOG_DIR/kine" . || die "falha ao compilar"
fi
ok "binário em $LOG_DIR/kine"

# --- 2. sobe o kine --------------------------------------------------------
info "subindo o kine contra o MongoDB"
sep="?"; case "$MONGO_URI" in *\?*) sep="&";; esac
"$LOG_DIR/kine" \
  --endpoint "${MONGO_URI}${sep}kine_database=k3s" \
  --listen-address "$KINE_ADDR" \
  --metrics-bind-address 0 \
  > "$LOG_DIR/kine.log" 2>&1 &
KINE_PID=$!
trap 'kill $KINE_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 60); do
  grep -q "Kine available" "$LOG_DIR/kine.log" 2>/dev/null && break
  sleep 1
done
grep -q "Kine available" "$LOG_DIR/kine.log" || {
  tail -20 "$LOG_DIR/kine.log"; die "o kine não subiu"
}
ok "kine escutando em $KINE_ADDR"

# --- 3. instala o k3s, se preciso -----------------------------------------
if ! command -v k3s >/dev/null 2>&1; then
  info "instalando o k3s (INSTALL_K3S_SKIP_START para configurar antes de subir)"
  curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true sh - \
    || die "falha ao instalar o k3s"
fi
ok "k3s $(k3s --version 2>/dev/null | head -1)"

# --- 4. sobe o k3s apontando para o nosso kine ----------------------------
info "subindo o k3s com --datastore-endpoint=http://$KINE_ADDR"
k3s server \
  --datastore-endpoint="http://$KINE_ADDR" \
  --disable traefik --disable servicelb --disable metrics-server \
  --kubelet-arg=fail-swap-on=false \
  --write-kubeconfig-mode 644 \
  > "$LOG_DIR/k3s.log" 2>&1 &
K3S_PID=$!
# O k3s faz re-exec e sobe containerd e pods como processos separados, então
# um kill no PID original deixa órfãos — que seguram o
# /var/lib/rancher/k3s/data/.lock e a porta 6443, travando qualquer tentativa
# seguinte no "runtime core not ready". O k3s-killall.sh é o único jeito
# confiável de derrubar tudo.
limpar_tudo() {
  kill "$KINE_PID" 2>/dev/null || true
  if command -v k3s-killall.sh >/dev/null 2>&1; then
    k3s-killall.sh >/dev/null 2>&1 || true
  else
    kill "$K3S_PID" 2>/dev/null || true
  fi
}
trap limpar_tudo EXIT

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
info "esperando o nó ficar Ready (pode levar alguns minutos)"
pronto=""
for _ in $(seq 1 180); do
  if k3s kubectl get nodes 2>/dev/null | grep -q " Ready"; then pronto=1; break; fi
  sleep 2
done
[ -n "$pronto" ] || { tail -40 "$LOG_DIR/k3s.log"; die "o nó não ficou Ready"; }
ok "nó Ready"

# --- 5. validação: o critério de aceite do MT-3 ---------------------------
info "nós e pods do sistema"
k3s kubectl get nodes -o wide
k3s kubectl get pods -A

info "deploy + rolling update"
k3s kubectl create deployment teste --image=rancher/mirrored-pause:3.6 --replicas=2
k3s kubectl rollout status deployment/teste --timeout=180s
k3s kubectl scale deployment/teste --replicas=4
k3s kubectl rollout status deployment/teste --timeout=180s
k3s kubectl set image deployment/teste mirrored-pause=rancher/mirrored-pause:3.9
k3s kubectl rollout status deployment/teste --timeout=180s
ok "deployment escalou e fez rolling update"

info "estado final"
k3s kubectl get deployment teste
k3s kubectl get events -A --sort-by=.lastTimestamp | tail -10

info "o que o MongoDB guardou"
k3s kubectl get --raw /metrics >/dev/null 2>&1 || true
grep -c "" "$LOG_DIR/kine.log" | xargs -I{} echo "  linhas de log do kine: {}"

echo
ok "MT-3 concluído. Logs em $LOG_DIR/"
echo "  para limpar:  sudo k3s-uninstall.sh"
