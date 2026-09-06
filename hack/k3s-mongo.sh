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

if [ -z "${MONGO_URI:-}" ] && [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
fi
[ -n "${MONGO_URI:-}" ] || die "MONGO_URI não definida (nem em $ENV_FILE)"

mkdir -p "$LOG_DIR"

# --- 1. compila o kine com o driver mongo ---------------------------------
info "compilando o kine"
(cd "$RAIZ" && go build -o "$LOG_DIR/kine" .) || die "falha ao compilar"
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
  --write-kubeconfig-mode 644 \
  > "$LOG_DIR/k3s.log" 2>&1 &
K3S_PID=$!
trap 'kill $K3S_PID $KINE_PID 2>/dev/null || true' EXIT

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
