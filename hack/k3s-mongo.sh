#!/usr/bin/env bash
# MT-3 — sobe um k3s real com o backend MongoDB.
#
# Arquitetura: o k3s NÃO usa o kine embutido dele. Rodamos o nosso kine
# standalone on 127.0.0.1:2399 and point k3s at it; k3s's `remote` driver
# (pkg/drivers/remote/remote.go) treats that address as an external etcd.
# So what is under test is our backend.
#
#   kube-apiserver -> k3s (driver remote) -> nosso kine -> MongoDB Atlas
#
# Usage:  sudo ./hack/k3s-mongo.sh
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

[ "$(id -u)" -eq 0 ] || die "run with sudo — o k3s precisa de root"

# A k3s survivor from a previous run holds the lock and port 6443, and the
# resulting failure ("runtime core not ready") does not point at the cause.
if pgrep -f "k3s server" >/dev/null 2>&1; then
  die "a k3s is already running - tear it down with: sudo systemctl stop k3s; sudo k3s-killall.sh"
fi

if [ -z "${MONGO_URI:-}" ] && [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
fi
[ -n "${MONGO_URI:-}" ] || die "MONGO_URI is not set (nor in $ENV_FILE)"

mkdir -p "$LOG_DIR"
[ -n "${SUDO_USER:-}" ] && chown "$SUDO_USER" "$LOG_DIR"

# --- 1. compila o kine com o driver mongo ---------------------------------
# find_go locates the compiler. sudo resets PATH (secure_path), and
# version managers like mise or asdf install go inside the user's home
# home - for root, `go` simply does not exist. mise's shims
# do not help either: they are links to mise itself, which depends on a shell
# hook. Only the absolute install path works under sudo.
# Requires the go.mod version: a mise with several versions installed usually
# has an old one first in glob order, and building with it fails.
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

  # pick the highest version satisfying go.mod
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

info "building kine"
GOBIN_REAL="$(achar_go)"
[ -n "$GOBIN_REAL" ] || die "found no go satisfying go.mod ($(awk '/^go /{print $2; exit}' go.mod)) — export GO=/path/to/go"
info "using $GOBIN_REAL ($("$GOBIN_REAL" version 2>/dev/null | awk '{print $3}'))"

# Build as the original user, to avoid leaving root-owned artifacts in the
# repository or the module cache.
if [ -n "${SUDO_USER:-}" ]; then
  sudo -u "$SUDO_USER" env HOME="/home/$SUDO_USER" \
    "$GOBIN_REAL" build -C "$RAIZ" -o "$LOG_DIR/kine" . || die "build failed"
else
  "$GOBIN_REAL" build -C "$RAIZ" -o "$LOG_DIR/kine" . || die "build failed"
fi
ok "binary at $LOG_DIR/kine"

# --- 2. sobe o kine --------------------------------------------------------
info "starting kine against MongoDB"
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
  tail -20 "$LOG_DIR/kine.log"; die "kine did not start"
}
ok "kine listening on $KINE_ADDR"

# --- 3. instala o k3s, se preciso -----------------------------------------
if ! command -v k3s >/dev/null 2>&1; then
  info "installing k3s (INSTALL_K3S_SKIP_START to configure before starting)"
  curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true sh - \
    || die "k3s install failed"
fi
ok "k3s $(k3s --version 2>/dev/null | head -1)"

# --- 4. sobe o k3s apontando para o nosso kine ----------------------------
info "starting k3s with --datastore-endpoint=http://$KINE_ADDR"
k3s server \
  --datastore-endpoint="http://$KINE_ADDR" \
  --disable traefik --disable servicelb --disable metrics-server \
  --kubelet-arg=fail-swap-on=false \
  --write-kubeconfig-mode 644 \
  > "$LOG_DIR/k3s.log" 2>&1 &
K3S_PID=$!
# k3s re-execs and starts containerd and pods as separate processes, so
# a kill on the original PID leaves orphans - which hold
# /var/lib/rancher/k3s/data/.lock and port 6443, blocking any subsequent attempt
# at "runtime core not ready". k3s-killall.sh is the only reliable
# way to bring everything down.
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
info "esperando o node ficar Ready (pode levar alguns minutos)"
pronto=""
for _ in $(seq 1 180); do
  if k3s kubectl get nodes 2>/dev/null | grep -q " Ready"; then pronto=1; break; fi
  sleep 2
done
[ -n "$pronto" ] || { tail -40 "$LOG_DIR/k3s.log"; die "the node never became Ready"; }
ok "node Ready"

# --- 5. validation: the MT-3 acceptance criteria -------------------------
info "nodes e pods do sistema"
k3s kubectl get nodes -o wide
k3s kubectl get pods -A

info "deploy + rolling update"
k3s kubectl create deployment teste --image=rancher/mirrored-pause:3.6 --replicas=2
k3s kubectl rollout status deployment/teste --timeout=180s
k3s kubectl scale deployment/teste --replicas=4
k3s kubectl rollout status deployment/teste --timeout=180s
k3s kubectl set image deployment/teste mirrored-pause=rancher/mirrored-pause:3.9
k3s kubectl rollout status deployment/teste --timeout=180s
ok "deployment scaled and rolled out"

info "final state"
k3s kubectl get deployment teste
k3s kubectl get events -A --sort-by=.lastTimestamp | tail -10

info "what MongoDB stored"
k3s kubectl get --raw /metrics >/dev/null 2>&1 || true
grep -c "" "$LOG_DIR/kine.log" | xargs -I{} echo "  lines of kine log: {}"

echo
ok "MT-3 finished. Logs in $LOG_DIR/"
echo "  to clean up:  sudo k3s-uninstall.sh"
