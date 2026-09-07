#!/usr/bin/env bash
# Starts k3s as a systemd service, pointing at a kine already
# running on 127.0.0.1:2399, to diagnose where the bootstrap hangs.
#
# Unlike k3s-mongo.sh, this script does NOT start kine and does NOT kill
# anything at the end: the cluster stays up for inspection.
#
# Usage:  sudo ./hack/k3s-diag.sh
set -euo pipefail

KINE_ADDR="${KINE_ADDR:-127.0.0.1:2399}"

info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[31merro:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run with sudo"
command -v k3s >/dev/null 2>&1 || die "k3s is not installed - run hack/k3s-mongo.sh"

# An orphaned k3s holds the lock and port 6443, and the resulting failure
# at the cause. k3s-killall.sh misses hand-started instances.
if pgrep -f "k3s server" >/dev/null 2>&1; then
  echo "  k3s processes already running:" >&2
  ps -o pid,stat,etime,comm -p "$(pgrep -f 'k3s server' | tr '\n' ',' | sed 's/,$//')" >&2 2>/dev/null
  die "tear everything down first: sudo ./hack/k3s-limpar.sh"
fi

info "checking whether kine answers on $KINE_ADDR"
timeout 5 bash -c "</dev/tcp/${KINE_ADDR/:/\/}" 2>/dev/null \
  || die "nothing listening on $KINE_ADDR — start kine first"
ok "kine reachable"

info "writing /etc/rancher/k3s/config.yaml"
mkdir -p /etc/rancher/k3s
cat > /etc/rancher/k3s/config.yaml <<YAML
datastore-endpoint: "http://${KINE_ADDR}"
kubelet-arg:
  - fail-swap-on=false
disable:
  - traefik
  - servicelb
  - metrics-server
write-kubeconfig-mode: "644"
YAML
ok "config.yaml written"

info "restarting k3s through systemd"
systemctl daemon-reload
systemctl restart k3s
ok "k3s started - not waiting, not killing anything"

cat <<'FIM'

The cluster is up. Useful commands:

  sudo systemctl status k3s --no-pager | head -20
  sudo journalctl -u k3s -f                 # live k3s log
  sudo journalctl -u k3s --no-pager | tail -40
  sudo k3s kubectl get nodes
  sudo k3s kubectl get pods -A

To tear down:
  sudo systemctl stop k3s
  sudo k3s-killall.sh                       # also kills containerd and pods

FIM
