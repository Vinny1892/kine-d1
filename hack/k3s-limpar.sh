#!/usr/bin/env bash
# Tears down EVERYTHING k3s, including orphans k3s-killall.sh misses.
#
# k3s-killall.sh handles what systemd started and the containers, but
# a hand-started `k3s server` (from a script, say) survives it -
# and keeps holding /var/lib/rancher/k3s/data/.lock and port 6443. The next
# attempt then hangs with no clear error, stopping at "Module br_netfilter was
# already loaded" with the supervisor returning "runtime core not ready".
#
# Usage:  sudo ./hack/k3s-limpar.sh
set -uo pipefail

info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }

[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }

info "stopping the service"
systemctl stop k3s 2>/dev/null || true

info "k3s-killall.sh (containers and child processes)"
command -v k3s-killall.sh >/dev/null 2>&1 && k3s-killall.sh >/dev/null 2>&1 || true

info "killing any remaining 'k3s server'"
for tentativa in TERM TERM KILL; do
  pids="$(pgrep -f 'k3s server' 2>/dev/null | tr '\n' ' ')"
  [ -z "${pids// /}" ] && break
  echo "  sending SIG$tentativa para: $pids"
  # shellcheck disable=SC2086
  kill -$tentativa $pids 2>/dev/null || true
  sleep 3
done

info "killing k3s containerd, if any is left"
pkill -9 -f 'containerd.*rancher/k3s' 2>/dev/null || true

# The service can get stuck in deactivating; reset-failed clears the state.
systemctl reset-failed k3s 2>/dev/null || true

echo
if pgrep -f 'k3s server' >/dev/null 2>&1; then
  echo "  processes are still alive:"
  ps -o pid,stat,etime,comm -p "$(pgrep -f 'k3s server' | tr '\n' ',' | sed 's/,$//')" 2>/dev/null
  exit 1
fi
ok "no k3s running"
ok "service state: $(systemctl is-active k3s 2>/dev/null || echo inactive)"
