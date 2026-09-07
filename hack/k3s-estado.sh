#!/usr/bin/env bash
# Coleta o estado do k3s num só lugar, incluindo o que exige root.
# Uso:  sudo ./hack/k3s-estado.sh
set -uo pipefail
titulo() { printf '\n\033[36m=== %s ===\033[0m\n' "$*"; }

titulo "serviço"
systemctl is-active k3s 2>&1
systemctl show k3s -p ActiveState,SubState,ExecMainPID 2>&1

titulo "processos"
ps -o pid,stat,etime,rss,comm -p "$(pgrep -f 'k3s server|containerd' 2>/dev/null | tr '\n' ',' | sed 's/,$//')" 2>/dev/null || echo "nenhum"

titulo "containerd do k3s — está no ar?"
ls -la /run/k3s/containerd/containerd.sock 2>&1
crictl --runtime-endpoint unix:///run/k3s/containerd/containerd.sock version 2>&1 | head -4

titulo "log do containerd (últimas 25)"
tail -25 /var/lib/rancher/k3s/agent/containerd/containerd.log 2>&1 | cut -c1-200

titulo "onde o k3s parou (últimas 20 do journal)"
journalctl -u k3s --no-pager -n 20 -o cat 2>&1 | cut -c1-200

titulo "o k3s está preso em quê? (stack das threads)"
PID="$(pgrep -f 'k3s server' | head -1)"
if [ -n "$PID" ]; then
  echo "PID $PID"
  cat "/proc/$PID/status" 2>/dev/null | grep -E "^(State|Threads)"
  # goroutines travadas aparecem no wchan de cada thread
  for t in /proc/$PID/task/*/; do
    printf '  %s %s\n' "$(basename "$t")" "$(cat "$t/wchan" 2>/dev/null)"
  done | sort -k2 | uniq -c -f1 | sort -rn | head -8
fi

titulo "cgroup delegation (o kubelet precisa disso no WSL)"
cat /sys/fs/cgroup/cgroup.subtree_control 2>&1
echo "systemd delegate: $(systemctl show k3s -p Delegate 2>&1)"
