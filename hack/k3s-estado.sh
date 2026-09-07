#!/usr/bin/env bash
# Collects k3s state in one place, including what requires root.
# Usage:  sudo ./hack/k3s-estado.sh
set -uo pipefail
titulo() { printf '\n\033[36m=== %s ===\033[0m\n' "$*"; }

titulo "service"
systemctl is-active k3s 2>&1
systemctl show k3s -p ActiveState,SubState,ExecMainPID 2>&1

titulo "processes"
ps -o pid,stat,etime,rss,comm -p "$(pgrep -f 'k3s server|containerd' 2>/dev/null | tr '\n' ',' | sed 's/,$//')" 2>/dev/null || echo "none"

titulo "containerd do k3s — up?"
ls -la /run/k3s/containerd/containerd.sock 2>&1
crictl --runtime-endpoint unix:///run/k3s/containerd/containerd.sock version 2>&1 | head -4

titulo "containerd log (last 25)"
tail -25 /var/lib/rancher/k3s/agent/containerd/containerd.log 2>&1 | cut -c1-200

titulo "where k3s stopped (last 20 journal lines)"
journalctl -u k3s --no-pager -n 20 -o cat 2>&1 | cut -c1-200

titulo "what is k3s stuck on? (thread stacks)"
PID="$(pgrep -f 'k3s server' | head -1)"
if [ -n "$PID" ]; then
  echo "PID $PID"
  cat "/proc/$PID/status" 2>/dev/null | grep -E "^(State|Threads)"
  # stuck goroutines show up in each thread's wchan
  for t in /proc/$PID/task/*/; do
    printf '  %s %s\n' "$(basename "$t")" "$(cat "$t/wchan" 2>/dev/null)"
  done | sort -k2 | uniq -c -f1 | sort -rn | head -8
fi

titulo "cgroup delegation (the kubelet needs this on WSL)"
cat /sys/fs/cgroup/cgroup.subtree_control 2>&1
echo "systemd delegate: $(systemctl show k3s -p Delegate 2>&1)"
