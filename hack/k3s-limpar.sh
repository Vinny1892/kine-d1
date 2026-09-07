#!/usr/bin/env bash
# Derruba TUDO do k3s, inclusive instâncias órfãs que o k3s-killall.sh não pega.
#
# O k3s-killall.sh cuida do que foi iniciado pelo systemd e dos containers, mas
# um `k3s server` disparado à mão (por um script, por exemplo) sobrevive a ele —
# e segue segurando /var/lib/rancher/k3s/data/.lock e a porta 6443. A próxima
# tentativa então trava sem erro claro, parando em "Module br_netfilter was
# already loaded" com o supervisor devolvendo "runtime core not ready".
#
# Uso:  sudo ./hack/k3s-limpar.sh
set -uo pipefail

info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }

[ "$(id -u)" -eq 0 ] || { echo "rode com sudo" >&2; exit 1; }

info "parando o serviço"
systemctl stop k3s 2>/dev/null || true

info "k3s-killall.sh (containers e processos filhos)"
command -v k3s-killall.sh >/dev/null 2>&1 && k3s-killall.sh >/dev/null 2>&1 || true

info "matando qualquer 'k3s server' remanescente"
for tentativa in TERM TERM KILL; do
  pids="$(pgrep -f 'k3s server' 2>/dev/null | tr '\n' ' ')"
  [ -z "${pids// /}" ] && break
  echo "  enviando SIG$tentativa para: $pids"
  # shellcheck disable=SC2086
  kill -$tentativa $pids 2>/dev/null || true
  sleep 3
done

info "matando containerd do k3s, se sobrou"
pkill -9 -f 'containerd.*rancher/k3s' 2>/dev/null || true

# O serviço pode ficar preso em deactivating; reset-failed limpa o estado.
systemctl reset-failed k3s 2>/dev/null || true

echo
if pgrep -f 'k3s server' >/dev/null 2>&1; then
  echo "  ainda há processos vivos:"
  ps -o pid,stat,etime,comm -p "$(pgrep -f 'k3s server' | tr '\n' ',' | sed 's/,$//')" 2>/dev/null
  exit 1
fi
ok "nenhum k3s rodando"
ok "estado do serviço: $(systemctl is-active k3s 2>/dev/null || echo inactive)"
