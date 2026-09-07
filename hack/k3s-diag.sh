#!/usr/bin/env bash
# Sobe o k3s como serviço systemd apontando para um kine que já esteja
# rodando em 127.0.0.1:2399, para diagnosticar onde o bootstrap trava.
#
# Diferente do k3s-mongo.sh, este script NÃO sobe o kine e NÃO mata nada no
# fim: o cluster fica de pé para inspeção.
#
# Uso:  sudo ./hack/k3s-diag.sh
set -euo pipefail

KINE_ADDR="${KINE_ADDR:-127.0.0.1:2399}"

info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[31merro:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "rode com sudo"
command -v k3s >/dev/null 2>&1 || die "k3s não está instalado — rode antes o hack/k3s-mongo.sh"

info "verificando se o kine responde em $KINE_ADDR"
timeout 5 bash -c "</dev/tcp/${KINE_ADDR/:/\/}" 2>/dev/null \
  || die "nada escutando em $KINE_ADDR — suba o kine primeiro"
ok "kine acessível"

info "escrevendo /etc/rancher/k3s/config.yaml"
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
ok "config.yaml escrito"

info "reiniciando o k3s pelo systemd"
systemctl daemon-reload
systemctl restart k3s
ok "k3s iniciado — não vou esperar nem matar nada"

cat <<'FIM'

O cluster ficou de pé. Comandos úteis:

  sudo systemctl status k3s --no-pager | head -20
  sudo journalctl -u k3s -f                 # log ao vivo do k3s
  sudo journalctl -u k3s --no-pager | tail -40
  sudo k3s kubectl get nodes
  sudo k3s kubectl get pods -A

Para derrubar:
  sudo systemctl stop k3s
  sudo k3s-killall.sh                       # mata também containerd e pods

FIM
