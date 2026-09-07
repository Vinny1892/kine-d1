#!/usr/bin/env bash
# MT-3 - provisions an EC2 instance, runs the k3s test, tears it down.
#
# Why EC2 and not WSL: no WSL2 o `modprobe iptable_nat` trava em estado D
# (uninterruptible), and since module loading is serialized in the kernel,
# every subsequent modprobe queues behind it. k3s waits on that child
# sempre, o containerd nunca sobe, and none of it produces an error in the log. See
# spikes/results/mt3.md.
#
# REGION: us-east-1, the cheapest (t3.small ~US$ 0.021/h against
# ~US$ 0.042/h in sa-east-1). The important caveat: latency to an
# Atlas in São Paulo goes from ~5 ms to ~120 ms. For validating functionality
# that changes nothing; to measure load or leader election under pressure, use
# the same region as the Atlas cluster, or the test measures distance, not the driver.
#
# Usage:
#   ./hack/ec2-mt3.sh criar      # provision and run the test
#   ./hack/ec2-mt3.sh destruir   # remove instance, SG and key pair
set -euo pipefail

REGIAO="${REGIAO:-us-east-1}"
TIPO="${TIPO:-t3.small}"      # 1 GB (t3.micro) risks OOM during bootstrap
NOME="${NOME:-kine-mongo-mt3}"
PERFIL="${AWS_PROFILE:-personal}"
CFG="${CFG:-$HOME/.config/kine-mongo}"
RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
BANCO="${BANCO:-k3s_ec2}"

export AWS_PROFILE="$PERFIL" AWS_DEFAULT_REGION="$REGIAO"

info() { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[31merro:\033[0m %s\n' "$*" >&2; exit 1; }

carregar_env() {
  [ -f "$CFG/env" ] || die "$CFG/env does not exist"
  while IFS='=' read -r k v; do
    case "$k" in ''|\#*) continue;; esac
    v="${v%\'}"; v="${v#\'}"; v="${v%\"}"; v="${v#\"}"
    export "$k=$v"
  done < "$CFG/env"
  [ -n "${MONGO_URI:-}" ] || die "MONGO_URI not set in $CFG/env"
}

destruir() {
  info "removing instances tagged Name=$NOME"
  local ids
  ids=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$NOME" "Name=instance-state-name,Values=pending,running,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || true)
  if [ -n "${ids// /}" ]; then
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $ids >/dev/null
    ok "terminating: $ids"
    # shellcheck disable=SC2086
    aws ec2 wait instance-terminated --instance-ids $ids 2>/dev/null || true
  else
    ok "no instances"
  fi
  # the SG can only be removed once the instance ENI is gone
  local sg
  sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$NOME" \
       --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo None)
  [ "$sg" != "None" ] && [ -n "$sg" ] && aws ec2 delete-security-group --group-id "$sg" 2>/dev/null \
    && ok "security group removed" || true
  aws ec2 delete-key-pair --key-name "$NOME" 2>/dev/null && ok "key pair removed" || true
  rm -f "$CFG/$NOME.pem" "$CFG/mt3-ip.txt" "$CFG/mt3-instance-id.txt"
  ok "cleaned up"
}

criar() {
  carregar_env
  command -v aws >/dev/null || die "aws cli not installed"

  local vpc subnet ami meu_ip sg
  vpc=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
  [ "$vpc" != "None" ] || die "no default VPC in $REGIAO"
  subnet=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true \
           --query 'Subnets[0].SubnetId' --output text)
  ami=$(aws ssm get-parameter \
        --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query Parameter.Value --output text)
  meu_ip=$(curl -s https://checkip.amazonaws.com)
  info "region=$REGIAO type=$TIPO ami=$ami"

  if [ ! -f "$CFG/$NOME.pem" ]; then
    aws ec2 create-key-pair --key-name "$NOME" --query KeyMaterial --output text > "$CFG/$NOME.pem"
    chmod 600 "$CFG/$NOME.pem"
    ok "key pair created"
  fi

  sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$NOME" \
       --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo None)
  if [ "$sg" = "None" ] || [ -z "$sg" ]; then
    sg=$(aws ec2 create-security-group --group-name "$NOME" --vpc-id "$vpc" \
         --description "SSH para o teste MT-3 do kine-mongo" --query GroupId --output text)
    aws ec2 authorize-security-group-ingress --group-id "$sg" \
      --protocol tcp --port 22 --cidr "$meu_ip/32" >/dev/null
    ok "security group $sg (22/tcp de $meu_ip/32)"
  fi

  local id
  id=$(aws ec2 run-instances --image-id "$ami" --instance-type "$TIPO" \
    --key-name "$NOME" --security-group-ids "$sg" --subnet-id "$subnet" \
    --associate-public-ip-address \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":20,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NOME},{Key=Projeto,Value=kine-mongo},{Key=Descartavel,Value=sim}]" \
    --query 'Instances[0].InstanceId' --output text)
  echo "$id" > "$CFG/mt3-instance-id.txt"
  ok "instance $id"

  aws ec2 wait instance-running --instance-ids "$id"
  local ip
  ip=$(aws ec2 describe-instances --instance-ids "$id" \
       --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  echo "$ip" > "$CFG/mt3-ip.txt"
  ok "IP $ip"

  until timeout 5 bash -c "</dev/tcp/$ip/22" 2>/dev/null; do sleep 5; done
  ok "SSH respondendo"

  info "building kine for linux/amd64"
  ( cd "$RAIZ" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/kine-linux . )
  scp -q -i "$CFG/$NOME.pem" -o StrictHostKeyChecking=accept-new /tmp/kine-linux "ubuntu@$ip:/tmp/kine"
  ok "binary uploaded"

  local sep="?"; case "$MONGO_URI" in *\?*) sep="&";; esac
  info "starting kine + k3s and running the test"
  ssh -i "$CFG/$NOME.pem" -o StrictHostKeyChecking=accept-new \
    "MONGO_URI=${MONGO_URI}${sep}kine_database=${BANCO}" "ubuntu@$ip" 'bash -s' <<'REMOTO'
set -e
sudo mkdir -p /var/log/kine /etc/rancher/k3s
sudo tee /etc/systemd/system/kine.service >/dev/null <<UNIT
[Unit]
Description=kine com backend MongoDB
After=network-online.target
[Service]
ExecStart=/tmp/kine --debug --endpoint '${MONGO_URI}' --listen-address 127.0.0.1:2399 --metrics-bind-address 0
Restart=no
StandardOutput=append:/var/log/kine/kine.log
StandardError=append:/var/log/kine/kine.log
[Install]
WantedBy=multi-user.target
UNIT
sudo tee /etc/rancher/k3s/config.yaml >/dev/null <<'YAML'
datastore-endpoint: "http://127.0.0.1:2399"
disable: [traefik, servicelb, metrics-server]
write-kubeconfig-mode: "644"
YAML
chmod +x /tmp/kine
sudo systemctl daemon-reload && sudo systemctl start kine
sleep 8
sudo grep -oE "Kine available.*" /var/log/kine/kine.log | tail -1

curl -sfL https://get.k3s.io | sh - >/dev/null 2>&1
K() { sudo k3s kubectl "$@"; }
for i in $(seq 1 90); do K get nodes --no-headers 2>/dev/null | grep -q " Ready" && break; sleep 4; done
echo "node: $(K get nodes --no-headers 2>/dev/null | awk '{print $2}')"

H0=$(K get lease kube-controller-manager -n kube-system -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
K create ns mt3 >/dev/null 2>&1
K -n mt3 create deployment web --image=nginx:alpine --replicas=2 >/dev/null 2>&1
K -n mt3 rollout status deployment/web --timeout=240s 2>&1 | tail -1
K -n mt3 scale deployment/web --replicas=4 >/dev/null 2>&1
K -n mt3 rollout status deployment/web --timeout=240s 2>&1 | tail -1
K -n mt3 set image deployment/web nginx=nginx:1.27-alpine >/dev/null 2>&1
K -n mt3 rollout status deployment/web --timeout=240s 2>&1 | tail -1
POD=$(K -n mt3 get pods -o jsonpath='{.items[0].metadata.name}')
echo "exec: $(K -n mt3 exec "$POD" -- nginx -v 2>&1 | head -1)"
H1=$(K get lease kube-controller-manager -n kube-system -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)
[ "$H0" = "$H1" ] && echo "leader election: held" || echo "leader election: LOST"
echo "kine errors: $(sudo grep -icE 'level=error|panic' /var/log/kine/kine.log)"
REMOTO
  echo
  ok "test finished - tear down with: $0 destruir"
  echo "  ssh -i $CFG/$NOME.pem ubuntu@$ip"
}

case "${1:-}" in
  criar)    criar ;;
  destruir) destruir ;;
  *) echo "uso: $0 {criar|destruir}"; exit 1 ;;
esac
