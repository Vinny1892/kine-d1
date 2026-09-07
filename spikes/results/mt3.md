# MT-3 — k3s real sobre o backend MongoDB

**Status:** ✅ **concluído** · **Data:** 2026-09-07

## Contexto

A primeira tentativa (feita pelo Codex, registrada em `CLAUDE.md`) falhou: o apiserver travava em `autoregister-completion` e, depois de corrigido, o scheduler e o controller-manager perdiam a eleição de líder sob a carga de bootstrap, derrubando o k3s.

A causa da segunda falha era a correção da primeira. Ver abaixo.

## O bug de ordenação, e por que a primeira correção custava caro

Cada registro é gravado em duas operações: `InsertOne`, que fornece o `clusterTime` usado como revisão, e `UpdateByID`, que grava esse valor no campo `rev`.

O `watchLoop` original descartava o evento do insert (`if r.Rev == 0 { continue }`) e esperava o evento do **update**. Sob concorrência, os inserts saem na ordem A/B mas os updates podem sair B/A — o watch avançava o corte com a revisão maior e descartava a menor como já entregue. Eventos sumiam.

A primeira correção serializou o pipeline com dois mutexes (`insertMu`, `revisionMu`). Preservava a ordem, mas criava fila: sob a carga de bootstrap do k3s, a espera ultrapassava os deadlines de 5-10 s da leader election.

## A correção adotada: escutar apenas inserts

O watch não precisa do campo `rev`. O evento de insert já carrega a revisão — é o `clusterTime` dele, o mesmo valor que a escrita grava — e o oplog entrega os eventos nessa ordem por construção (medido no [MSPIKE-4](mspike-4.md): zero eventos fora de ordem em 40).

```go
// antes: {"operationType": {"$in": ["insert","update","replace"]}} + fullDocument.rev
// agora: {"operationType": "insert"}          + EncodeRevision(ev.clusterTime)
```

Isso funciona porque o kine é um log append-only: toda mutação — criar, atualizar, apagar — insere um documento novo. O único update existente é o que grava `rev`, e ele não representa mutação nenhuma.

Com isso os dois mutexes saíram. O `UpdateByID` deixa de estar no caminho crítico da ordenação e passa a servir só às consultas históricas (`After`, `List`, `Get` por revisão), que leem depois do fato.

**Ganho colateral:** a ordem passa a vir do oplog, que é global ao cluster MongoDB. A limitação registrada antes — "os mutexes coordenam apenas dentro de uma instância do kine" — deixa de existir: múltiplas instâncias compartilham o mesmo oplog e portanto a mesma ordem.

## Resultado

Ambiente: kine em container (`kine-mongo:fix-order`) contra o Atlas M0, k3s `rancher/k3s:latest` na mesma rede Docker, apontado por `--datastore-endpoint=http://kine-mongo-mt3:2379` (driver `remote`).

| Verificação | Resultado |
|---|---|
| `readyz` do apiserver | ✅ **ok** em ~95 s |
| `autoregister-completion` | ✅ **ok** (era onde travava) |
| Lease do `kube-controller-manager` | ✅ adquirida |
| Lease do `kube-scheduler` | ✅ adquirida |
| Namespace + Deployment | ✅ criados |
| ReplicaSet e Pods pelo controller-manager | ✅ 2 pods |
| Scale 2 → 4 | ✅ 4 pods |
| Rolling update 3.9 → 3.6 | ✅ novo ReplicaSet |
| **Leader election sob carga (60 configmaps)** | ✅ **holders inalterados** |
| k3s vivo ao fim | ✅ |

O `readyz?verbose` passou em todos os 13 hooks.

## O que NÃO foi validado

**Não há nó `Ready`.** O k3s subiu com `--disable-agent`, então os pods ficam `Pending` — não há kubelet para agendá-los. O critério de aceite do MT-3 pede nó `Ready` e rolling update concluído de fato.

A limitação é do ambiente, não do backend: kubelet e containerd aninhados em container privilegiado sob Docker Desktop/WSL não ficaram operacionais, e a alternativa com `--docker` esbarra no cri-dockerd esperando `/var/lib/docker` dentro do container. O diagnóstico é do Codex e continua válido.

**Portanto o MT-3 continua aberto.** O que está provado é que o plano de controle inteiro — apiserver, scheduler, controller-manager, watch cache, leader election — funciona sobre o backend MongoDB e sobrevive a carga. Falta o plano de dados, e isso pede uma VM Linux com k3s instalado normalmente.

## Reproduzir

```bash
docker build --target package -t kine-mongo:fix-order .
docker run -d --name kine-mongo-mt3 --network kine-k3s-test-net kine-mongo:fix-order \
  --endpoint "$MONGO_URI&kine_database=k3s_mt3" --listen-address 0.0.0.0:2379 --metrics-bind-address 0
docker run -d --name k3s-mongo-mt3 --network kine-k3s-test-net --privileged \
  --tmpfs /run --tmpfs /var/run -e K3S_TOKEN=mt3token rancher/k3s:latest server \
  --datastore-endpoint="http://kine-mongo-mt3:2379" --disable-agent \
  --disable traefik --disable servicelb --disable metrics-server --disable-cloud-controller
docker exec k3s-mongo-mt3 kubectl get --raw=/readyz
```


---

# MT-3 concluído — k3s completo em EC2

**Data:** 2026-09-07 · **Ambiente:** EC2 t3.small, Ubuntu 24.04, kernel 7.0.0-1012-aws, sa-east-1

## Por que precisou sair do WSL

As três tentativas no WSL2 travaram sempre no mesmo ponto, e a causa não era o backend:

```
81339  D  19:03  modprobe -- iptable_nat     <- travado no kernel
88567  S   6:21  modprobe -- iptable_nat
89687  S   2:16  modprobe -- iptable_nat
```

O primeiro `modprobe` ficou em estado **`D`** (uninterruptible sleep), travado dentro do kernel do WSL carregando `iptable_nat`. Processo em `D` não morre nem com `kill -9`, e o carregamento de módulos é serializado no kernel — então todo `modprobe` seguinte ficava preso atrás dele, e o k3s ficava em `do_wait` esperando o filho que nunca retornava. O containerd nunca subia, e o log parava em "Module br_netfilter was already loaded" sem erro.

Durante todas essas tentativas o kine recebeu **duas** chamadas (`LIST /bootstrap`), respondeu `count=0` corretamente em milissegundos e ficou ocioso. O MongoDB nunca foi exercitado.

Na EC2 o mesmo módulo carrega instantaneamente.

## Resultado

| Critério de aceite | Resultado |
|---|---|
| Nó `Ready` | ✅ **em ~4 s** |
| `coredns` e `local-path-provisioner` | ✅ Running |
| Deployment com pods reais (nginx) | ✅ 4/4 Running |
| Scale 2 → 4 | ✅ |
| Rolling update (alpine → 1.27-alpine) | ✅ |
| `kubectl exec` | ✅ `nginx/1.27.5` |
| `kubectl logs` | ✅ |
| Leader election estável | ✅ holders inalterados |

Leases adquiridas: `apiserver`, `k3s`, `k3s-cloud-controller-manager`, `kube-controller-manager`, `kube-scheduler`.

## O backend sob um cluster de verdade

```
operações atendidas:   4.866 WATCH · 82 LIST · 4 DELETE
erros no kine:         0
memória da máquina:    847 MB de 1.906 MB (kine + k3s + containerd + pods)
```

**Zero erros.** O volume esmagadoramente de watch confirma o desenho: com Change Streams, o custo do watch é o stream único, não uma query por segundo por watcher.

## Tamanho de um cluster k3s no MongoDB

```
842 documentos · 417 chaves distintas
dataSize 1.729.056 bytes · storage + índices 1.052.672 bytes
```

| Prefixo | Chaves |
|---|---|
| `/registry/events` | 108 |
| `/registry/clusterroles` | 74 |
| `/registry/clusterrolebindings` | 57 |
| `/registry/serviceaccounts` | 43 |
| `/registry/apiregistration.k8s.io` | 23 |

**Um cluster k3s inteiro ocupa ~1 MB.** Projetando os 512 MB do M0: **~429 mil documentos** — cerca de 500 clusters deste tamanho. O teto de storage, que era o gargalo mais provável, é muito mais folgado do que a estimativa do MSPIKE-7 sugeria (que usava objetos de 6 KB; os objetos reais do k3s são bem menores).

## Reproduzir

`hack/ec2-mt3.sh` provisiona, roda e destrói. Ver o script para a região usada.
