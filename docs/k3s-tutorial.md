# Tutorial: running k3s on MongoDB

End-to-end walkthrough: from an empty MongoDB to a k3s cluster with a running
workload. Takes about 15 minutes.

Two things worth knowing before you start:

- **k3s does not use this driver directly.** You run kine as a separate
  process, and point k3s at it as if it were an external etcd. k3s ships its
  own bundled kine, but pointing `--datastore-endpoint` at an `http://` address
  makes it use the `remote` driver and pass the connection straight through.
- **This does not work on WSL2.** `modprobe iptable_nat` hangs in `D` state
  inside the WSL kernel, immune to `kill -9`, and k3s waits on it forever
  without logging an error. Use a real Linux machine or a VM. See
  [Troubleshooting](#troubleshooting).

```
kube-apiserver → k3s (remote driver) → kine → MongoDB
```

---

## 1. Get a MongoDB replica set

Any replica set works. The free Atlas tier is the cheapest way to try this.

### Atlas (free)

1. Create an **M0** cluster at [cloud.mongodb.com](https://cloud.mongodb.com).
   Pick the region closest to where kine will run — write latency lands
   directly in the leader election path.
2. **Database Access** → add a user with `readWrite`.
3. **Network Access** → allow the IP where kine runs.
4. **Connect → Drivers** → copy the connection string.

### Local (Docker)

A single-node replica set is enough:

```bash
docker run -d --name mongo-kine -p 27017:27017 mongo:8 \
  mongod --replSet rs0 --bind_ip_all

docker exec mongo-kine mongosh --quiet --eval \
  'rs.initiate({_id:"rs0", members:[{_id:0, host:"localhost:27017"}]})'
```

Connection string: `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true`

> A standalone `mongod` will not work. Change streams and transactions require
> a replica set, and both are load-bearing in this driver's design.

---

## 2. Build kine

```bash
git clone https://github.com/Vinny1892/kine-mongo.git
cd kine-mongo
go build -o kine .
```

---

## 3. Store the credentials outside the repo

The connection string contains a password. Keep it out of shell history and
out of git:

```bash
mkdir -p ~/.config/kine-mongo
cat > ~/.config/kine-mongo/env <<'ENV'
MONGO_URI='mongodb+srv://user:password@cluster.example.mongodb.net/?retryWrites=true&w=majority'
ENV
chmod 600 ~/.config/kine-mongo/env
```

The single quotes matter: the URI contains `&`, which the shell would read as a
background operator when you `source` the file.

---

## 4. Start kine

```bash
set -a; source ~/.config/kine-mongo/env; set +a

./kine \
  --endpoint "${MONGO_URI}&kine_database=k3s" \
  --listen-address 127.0.0.1:2379 \
  --metrics-bind-address :8080
```

Use `?` instead of `&` before `kine_database` if your URI has no query string
yet.

You should see:

```
Configuring MongoDB collections and indexes…
Cluster epoch base set to 1767225600
MongoDB ready: database=k3s collection=kine epochBase=1767225600
MongoDB started: current revision=22563643195394, compacted revision=0
Kine available at http://127.0.0.1:2379
```

Two things in that output are worth understanding:

- **The revision is a huge number.** It is derived from MongoDB's `clusterTime`,
  not a counter — see [ADR-0001](adr/0001-revision-from-clustertime.md). Revisions
  are monotonic but not dense: they jump. The apiserver treats `resourceVersion`
  as an opaque value, so this is fine.
- **The epoch base is written once and never changes.** It defines how
  `clusterTime` maps to etcd revisions. kine refuses to start if the DSN asks
  for a different value than the one stored.

### As a systemd service

For anything beyond a quick test, run kine under systemd so it survives your
shell:

```bash
sudo tee /etc/systemd/system/kine.service >/dev/null <<UNIT
[Unit]
Description=kine with MongoDB backend
After=network-online.target

[Service]
ExecStart=/usr/local/bin/kine --endpoint '${MONGO_URI}&kine_database=k3s' \
  --listen-address 127.0.0.1:2379 --metrics-bind-address :8080
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now kine
```

---

## 5. Start k3s

Point k3s at kine over HTTP. Everything else is a normal k3s install.

```bash
sudo mkdir -p /etc/rancher/k3s
sudo tee /etc/rancher/k3s/config.yaml >/dev/null <<'YAML'
datastore-endpoint: "http://127.0.0.1:2379"
write-kubeconfig-mode: "644"
disable:
  - traefik
  - servicelb
  - metrics-server
YAML

curl -sfL https://get.k3s.io | sh -
```

Disabling those three add-ons is optional — it just keeps the first run light.

---

## 6. Verify

```bash
sudo k3s kubectl get nodes
```

```
NAME       STATUS   ROLES    AGE   VERSION
mynode     Ready    <none>   12s   v1.36.4+k3s1
```

Then run a real workload:

```bash
sudo k3s kubectl create namespace demo
sudo k3s kubectl -n demo create deployment web --image=nginx:alpine --replicas=2
sudo k3s kubectl -n demo rollout status deployment/web

sudo k3s kubectl -n demo scale deployment/web --replicas=4
sudo k3s kubectl -n demo set image deployment/web nginx=nginx:1.27-alpine
sudo k3s kubectl -n demo rollout status deployment/web

POD=$(sudo k3s kubectl -n demo get pods -o jsonpath='{.items[0].metadata.name}')
sudo k3s kubectl -n demo exec "$POD" -- nginx -v
sudo k3s kubectl -n demo logs "$POD" --tail=5
```

### See what landed in MongoDB

```javascript
use k3s
db.kine.countDocuments()                  // ~840 for a fresh cluster
db.kine.distinct("name").length           // ~420 distinct keys
db.kine_meta.findOne()                    // epoch base and compacted revision
```

A whole k3s cluster is about **1 MB**. On Atlas M0's 512 MB that leaves room
for roughly 429,000 documents.

---

## 7. What to watch

kine exposes Prometheus metrics on `--metrics-bind-address`. The MongoDB
backend adds its own, all prefixed `kine_mongo_`.

**Do not alert on error rate.** When you exceed Atlas M0's operation ceiling,
it does not return errors — it queues. Measured: zero errors at 6× the ceiling,
but write p99 went from 810 ms to **63 s**, and the change stream fell **44 s**
behind. Since leader election has a 10 s `RenewDeadline`, the cluster loses its
leader with nothing in the logs to explain why.

What actually catches it:

| Metric | Alert when |
|---|---|
| `kine_mongo_op_duration_seconds` (write p99) | above ~1 s — the cluster is heading toward losing leadership |
| `kine_mongo_change_stream_lag_seconds` | above a few seconds — informers are going stale |
| `kine_mongo_storage_bytes` | approaching your tier's limit — M0 does not grow, it stops |

Practical ceiling: **~30 cluster mutations/s**. And what hurts is
**concurrency**, not volume: one write at a time costs ~87 ms, twelve at once
cost ~884 ms.

---

## Troubleshooting

**`Kine available` never appears.** kine could not reach MongoDB. Check that
the IP is allowed in Atlas Network Access, and that the URI includes the
password.

**`epoch base conflicts with the stored value`.** The DSN asks for a
`kine_epoch_base` different from what is in `kine_meta`. Remove the parameter —
the stored value is the correct one. Changing it would invalidate every revision
already handed to the apiserver.

**k3s hangs after `Module br_netfilter was already loaded`, no error.** Two
known causes:

1. **A leftover k3s instance** holds `/var/lib/rancher/k3s/data/.lock` and port
   6443. `k3s-killall.sh` does not catch instances started by hand. Use
   `hack/k3s-limpar.sh`, which escalates `TERM → TERM → KILL` and verifies
   nothing survived.
2. **You are on WSL2.** `modprobe iptable_nat` hangs in `D` state — an
   uninterruptible kernel sleep that `kill -9` cannot touch. Module loading is
   serialized in the kernel, so every subsequent `modprobe` queues behind it and
   k3s waits on the child forever. There is no workaround; use a VM. The
   `hack/ec2-mt3.sh` script provisions one, runs the full test, and tears it
   down.

**Watch stops delivering events.** The change stream may have fallen outside
the oplog window (~4.4 h on M0). The driver detects `ChangeStreamHistoryLost`
and backfills by reading the collection directly, so this should self-heal —
check `kine_mongo_change_stream_reconnects_total{motivo="historico_perdido"}`.

**Everything got slow, no errors anywhere.** That is the signature of exceeding
the operation ceiling. Look at write p99, not error rate.

---

## Uninstalling

```bash
sudo /usr/local/bin/k3s-uninstall.sh
sudo systemctl disable --now kine
```

The cluster state stays in MongoDB. To wipe it:

```javascript
use k3s
db.dropDatabase()
```
