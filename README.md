Kine (Kine is not etcd)
=======================

Kine is an etcdshim that translates etcd API to:
- SQLite
- Postgres
- MySQL/MariaDB
- NATS
- MongoDB

## MongoDB backend

This fork adds a **MongoDB** backend to kine, so you can run a Kubernetes
cluster (k3s) with MongoDB as the datastore instead of etcd.

```bash
kine --endpoint "mongodb+srv://user:password@cluster.example.mongodb.net/"
```

Requires MongoDB as a **replica set** — Atlas M0 (free tier) works. Change
streams and transactions need it; a standalone server will not do.

- **[Tutorial: running k3s on MongoDB](docs/k3s-tutorial.md)** — start here
- [Reference](docs/mongodb.md) — DSN options, what was measured, limits
- [When not to use it](docs/mongodb.md#when-not-to-use-it) — read before
  taking this seriously
- [Backup and restore](docs/backup-restore.md) — Atlas M0 has no automatic
  backups

Validated against a real k3s cluster: node `Ready`, deployment, scale, rolling
update, `kubectl exec` and `logs`. Not for critical production — see the link
above.

## Features
- Can be ran standalone so any k8s (not just K3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update, Delete)

See an [example](/examples/minimal.md).

## Developer Documentation

A high level flow diagram and overview of code structure is available at [docs/flow.md](/docs/flow.md).

MongoDB backend design decisions are recorded as ADRs in [docs/adr/](/docs/adr/).
