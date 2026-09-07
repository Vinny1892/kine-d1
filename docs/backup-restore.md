# Backup and restore

Atlas M0 has **no automatic backups** — that is one of the free tier's limits.
Backups are on you, and this is the runbook.

> Exercised end to end against a real cluster. See [Validation](#validation).

## What has to be saved

Two collections, and both matter:

| Collection | Contents | If lost |
|---|---|---|
| `kine` | the revision log — all cluster state | you lose the cluster |
| `kine_meta` | the **epoch base** and compacted revision | restored revisions come to mean something else |

`kine_meta` is small and easy to forget, and losing it is worse than it looks:
the epoch base defines the translation between `clusterTime` and etcd revisions
([ADR-0001](adr/0001-revision-from-clustertime.md)). Restoring `kine` with a
different epoch base hands the apiserver revisions that correspond to nothing.

## Backup

```bash
# adjust the database if you used kine_database
mongodump --uri="$MONGO_URI" --db=kine --out=./backup-$(date +%Y%m%d-%H%M)
```

That saves both collections. To check:

```bash
ls backup-*/kine/
# kine.bson  kine.metadata.json  kine_meta.bson  kine_meta.metadata.json
```

### With the cluster running

`mongodump` does not freeze the database, so the dump is an inconsistent
snapshot: it may contain a revision without its predecessor. For kine that is
tolerable — the log is append-only and the apiserver relists when it finds a
gap — but if you want an exact point, **stop kine first**:

```bash
sudo systemctl stop k3s kine
mongodump --uri="$MONGO_URI" --db=kine --out=./backup-clean
sudo systemctl start kine k3s
```

## Restore

**Restoring over a running cluster corrupts state.** Stop everything first.

```bash
sudo systemctl stop k3s kine

# --drop removes the collections before restoring; without it the restore
# merges with whatever is there and you end up with two histories interleaved
mongorestore --uri="$MONGO_URI" --drop --db=kine ./backup-20260907-1430/kine

sudo systemctl start kine
sudo systemctl start k3s
```

Check the epoch base in kine's log — it must match what it was before:

```
MongoDB ready: database=kine collection=kine epochBase=1767225600
```

If the value differs, `kine_meta` was not restored. Stop, restore the
collection, and start over — do not let the cluster come up like that.

## Migrating to another MongoDB cluster

Same procedure, different destination URI. The epoch base travels inside
`kine_meta`, so revisions stay valid.

```bash
mongodump    --uri="$SOURCE"      --db=kine --out=./migration
mongorestore --uri="$DESTINATION" --drop --db=kine ./migration/kine
```

What does **not** work is copying only the `kine` collection. The destination
would generate a fresh epoch base on first run, and existing revisions would
decode to a different instant.

## What the backup does not cover

- **k3s certificates and tokens.** They live in
  `/var/lib/rancher/k3s/server/tls` and in the datastore itself (k3s stores its
  bootstrap there). A MongoDB restore brings back the bootstrap; the node's
  local files, no.
- **Pod volumes.** `local-path-provisioner` writes to disk, outside MongoDB.

## Validation

The procedure was exercised against a real cluster: dump, drop both
collections, restore with `--drop`, and the cluster came back with the same
epoch base, keys intact and the watch working. Covered by `TestBackupRestore`
in `pkg/drivers/mongo/` — it requires `mongodump`/`mongorestore` on `PATH` and
skips if they are missing.
