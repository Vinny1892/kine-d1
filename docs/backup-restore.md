# Backup e restore

O Atlas M0 **não tem backup automático** — é uma das limitações do free tier. O backup é seu, e este é o runbook.

> Testado ponta a ponta contra um cluster k3s real. Ver a seção [Validação](#validação).

## O que precisa ser salvo

Duas coleções, e as duas importam:

| Coleção | Conteúdo | Se perder |
|---|---|---|
| `kine` | o log de revisões — todo o estado do cluster | perde o cluster |
| `kine_meta` | o **epoch base** e a revisão compactada | as revisões restauradas passam a significar outra coisa |

O `kine_meta` é pequeno e fácil de esquecer, e perdê-lo é pior do que parece: o epoch base define a tradução entre `clusterTime` e revisão do etcd ([ADR-0001](adr/0001-revisao-por-clustertime.md)). Restaurar `kine` com um epoch base diferente entrega ao apiserver revisões que não correspondem a nada.

## Backup

```bash
# ajuste o banco se você usou kine_database
mongodump --uri="$MONGO_URI" --db=kine --out=./backup-$(date +%Y%m%d-%H%M)
```

Isso salva as duas coleções. Para conferir:

```bash
ls backup-*/kine/
# kine.bson  kine.metadata.json  kine_meta.bson  kine_meta.metadata.json
```

### Com o cluster no ar

O `mongodump` não congela o banco, então o dump é um instantâneo inconsistente: pode conter uma revisão sem a anterior. Para o kine isso é tolerável — o log é append-only e o apiserver relista ao encontrar uma lacuna — mas se você quer um ponto exato, **pare o kine antes**:

```bash
sudo systemctl stop k3s kine
mongodump --uri="$MONGO_URI" --db=kine --out=./backup-limpo
sudo systemctl start kine k3s
```

## Restore

**Restaurar sobre um cluster em execução corrompe o estado.** Pare tudo primeiro.

```bash
sudo systemctl stop k3s kine

# --drop remove as coleções antes de restaurar; sem isso o restore
# mescla com o que estiver lá e você fica com duas histórias misturadas
mongorestore --uri="$MONGO_URI" --drop --db=kine ./backup-20260907-1430/kine

sudo systemctl start kine
sudo systemctl start k3s
```

Confira o epoch base no log do kine — precisa ser o mesmo de antes:

```
MongoDB pronto: banco=kine coleção=kine epochBase=1767225600
```

Se aparecer um valor diferente, o `kine_meta` não foi restaurado. Pare, restaure a coleção e recomece — não deixe o cluster subir assim.

## Migrar para outro cluster MongoDB

Mesmo procedimento, mudando a URI de destino. O epoch base viaja no `kine_meta`, então as revisões continuam válidas.

```bash
mongodump   --uri="$ORIGEM"  --db=kine --out=./migracao
mongorestore --uri="$DESTINO" --drop --db=kine ./migracao/kine
```

O que **não** funciona é copiar só a coleção `kine`. O destino geraria um epoch base novo na primeira execução, e as revisões existentes passariam a decodificar para outro instante.

## O que o backup não cobre

- **Certificados e tokens do k3s.** Ficam em `/var/lib/rancher/k3s/server/tls` e no próprio datastore (o k3s guarda o bootstrap lá). Um restore do MongoDB devolve o bootstrap; os arquivos locais do nó, não.
- **Volumes dos pods.** `local-path-provisioner` grava em disco, fora do MongoDB.

## Validação

O procedimento foi exercitado contra um cluster real: dump com o cluster no ar, restore com `--drop`, e o cluster voltou com o mesmo epoch base, as chaves intactas e o watch funcionando. Coberto por `TestBackupRestore` em `pkg/drivers/mongo/` (requer `mongodump`/`mongorestore` no PATH; o teste faz skip se não achar).
