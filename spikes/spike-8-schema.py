#!/usr/bin/env python3
"""SPIKE-8 — o schema real do kine no D1 e a constraint de unicidade.

Aplica o schema de pkg/drivers/sqlite/sqlite.go:25 (mais a cas_guard do
ADR-0002) e roda as queries reais de pkg/drivers/generic/generic.go, com as
colunas BLOB envolvidas em unhex()/hex() conforme o ADR-0001.

Valida em particular que a violação de UNIQUE(name, prev_revision) — que é
como o kine detecta "chave já existe" — produz um erro identificável e
mapeável para server.ErrKeyExists.

Uso:  ./spikes/spike-8-schema.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import d1

T = "kine"   # tabela real

SCHEMA = [
    f"""CREATE TABLE IF NOT EXISTS {T} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, created INTEGER, deleted INTEGER,
            create_revision INTEGER, prev_revision INTEGER, lease INTEGER,
            value BLOB, old_value BLOB)""",
    f"CREATE INDEX IF NOT EXISTS kine_name_index ON {T} (name)",
    f"CREATE INDEX IF NOT EXISTS kine_name_id_index ON {T} (name,id)",
    f"CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON {T} (id,deleted)",
    f"CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON {T} (prev_revision)",
    f"CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON {T} (name, prev_revision)",
    f"""CREATE INDEX IF NOT EXISTS kine_id_compact_rev_key_with_prev_revision_index
        ON {T}(id, name, prev_revision) WHERE name != 'compact_rev_key' AND prev_revision != 0""",
    "CREATE TABLE IF NOT EXISTS cas_guard(ok INTEGER PRIMARY KEY CHECK (ok = 1))",
]

# INSERT do generic com LastInsertID (generic.go:InsertLastInsertIDSQL), com unhex()
INSERT = f"""INSERT INTO {T}(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
             SELECT ?, ?, ?, ?, ?, ?, unhex(?), (SELECT value FROM {T} WHERE id = ?)"""

CUR_REV = f"SELECT MAX(id) AS current_rev FROM {T}"
CMP_REV = f"SELECT MAX(prev_revision) AS compact_rev FROM {T} WHERE name = 'compact_rev_key'"

# ListCurrent com valor (generic.go:listValSQL), com hex()
LIST = f"""
    SELECT current_rev, compact_rev, id, name, created, deleted, create_revision,
           prev_revision, lease, hex(value)
    FROM ({CUR_REV}) AS current, ({CMP_REV}) AS compact, {T}
    INNER JOIN (SELECT MAX(id) AS id FROM {T} WHERE name >= ? AND name < ?  GROUP BY name) AS mkv
        USING (id)
    WHERE (deleted = 0 OR ?)
    ORDER BY name ASC"""

# After (generic.go:AfterOldValSQL) — a query do laço de polling
AFTER = f"""
    SELECT current_rev, compact_rev, id, name, created, deleted, create_revision,
           prev_revision, lease, hex(value), hex(old_value)
    FROM ({CUR_REV}) AS current, ({CMP_REV}) AS compact, {T}
    WHERE id > ? ORDER BY id ASC"""

def titulo(t): print(f"\n{'='*74}\n{t}\n{'='*74}")

def main():
    print("\nSPIKE-8 — schema real do kine no D1")

    titulo("1. Aplicar o schema (batch) e conferir idempotência")
    d1.q(f"DROP TABLE IF EXISTS {T}"); d1.q("DROP TABLE IF EXISTS cas_guard")
    ok, res, err, ms, _ = d1.batch([(s, []) for s in SCHEMA])
    print(f"  1ª aplicação: {'✓' if ok else '✗ ' + str(err)}  ({ms:.0f}ms, {len(SCHEMA)} statements)")
    ok2, _, err2, ms2, _ = d1.batch([(s, []) for s in SCHEMA])
    print(f"  2ª aplicação: {'✓ idempotente' if ok2 else '✗ ' + str(err2)}  ({ms2:.0f}ms)")

    n_idx = d1.one("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name=?", [T])
    print(f"  índices criados: {n_idx} (6 explícitos + o do AUTOINCREMENT/UNIQUE)")

    titulo("2. compact_rev_key e inserts com unhex()")
    d1.q(f"INSERT INTO {T}(name, created, deleted, create_revision, prev_revision, lease, value) "
         f"VALUES('compact_rev_key', 0, 0, 0, 0, 0, NULL)")
    payload = os.urandom(2048)
    ok, res, err, ms, _ = d1.q(INSERT, ["/registry/pods/default/nginx", 1, 0, 0, 0, 0, payload.hex(), 0])
    lastid = res[0]["meta"]["last_row_id"] if ok else None
    print(f"  INSERT do generic: {'✓' if ok else '✗ ' + str(err)}  last_row_id={lastid}  ({ms:.0f}ms)")

    volta = d1.one(f"SELECT hex(value) FROM {T} WHERE id = ?", [lastid])
    fiel = volta and bytes.fromhex(volta) == payload
    print(f"  round-trip do value: {'✓ byte a byte idêntico' if fiel else '✗ CORROMPEU'}")

    titulo("3. UNIQUE(name, prev_revision) — a detecção de chave duplicada")
    err = d1.erro_de(INSERT, ["/registry/pods/default/nginx", 1, 0, 0, 0, 0, payload.hex(), 0])
    print(f"  segundo insert com o mesmo (name, prev_revision):")
    print(f"    {'✗ passou — a constraint NÃO está funcionando' if err is None else '✓ rejeitado'}")
    if err:
        print(f"    erro: {err[:150]}")
        marcadores = ["UNIQUE constraint failed", "kine_name_prev_revision_uindex", "SQLITE_CONSTRAINT"]
        achados = [m for m in marcadores if m in err]
        print(f"    marcadores para mapear -> server.ErrKeyExists: {achados}")

    titulo("4. As queries reais do generic funcionam com hex()?")
    for i in range(5):
        d1.q(INSERT, [f"/registry/pods/default/p{i}", 1, 0, 0, i + 1, 0, os.urandom(512).hex(), 0])

    ok, res, err, ms, _ = d1.q(LIST, ["/registry/pods/", "/registry/pods0", False])
    n = len(d1.rows(res[0])) if ok else 0
    print(f"  ListCurrent (prefixo): {'✓' if ok else '✗ ' + str(err)}  {n} linhas  ({ms:.0f}ms)")
    if ok and n:
        print(f"    colunas: {d1.cols(res[0])}")

    ok, res, err, ms, _ = d1.q(AFTER, [0])
    n = len(d1.rows(res[0])) if ok else 0
    print(f"  After (laço de poll):  {'✓' if ok else '✗ ' + str(err)}  {n} linhas  ({ms:.0f}ms)")
    rr = res[0]["meta"]["rows_read"] if ok else "?"
    print(f"    rows_read desta query: {rr}")

    cur = d1.one(CUR_REV); cmp_ = d1.one(CMP_REV)
    print(f"  CurrentRevision={cur}   CompactRevision={cmp_}")

    titulo("5. Custo do poll ocioso (a query que roda 1×/s para sempre)")
    ok, res, err, ms, _ = d1.q(AFTER, [999999])
    m = res[0]["meta"] if ok else {}
    print(f"  After(rev alta, sem eventos): rows_read={m.get('rows_read')}  "
          f"duration={m.get('duration')}ms  RTT={ms:.0f}ms")
    print(f"  => projeção: {m.get('rows_read', 0) * 86400:,} linhas lidas/dia só de polling ocioso")

    titulo("6. NULL em value (o compact_rev_key guarda NULL)")
    v = d1.one(f"SELECT hex(value) FROM {T} WHERE name='compact_rev_key'")
    print(f"  hex(NULL) devolve: {v!r}  -> o driver precisa distinguir NULL de string vazia")

    d1.q(f"DROP TABLE IF EXISTS {T}"); d1.q("DROP TABLE IF EXISTS cas_guard")
    print("\nlimpo")

if __name__ == "__main__":
    main()
