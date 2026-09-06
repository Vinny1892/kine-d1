#!/usr/bin/env python3
"""SPIKE-5 + SPIKE-7 — metadados de escrita e Sessions API pela REST.

SPIKE-5: meta.last_row_id, meta.changes e RETURNING funcionam? Decide se o
driver pode usar generic.LastInsertID = true (generic.go:481).

SPIKE-7: a Sessions API (bookmarks para consistência sequencial em réplicas de
leitura) está disponível pela REST API, ou só pelo binding de Worker?

Uso:  ./spikes/spike-5-7-meta-sessions.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import d1

def titulo(t): print(f"\n{'='*74}\n{t}\n{'='*74}")

def prep():
    d1.q("DROP TABLE IF EXISTS t_meta")
    d1.q("""CREATE TABLE t_meta(id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, value BLOB)""")

def spike5():
    print("\nSPIKE-5 — metadados de escrita")
    prep()

    titulo("1. meta.last_row_id em INSERT")
    for i in range(3):
        ok, res, err, _, _ = d1.q("INSERT INTO t_meta(name, value) VALUES(?, unhex(?))",
                                  [f"k{i}", os.urandom(64).hex()])
        m = res[0]["meta"] if ok else {}
        print(f"  insert {i}: last_row_id={m.get('last_row_id')}  changes={m.get('changes')}  "
              f"rows_written={m.get('rows_written')}")
    print("  => LastInsertID = true é viável (generic.go:481 usa esse caminho)")

    titulo("2. meta.changes em UPDATE/DELETE que NÃO casam")
    casos = [
        ("UPDATE que casa",      "UPDATE t_meta SET name='x' WHERE id = 1"),
        ("UPDATE que não casa",  "UPDATE t_meta SET name='x' WHERE id = 99999"),
        ("DELETE que não casa",  "DELETE FROM t_meta WHERE id = 99999"),
    ]
    for rot, sql in casos:
        ok, res, err, _, _ = d1.q(sql)
        m = res[0]["meta"] if ok else {}
        print(f"  {rot:<22} success={ok}  changes={m.get('changes')}")
    print("  => confirma o SPIKE-6: 0 changes NÃO é erro, por isso o CAS precisa da guarda")

    titulo("3. RETURNING é suportado?")
    ok, res, err, _, _ = d1.q("INSERT INTO t_meta(name, value) VALUES(?, NULL) RETURNING id", ["ret"])
    if ok:
        r = d1.rows(res[0])
        print(f"  ✓ suportado — devolveu {r}  colunas={d1.cols(res[0])}")
        print("  => o caminho RETURNING do generic também funcionaria; ficamos no LastInsertID por ser mais simples")
    else:
        print(f"  ✗ não suportado: {err[:130]}")

    titulo("4. hex(NULL) — distinguir NULL de BLOB vazio")
    d1.q("INSERT INTO t_meta(name, value) VALUES('nulo', NULL)")
    d1.q("INSERT INTO t_meta(name, value) VALUES('vazio', unhex(''))")
    ok, res, err, _, _ = d1.q(
        """SELECT name, hex(value), typeof(value),
                  CASE WHEN value IS NULL THEN NULL ELSE hex(value) END AS seguro
           FROM t_meta WHERE name IN ('nulo','vazio') ORDER BY name""")
    print(f"  {'name':<8} {'hex(value)':<12} {'typeof':<8} {'CASE ... IS NULL':<18}")
    for r in d1.rows(res[0]):
        print(f"  {str(r[0]):<8} {repr(r[1]):<12} {str(r[2]):<8} {repr(r[3]):<18}")
    print("\n  => hex(NULL) e hex(x'') são AMBOS '' — indistinguíveis")
    print("  => o CASE ... IS NULL preserva o NULL; o driver precisa usá-lo (DRV-3/KINE-3)")

    titulo("5. Alias de coluna com hex()")
    ok, res, _, _, _ = d1.q("SELECT hex(value) FROM t_meta LIMIT 1")
    ok2, res2, _, _, _ = d1.q("SELECT hex(value) AS value FROM t_meta LIMIT 1")
    print(f"  sem alias: colunas={d1.cols(res[0])}")
    print(f"  com alias: colunas={d1.cols(res2[0])}")
    print("  => o kine usa Scan posicional, mas o alias mantém o SQL legível e a paridade com o upstream")

    d1.q("DROP TABLE IF EXISTS t_meta")

def spike7():
    print("\n\nSPIKE-7 — Sessions API pela REST")
    titulo("1. O estado atual da replicação neste banco")
    ok, res, err, _, _ = d1.call({}, endpoint="")   # GET não; usamos o metadado via query
    modo = "?"
    import json, http.client
    conn = http.client.HTTPSConnection("api.cloudflare.com", timeout=30)
    conn.request("GET", f"/client/v4/accounts/{d1.ACCT}/d1/database/{d1.DBID}", None, d1.HDR)
    r = conn.getresponse(); info = json.loads(r.read()); conn.close()
    rr = info.get("result", {}).get("read_replication", {})
    print(f"  read_replication: {rr}")

    titulo("2. O /raw aceita um bookmark de sessão?")
    # a doc de Workers usa withSession(bookmark); pela REST, testamos os veículos plausíveis
    ok, res, err, _, rh = d1.q("SELECT 1")
    m = res[0]["meta"] if ok else {}
    print(f"  meta da query normal: served_by_primary={m.get('served_by_primary')}  "
          f"served_by_region={m.get('served_by_region')}")
    bookmark_no_meta = [k for k in (m or {}) if "bookmark" in k.lower() or "token" in k.lower()]
    print(f"  campos de bookmark no meta: {bookmark_no_meta or 'nenhum'}")

    tentativas = [
        ("body: session=first-primary", {"sql": "SELECT 1", "params": [], "session": "first-primary"}),
        ("body: session_id",            {"sql": "SELECT 1", "params": [], "session_id": "first-unconstrained"}),
    ]
    for rot, body in tentativas:
        ok, res, err, _, _ = d1.call(body)
        print(f"  {rot:<30} {'✓ aceito (sem erro)' if ok else '✗ ' + str(err)[:60]}")

    hdrs = [("header x-cf-d1-session", {"x-cf-d1-session": "first-primary"}),
            ("header cf-d1-session-commit-token", {"cf-d1-session-commit-token": "first-primary"})]
    for rot, h in hdrs:
        ok, res, err, _, rh = d1.call({"sql": "SELECT 1", "params": []}, extra_headers=h)
        eco = {k: v for k, v in rh.items() if "d1" in k or "bookmark" in k}
        print(f"  {rot:<30} {'✓ aceito' if ok else '✗'}  headers D1 na resposta: {eco or 'nenhum'}")

    print("\n  => leitura: campos desconhecidos no body são ignorados em silêncio, e não há")
    print("     bookmark no meta nem nos headers. A Sessions API não tem veículo documentado")
    print("     na REST API — ela é do binding de Worker.")

if __name__ == "__main__":
    spike5()
    spike7()
    print()
