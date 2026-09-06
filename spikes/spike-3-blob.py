#!/usr/bin/env python3
"""SPIKE-3 — como representar BLOB binário sobre a REST API JSON do D1.

O corpo da API é JSON e `params` é documentado como array de strings, mas os
valores do kine são protobuf binário arbitrário (bytes 0x00-0xFF). Não há como
passar bytes crus. Este spike compara as estratégias possíveis em correção,
latência e tamanho.

A escolha muda o schema da tabela kine, então bloqueia DRV-5 e KINE-2.

Uso:  ./spikes/spike-3-blob.py
"""
import base64, http.client, json, os, statistics, sys, time

ENV = os.environ.get("KINE_D1_ENV", os.path.expanduser("~/.config/kine-d1/env"))

def load_env():
    cfg = {}
    for line in open(ENV):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); cfg[k.strip()] = v.strip()
    t = cfg.get("CLOUDFLARE_API_TOKEN") or cfg.get("ApiToken")
    a = cfg.get("CLOUDFLARE_ACCOUNT_ID") or cfg.get("AccountId")
    d = cfg.get("D1_DATABASE_ID")
    if not (t and a and d): sys.exit(f"credenciais incompletas em {ENV}")
    return a, d, t

ACCT, DBID, TOKEN = load_env()
PATH = f"/client/v4/accounts/{ACCT}/d1/database/{DBID}/raw"
HDR = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
_conn = None

def q(sql, params=None):
    """Executa e devolve (ok, results, meta, ms, bytes_enviados, erro)."""
    global _conn
    body = json.dumps({"sql": sql, "params": params or []})
    for attempt in (1, 2):
        try:
            if _conn is None:
                _conn = http.client.HTTPSConnection("api.cloudflare.com", timeout=60)
            t = time.perf_counter()
            _conn.request("POST", PATH, body, HDR)
            r = _conn.getresponse(); payload = r.read()
            ms = (time.perf_counter() - t) * 1000
            d = json.loads(payload)
            if not d.get("success"):
                return False, None, None, ms, len(body), json.dumps(d.get("errors"))[:200]
            res = d["result"][0]
            return True, res["results"], res["meta"], ms, len(body), None
        except Exception as e:
            try: _conn.close()
            except Exception: pass
            _conn = None
            if attempt == 2:
                return False, None, None, 0, len(body), f"{type(e).__name__}: {e}"

def rows(results):
    return results["rows"] if isinstance(results, dict) else results

# ---------------------------------------------------------------- estratégias
# Cada uma: (nome, tipo_coluna, sql_insert, codificar, sql_select, decodificar)
ESTRATEGIAS = [
    ("base64 -> TEXT", "TEXT",
     "INSERT INTO t_blob(k, v) VALUES(?, ?)",
     lambda b: base64.b64encode(b).decode(),
     "SELECT v FROM t_blob WHERE k = ?",
     lambda s: base64.b64decode(s)),

    ("hex + unhex() -> BLOB", "BLOB",
     "INSERT INTO t_blob(k, v) VALUES(?, unhex(?))",
     lambda b: b.hex(),
     "SELECT hex(v) FROM t_blob WHERE k = ?",
     lambda s: bytes.fromhex(s)),

    ("array de ints -> BLOB", "BLOB",
     "INSERT INTO t_blob(k, v) VALUES(?, ?)",
     lambda b: list(b),
     "SELECT hex(v) FROM t_blob WHERE k = ?",
     lambda s: bytes.fromhex(s)),

    ("base64 -> CAST AS BLOB", "BLOB",
     "INSERT INTO t_blob(k, v) VALUES(?, CAST(? AS BLOB))",
     lambda b: base64.b64encode(b).decode(),
     "SELECT hex(v) FROM t_blob WHERE k = ?",
     lambda s: base64.b64decode(bytes.fromhex(s))),

    ("string UTF-8 crua", "TEXT",
     "INSERT INTO t_blob(k, v) VALUES(?, ?)",
     lambda b: b.decode("utf-8", "strict"),
     "SELECT v FROM t_blob WHERE k = ?",
     lambda s: s.encode("utf-8")),
]

TAMANHOS = [("1 KB", 1024), ("100 KB", 100 * 1024), ("1,4 MB", 1400 * 1024)]

def prep(tipo):
    q("DROP TABLE IF EXISTS t_blob")
    ok, _, _, _, _, err = q(f"CREATE TABLE t_blob(k TEXT PRIMARY KEY, v {tipo})")
    return ok, err

def testar(nome, tipo, ins, enc, sel, dec, dados, rotulo):
    ok, err = prep(tipo)
    if not ok: return {"status": "erro no CREATE", "detalhe": err}
    try:
        codificado = enc(dados)
    except Exception as e:
        return {"status": "✗ não codifica", "detalhe": type(e).__name__}

    ok, _, meta, ms_ins, envio, err = q(ins, [rotulo, codificado])
    if not ok:
        return {"status": "✗ INSERT falhou", "detalhe": err, "envio": envio}

    ok, res, meta_sel, ms_sel, _, err = q(sel, [rotulo])
    if not ok:
        return {"status": "✗ SELECT falhou", "detalhe": err}
    r = rows(res)
    if not r:
        return {"status": "✗ nada retornou"}
    try:
        voltou = dec(r[0][0])
    except Exception as e:
        return {"status": "✗ não decodifica", "detalhe": f"{type(e).__name__}: {e}"}

    fiel = voltou == dados
    ok2, res2, _, _, _, _ = q("SELECT length(v), typeof(v) FROM t_blob WHERE k = ?", [rotulo])
    guardado, tipo_real = (rows(res2)[0] if ok2 and rows(res2) else (None, "?"))
    return {
        "status": "✓ round-trip fiel" if fiel else "✗ CORROMPEU",
        "insert_ms": round(ms_ins), "select_ms": round(ms_sel),
        "envio_kb": round(envio / 1024, 1),
        "overhead": f"{envio / len(dados):.2f}x",
        "guardado": guardado, "typeof": tipo_real,
    }

def main():
    print("\nSPIKE-3 — representação de BLOB sobre a REST API JSON do D1")
    print("Payload: bytes aleatórios (pior caso — sem compressibilidade)\n")

    for rotulo_t, n in TAMANHOS:
        dados = os.urandom(n)
        print(f"\n{'='*94}\nPAYLOAD {rotulo_t} ({n:,} bytes)\n{'='*94}")
        print(f"{'estratégia':<26} {'resultado':<20} {'ins':>6} {'sel':>6} "
              f"{'envio':>9} {'overh':>7} {'no banco':>10} {'typeof':>8}")
        print("-" * 94)
        for nome, tipo, ins, enc, sel, dec in ESTRATEGIAS:
            r = testar(nome, tipo, ins, enc, sel, dec, dados, f"k-{n}")
            if r["status"].startswith("✓"):
                print(f"{nome:<26} {r['status']:<20} {r['insert_ms']:>5}ms {r['select_ms']:>5}ms "
                      f"{r['envio_kb']:>7} KB {r['overhead']:>7} {str(r['guardado']):>10} {r['typeof']:>8}")
            else:
                det = r.get("detalhe", "")
                print(f"{nome:<26} {r['status']:<20} {det[:46]}")

    # limite de 2 MB por linha: value + old_value juntos
    print(f"\n{'='*94}\nLIMITE DE 2 MB POR LINHA — value + old_value na mesma linha\n{'='*94}")
    q("DROP TABLE IF EXISTS t_lim")
    q("CREATE TABLE t_lim(k TEXT PRIMARY KEY, value BLOB, old_value BLOB)")
    for rotulo, tam in [("2 × 700 KB = 1,4 MB", 700*1024), ("2 × 1,0 MB = 2,0 MB", 1024*1024),
                        ("2 × 1,4 MB = 2,8 MB", 1400*1024)]:
        a, b = os.urandom(tam), os.urandom(tam)
        ok, _, _, ms, envio, err = q(
            "INSERT OR REPLACE INTO t_lim(k, value, old_value) VALUES(?, unhex(?), unhex(?))",
            [rotulo, a.hex(), b.hex()])
        estado = "✓ aceito" if ok else "✗ REJEITADO"
        print(f"  {rotulo:<24} {estado:<14} envio={envio/1024/1024:.2f} MB  "
              f"{(err or '')[:70]}")

    q("DROP TABLE IF EXISTS t_blob"); q("DROP TABLE IF EXISTS t_lim")
    print("\ntabelas de teste removidas")

if __name__ == "__main__":
    main()
