#!/usr/bin/env python3
"""SPIKE-6 — batch atômico do D1 e o padrão compare-and-swap da compactação.

O D1 rejeita BEGIN TRANSACTION/SAVEPOINT; só existe `batch`, atômico mas não
interativo. O compact do kine (pkg/logstructured/sqllog/sql.go:234) precisa de:

    ler revisão atual -> ler compact_rev -> COMPARAR com o esperado
    -> apagar linhas -> gravar novo compact_rev  (tudo atômico)

A comparação é o problema: um `UPDATE ... WHERE cond` que não casa afeta 0
linhas e **não gera erro**, então o batch seguiria em frente e apagaria dados
com base numa premissa falsa.

Este spike responde:
  1. o batch é mesmo atômico? (falha no meio reverte o começo?)
  2. como fazer um CAS falho ABORTAR o batch?
  3. o padrão sobrevive a duas instâncias compactando ao mesmo tempo?

Uso:  ./spikes/spike-6-batch-cas.py
"""
import http.client, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor

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
HDR = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def call(endpoint, body_obj):
    """Uma chamada; devolve (ok_global, lista_de_results, erro)."""
    conn = http.client.HTTPSConnection("api.cloudflare.com", timeout=60)
    p = f"/client/v4/accounts/{ACCT}/d1/database/{DBID}/{endpoint}"
    conn.request("POST", p, json.dumps(body_obj), HDR)
    r = conn.getresponse(); payload = r.read(); conn.close()
    d = json.loads(payload)
    if not d.get("success"):
        return False, None, json.dumps(d.get("errors"))[:220]
    return True, d.get("result", []), None

def q(sql, params=None):
    ok, res, err = call("raw", {"sql": sql, "params": params or []})
    return ok, res, err

def batch(stmts):
    """stmts: lista de (sql, params). Envia como batch numa única chamada."""
    return call("raw", {"batch": [{"sql": s, "params": p or []} for s, p in stmts]})

def rows(res_item):
    r = res_item["results"]
    return r["rows"] if isinstance(r, dict) else r

def contar():
    ok, res, err = q("SELECT COUNT(*) FROM kine_sim WHERE name != 'compact_rev_key'")
    return rows(res[0])[0][0] if ok else f"erro: {err}"

def compact_rev():
    ok, res, err = q("SELECT prev_revision FROM kine_sim WHERE name = 'compact_rev_key'")
    r = rows(res[0]) if ok else None
    return r[0][0] if r else None

def semear(n=20):
    q("DROP TABLE IF EXISTS kine_sim")
    q("""CREATE TABLE kine_sim(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT,
            prev_revision INTEGER, value BLOB)""")
    q("INSERT INTO kine_sim(name, prev_revision) VALUES('compact_rev_key', 0)")
    stmts = [("INSERT INTO kine_sim(name, prev_revision) VALUES(?, 0)", [f"/registry/pods/p{i}"])
             for i in range(n)]
    batch(stmts)

def titulo(t):
    print(f"\n{'='*76}\n{t}\n{'='*76}")

# ---------------------------------------------------------------------------
def teste_1_atomicidade():
    titulo("1. O batch é atômico? (statement do meio falha)")
    semear(5)
    antes = contar()
    ok, res, err = batch([
        ("INSERT INTO kine_sim(name, prev_revision) VALUES('novo-A', 0)", []),
        ("INSERT INTO kine_sim(id, name, prev_revision) VALUES(1, 'colide-PK', 0)", []),  # PK duplicada
        ("INSERT INTO kine_sim(name, prev_revision) VALUES('novo-C', 0)", []),
    ])
    depois = contar()
    print(f"  batch retornou success={ok}  erro={(err or '')[:80]}")
    print(f"  linhas antes={antes}  depois={depois}")
    ok2, res2, _ = q("SELECT COUNT(*) FROM kine_sim WHERE name IN ('novo-A','novo-C')")
    vazou = rows(res2[0])[0][0]
    print(f"  linhas do batch que sobraram: {vazou}")
    print(f"  => {'✓ ATÔMICO — nada foi aplicado' if vazou == 0 else '✗ NÃO é atômico: ' + str(vazou) + ' vazaram'}")
    return vazou == 0

# ---------------------------------------------------------------------------
GUARD_CHECK = """INSERT INTO cas_guard(ok)
                 SELECT 0 WHERE (SELECT prev_revision FROM kine_sim
                                 WHERE name='compact_rev_key') != ?"""

ABS_OVERFLOW = """SELECT CASE WHEN (SELECT prev_revision FROM kine_sim
                                    WHERE name='compact_rev_key') = ?
                              THEN 1 ELSE abs(-9223372036854775808) END"""

def teste_2_mecanismos():
    titulo("2. Como fazer um CAS falho abortar o batch?")
    semear(5)
    q("DROP TABLE IF EXISTS cas_guard")
    q("CREATE TABLE cas_guard(ok INTEGER PRIMARY KEY CHECK (ok = 1))")
    real = compact_rev()
    print(f"  compact_rev real no banco: {real}\n")

    mecanismos = [
        ("UPDATE ... WHERE (ingênuo)",
         "UPDATE kine_sim SET prev_revision = 999 WHERE name='compact_rev_key' AND prev_revision = ?"),
        ("INSERT + CHECK constraint", GUARD_CHECK),
        ("abs() integer overflow", ABS_OVERFLOW),
    ]
    resultados = {}
    for nome, sql in mecanismos:
        linha = []
        for rotulo, valor in [("CAS correto", real), ("CAS errado", real + 12345)]:
            ok, res, err = q(sql, [valor])
            linha.append(f"{rotulo}: {'passa' if ok else 'ABORTA'}")
            if rotulo == "CAS correto": passou_certo = ok
            else: abortou_errado = not ok
            q("DELETE FROM cas_guard")
            q("UPDATE kine_sim SET prev_revision = ? WHERE name='compact_rev_key'", [real])
        serve = passou_certo and abortou_errado
        resultados[nome] = serve
        print(f"  {nome:<28} {' | '.join(linha):<40} {'✓ serve' if serve else '✗ não serve'}")
    return resultados

# ---------------------------------------------------------------------------
def compactar(alvo, esperado, mecanismo):
    """Simula um ciclo de compact do kine como um único batch atômico."""
    guarda = (GUARD_CHECK if mecanismo == "check" else ABS_OVERFLOW, [esperado])
    return batch([
        guarda,
        ("DELETE FROM kine_sim WHERE name != 'compact_rev_key' AND id <= ?", [alvo]),
        ("UPDATE kine_sim SET prev_revision = ? WHERE name = 'compact_rev_key'", [alvo]),
    ])

def teste_3_compact(mecanismo):
    titulo(f"3. Ciclo de compact completo num batch (guarda: {mecanismo})")
    semear(20)
    q("DROP TABLE IF EXISTS cas_guard")
    q("CREATE TABLE cas_guard(ok INTEGER PRIMARY KEY CHECK (ok = 1))")

    print(f"  estado inicial: {contar()} linhas, compact_rev={compact_rev()}")

    ok, _, err = compactar(alvo=10, esperado=compact_rev(), mecanismo=mecanismo)
    print(f"\n  a) CAS correto  -> success={ok}  linhas={contar()}  compact_rev={compact_rev()}")
    bom = ok and compact_rev() == 10

    n_antes, rev_antes = contar(), compact_rev()
    ok, _, err = compactar(alvo=18, esperado=99999, mecanismo=mecanismo)
    n_dep, rev_dep = contar(), compact_rev()
    print(f"  b) CAS errado   -> success={ok}  linhas={n_dep}  compact_rev={rev_dep}")
    print(f"     erro: {(err or '—')[:88]}")
    protegeu = (not ok) and n_dep == n_antes and rev_dep == rev_antes
    print(f"\n  => {'✓ o CAS falho NÃO apagou nada e NÃO moveu a revisão' if protegeu else '✗ FALHOU: dados foram alterados apesar do CAS errado'}")
    return bom and protegeu

# ---------------------------------------------------------------------------
def teste_4_concorrencia(mecanismo):
    titulo(f"4. Duas instâncias compactando ao mesmo tempo (guarda: {mecanismo})")
    semear(40)
    q("DROP TABLE IF EXISTS cas_guard")
    q("CREATE TABLE cas_guard(ok INTEGER PRIMARY KEY CHECK (ok = 1))")
    esperado = compact_rev()
    print(f"  ambas leem compact_rev={esperado} e tentam compactar para alvos diferentes")

    def tentar(alvo):
        ok, _, err = compactar(alvo=alvo, esperado=esperado, mecanismo=mecanismo)
        return alvo, ok, (err or "")[:60]

    with ThreadPoolExecutor(max_workers=2) as ex:
        res = list(ex.map(tentar, [15, 25]))
    for alvo, ok, err in res:
        print(f"    alvo={alvo}: {'✓ venceu' if ok else '✗ abortou'}  {err}")
    vencedores = sum(1 for _, ok, _ in res if ok)
    rev = compact_rev()
    print(f"\n  vencedores: {vencedores}  compact_rev final: {rev}  linhas: {contar()}")
    bom = vencedores == 1 and rev in (15, 25)
    print(f"  => {'✓ exatamente uma venceu — sem corrupção' if bom else '✗ ' + str(vencedores) + ' venceram (esperado 1)'}")
    return bom

# ---------------------------------------------------------------------------
def main():
    print("\nSPIKE-6 — batch atômico e o padrão CAS da compactação")
    atomico = teste_1_atomicidade()
    mecs = teste_2_mecanismos()
    escolhido = "check" if mecs.get("INSERT + CHECK constraint") else "abs"
    compact_ok = teste_3_compact(escolhido)
    conc_ok = teste_4_concorrencia(escolhido)

    q("DROP TABLE IF EXISTS kine_sim"); q("DROP TABLE IF EXISTS cas_guard")
    titulo("VEREDITO")
    print(f"  batch atômico ................. {'✓' if atomico else '✗'}")
    for k, v in mecs.items():
        print(f"  guarda: {k:<28} {'✓ serve' if v else '✗ não serve'}")
    print(f"  ciclo de compact protegido .... {'✓' if compact_ok else '✗'}")
    print(f"  seguro sob concorrência ....... {'✓' if conc_ok else '✗'}")
    viavel = atomico and compact_ok and conc_ok
    print(f"\n  {'✓ A transação diferida com CAS (KINE-5) é viável.' if viavel else '✗ O desenho do KINE-5 precisa ser revisto.'}")

if __name__ == "__main__":
    main()
