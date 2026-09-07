#!/usr/bin/env python3
"""SPIKE-4 — latência e custo das queries REAIS do kine contra o D1.

Mede p50/p95/p99 por tipo de query (as de pkg/drivers/generic/generic.go, com
hex()/unhex() conforme o ADR-0001) e o custo em rows_read/rows_written de cada
operação — o insumo do modelo de custo da seção 6 do IDEA.md.

Uso:  ./spikes/spike-4-latencia.py [n_amostras]
"""
import os, statistics, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import d1, importlib.util

spec = importlib.util.spec_from_file_location("s8", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spike-8-schema.py"))
s8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(s8)

N = int(sys.argv[1]) if len(sys.argv) > 1 else 25
VAL = 6 * 1024   # objeto k8s típico: ~6 KB

def pct(v, q):
    v = sorted(v)
    return v[min(int(len(v) * q), len(v) - 1)]

def medir(rotulo, sql, params_fn, n=N):
    lat, rr, rw = [], [], []
    for i in range(n):
        ok, res, err, ms, _ = d1.q(sql, params_fn(i))
        if not ok:
            return rotulo, None, err
        m = res[0]["meta"]
        lat.append(ms); rr.append(m.get("rows_read", 0)); rw.append(m.get("rows_written", 0))
    return rotulo, (lat, rr, rw), None

def main():
    print(f"\nSPIKE-4 — latência e custo das queries reais do kine ({N} amostras cada)")
    print(f"Objeto de teste: {VAL//1024} KB (tamanho típico de objeto k8s)\n")

    d1.q("DROP TABLE IF EXISTS kine"); d1.q("DROP TABLE IF EXISTS cas_guard")
    d1.batch([(s, []) for s in s8.SCHEMA])
    d1.q("INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value) "
         "VALUES('compact_rev_key',0,0,0,0,0,NULL)")

    # popula com um cluster pequeno: 300 chaves
    print("populando 300 chaves…", flush=True)
    for lote in range(6):
        d1.batch([(s8.INSERT, [f"/registry/pods/default/p{lote*50+i}", 1, 0, 0, lote*50+i+1, 0,
                               os.urandom(VAL).hex(), 0]) for i in range(50)])
    total = d1.one("SELECT COUNT(*) FROM kine")
    print(f"  {total} linhas na tabela\n")

    testes = [
        ("INSERT (Create/Update)", s8.INSERT,
         lambda i: [f"/registry/pods/default/novo{i}", 1, 0, 0, 10000 + i, 0, os.urandom(VAL).hex(), 0]),
        ("Get de 1 chave", 
         f"""SELECT current_rev, compact_rev, id, name, created, deleted, create_revision,
                    prev_revision, lease, CASE WHEN value IS NULL THEN NULL ELSE hex(value) END AS value
             FROM ({s8.CUR_REV}) AS current, ({s8.CMP_REV}) AS compact, kine
             WHERE id = (SELECT MAX(id) FROM kine WHERE name = ?) AND (deleted = 0 OR ?)""",
         lambda i: [f"/registry/pods/default/p{i % 300}", False]),
        ("List de prefixo (300 chaves)", s8.LIST,
         lambda i: ["/registry/pods/", "/registry/pods0", False]),
        ("After — poll COM eventos", s8.AFTER, lambda i: [0]),
        ("After — poll OCIOSO", s8.AFTER, lambda i: [999999]),
        ("CurrentRevision", s8.CUR_REV, lambda i: []),
        ("CompactRevision", s8.CMP_REV, lambda i: []),
    ]

    print(f"{'query':<30} {'p50':>7} {'p95':>7} {'p99':>7} {'rows_read':>10} {'rows_written':>13}")
    print("-" * 80)
    resultados = {}
    for rot, sql, pf in testes:
        n = N if "List" not in rot and "COM eventos" not in rot else max(8, N // 3)
        rot, dados, err = medir(rot, sql, pf, n)
        if err:
            print(f"{rot:<30} ✗ {err[:44]}"); continue
        lat, rr, rw = dados
        resultados[rot] = (statistics.median(lat), statistics.median(rr), statistics.median(rw))
        print(f"{rot:<30} {statistics.median(lat):>6.0f}ms {pct(lat,.95):>6.0f}ms "
              f"{pct(lat,.99):>6.0f}ms {statistics.median(rr):>10.0f} {statistics.median(rw):>13.0f}")

    # ---------------- modelo de custo corrigido ----------------
    print(f"\n{'='*80}\nMODELO DE CUSTO — corrigido com os valores medidos\n{'='*80}")
    ins_w = resultados.get("INSERT (Create/Update)", (0, 0, 8))[2]
    poll_r = resultados.get("After — poll OCIOSO", (0, 1, 0))[1]
    ok, res, _, _, _ = d1.q("DELETE FROM kine WHERE id = (SELECT MAX(id) FROM kine)")
    del_w = res[0]["meta"]["rows_written"] if ok else 1

    print(f"\n  custo unitário medido:")
    print(f"    1 INSERT  = {ins_w:.0f} rows_written  (1 linha + 6 índices + sqlite_sequence)")
    print(f"    1 DELETE  = {del_w:.0f} rows_written")
    print(f"    1 poll ocioso = {poll_r:.0f} rows_read")

    print(f"\n  {'cenário':<26} {'writes/s':>9} {'rows_w/mês':>14} {'custo/mês':>12}")
    print("  " + "-" * 64)
    for nome, wps in [("médio, ocioso", 3.3), ("médio, operação normal", 10.0), ("grande (50 nós)", 30.0)]:
        por_dia = wps * 86400 * (ins_w + del_w)
        por_mes = por_dia * 30
        excedente = max(0, por_mes - 50_000_000)
        print(f"  {nome:<26} {wps:>9.1f} {por_mes:>14,.0f} {'$' + format(excedente/1_000_000, '.2f'):>12}")

    leitura_mes = poll_r * 86400 * 30
    print(f"\n  rows_read do polling ocioso: {leitura_mes:,.0f}/mês "
          f"({leitura_mes/25_000_000_000*100:.4f}% da franquia de 25 bi)")

    d1.q("DROP TABLE IF EXISTS kine"); d1.q("DROP TABLE IF EXISTS cas_guard")
    print("\nlimpo")

if __name__ == "__main__":
    main()
