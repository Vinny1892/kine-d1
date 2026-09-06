#!/usr/bin/env python3
"""SPIKE-9 — custo end-to-end de um ciclo real do kine, e o teto de cluster.

Não basta saber que um INSERT custa 8 rows_written: o ciclo completo do kine
inclui a compactação, que apaga linhas (e DELETE também é cobrado). Este spike
mede o custo REAL por mutação, ponta a ponta, e converte isso em teto de
cluster para um dado orçamento.

Uso:  ./spikes/spike-9-carga.py [n_mutacoes]
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import d1, importlib.util

spec = importlib.util.spec_from_file_location(
    "s8", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spike-8-schema.py"))
s8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(s8)

N = int(sys.argv[1]) if len(sys.argv) > 1 else 400
VAL = 6 * 1024

COMPACT_SQL = """DELETE FROM kine AS kv
    WHERE kv.id IN (
        SELECT kp.prev_revision AS id FROM kine AS kp
        WHERE kp.name != 'compact_rev_key' AND kp.prev_revision != 0 AND kp.id <= ?
        UNION
        SELECT kd.id AS id FROM kine AS kd WHERE kd.deleted != 0 AND kd.id <= ?)"""

def titulo(t): print(f"\n{'='*76}\n{t}\n{'='*76}")

def main():
    print(f"\nSPIKE-9 — custo end-to-end do ciclo do kine ({N} mutações)\n")
    d1.q("DROP TABLE IF EXISTS kine"); d1.q("DROP TABLE IF EXISTS cas_guard")
    d1.batch([(s, []) for s in s8.SCHEMA])
    d1.q("INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value) "
         "VALUES('compact_rev_key',0,0,0,0,0,NULL)")

    titulo("1. Fase de escrita — mutações como o kine as faz")
    # 100 chaves distintas, cada uma atualizada N/100 vezes (padrão de status update)
    n_chaves = 100
    total_w = total_r = 0
    t0 = time.perf_counter()
    prev = {f"/registry/pods/default/p{i}": 0 for i in range(n_chaves)}
    feitas = 0
    while feitas < N:
        lote = []
        for i in range(min(50, N - feitas)):
            k = f"/registry/pods/default/p{(feitas + i) % n_chaves}"
            lote.append((s8.INSERT, [k, 0, 0, 0, prev[k], 0, os.urandom(VAL).hex(), prev[k]]))
            prev[k] = 0  # simplificação: prev_revision distinto evitaria a UNIQUE
        ok, res, err, ms, _ = d1.batch(lote)
        if not ok:
            # a UNIQUE(name, prev_revision) impede repetir o par; usa prev_revision crescente
            break
        for item in res:
            total_w += item["meta"].get("rows_written", 0)
            total_r += item["meta"].get("rows_read", 0)
        feitas += len(lote)

    # caminho realista: prev_revision sempre crescente, como o kine faz
    if feitas < N:
        rev = 1
        while feitas < N:
            lote = []
            for i in range(min(50, N - feitas)):
                k = f"/registry/pods/default/p{(feitas + i) % n_chaves}"
                rev += 1
                lote.append((s8.INSERT, [k, 0, 0, 0, rev, 0, os.urandom(VAL).hex(), rev - 1]))
            ok, res, err, ms, _ = d1.batch(lote)
            if not ok:
                print(f"  ✗ {err[:100]}"); break
            for item in res:
                total_w += item["meta"].get("rows_written", 0)
                total_r += item["meta"].get("rows_read", 0)
            feitas += len(lote)

    dt = time.perf_counter() - t0
    linhas = d1.one("SELECT COUNT(*) FROM kine")
    print(f"  {feitas} mutações em {dt:.1f}s")
    print(f"  rows_written: {total_w:,}  ({total_w/max(feitas,1):.1f} por mutação)")
    print(f"  rows_read:    {total_r:,}  ({total_r/max(feitas,1):.1f} por mutação)")
    print(f"  linhas na tabela: {linhas}")

    titulo("2. Fase de compactação — o custo do outro lado do ciclo")
    cur = d1.one("SELECT MAX(id) FROM kine")
    alvo = cur - 50   # retém as últimas 50, como o compactMinRetain faz
    ok, res, err, ms, _ = d1.batch([
        (COMPACT_SQL, [alvo, alvo]),
        ("UPDATE kine SET prev_revision = ? WHERE name = 'compact_rev_key'", [alvo]),
    ])
    if ok:
        cw = sum(i["meta"].get("rows_written", 0) for i in res)
        cr = sum(i["meta"].get("rows_read", 0) for i in res)
        apagadas = res[0]["meta"].get("changes", 0)
        print(f"  compactou até rev {alvo}: {apagadas} linhas apagadas em {ms:.0f}ms")
        print(f"  rows_written: {cw:,}  ({cw/max(apagadas,1):.2f} por linha apagada)")
        print(f"  rows_read:    {cr:,}")
    else:
        print(f"  ✗ {err[:120]}"); cw = 0; apagadas = 1

    total_ciclo = total_w + cw
    por_mutacao = total_ciclo / max(feitas, 1)

    titulo("3. Custo real por mutação, ponta a ponta")
    print(f"  escrita ...... {total_w/max(feitas,1):>6.2f} rows_written/mutação")
    print(f"  compactação .. {cw/max(feitas,1):>6.2f} rows_written/mutação")
    print(f"  {'TOTAL':<12} {por_mutacao:>6.2f} rows_written/mutação")

    # ---- conversão para dinheiro e teto de cluster ----
    seg_mes = 86400 * 30
    por_write_s_mes = por_mutacao * seg_mes           # rows_written/mês por 1 write/s
    franquia = 50_000_000
    gratis_ws = franquia / por_write_s_mes
    usd_por_ws = por_write_s_mes / 1_000_000          # $1 por milhão

    titulo("4. A métrica que interessa")
    print(f"  1 write/s sustentado = {por_write_s_mes:,.0f} rows_written/mês")
    print(f"  franquia de 50 mi/mês cobre .......... {gratis_ws:.2f} writes/s")
    print(f"  cada write/s ADICIONAL custa ......... US$ {usd_por_ws:.2f}/mês")

    print(f"\n  {'orçamento/mês':>14} {'writes/s suportados':>22}")
    print("  " + "-" * 38)
    for orc in (0, 50, 100, 250, 500, 1000):
        ws = gratis_ws + orc / usd_por_ws
        print(f"  {'US$ ' + str(orc):>14} {ws:>22.1f}")

    d1.q("DROP TABLE IF EXISTS kine"); d1.q("DROP TABLE IF EXISTS cas_guard")
    print("\nlimpo")
    return por_mutacao, gratis_ws, usd_por_ws

if __name__ == "__main__":
    main()
