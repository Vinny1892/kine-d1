#!/usr/bin/env python3
"""MSPIKE-6 — o que acontece ao estourar o teto de 100 ops/s do M0.

É a premissa que justificou abandonar o Cloudflare D1: excesso de carga precisa
become LATENCY, not an invoice. If Atlas answers with errors instead of
queueing, the premise is partly wrong - not a huge bill, but a broken
quebrado, e o driver precisa tratar isso.

Also measures whether queueing reaches change streams: if the watch stalls,
it can fall outside the oplog window (4.4h, MSPIKE-8) during a spike.

Usage:  spikes/mspike-6-throttle.py [multiplicador]
"""
import os, sys, threading, time, statistics
import concurrent.futures as cf
from collections import Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import section
from pymongo.errors import OperationFailure, PyMongoError

MULT = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
DB_TESTE = "kine_throttle"

def phase(col, nome, alvo_ops, dur, operacao):
    """Dispara alvo_ops por segundo durante dur segundos."""
    workers = max(8, int(alvo_ops * 0.3))
    total = int(alvo_ops * dur)
    lat, erros, lock = [], [], threading.Lock()
    inicio = time.perf_counter()

    def tarefa(i):
        agendado = inicio + i / alvo_ops
        atraso = agendado - time.perf_counter()
        if atraso > 0:
            time.sleep(atraso)
        t0 = time.perf_counter()
        try:
            operacao(col, i)
            with lock:
                lat.append((time.perf_counter() - t0) * 1000)
        except PyMongoError as e:
            cod = getattr(e, "code", None)
            with lock:
                erros.append(f"{type(e).__name__}({cod})")
        except Exception as e:
            with lock:
                erros.append(type(e).__name__)

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(tarefa, range(total)))

    dt = time.perf_counter() - inicio
    real = len(lat) / dt if dt else 0
    p = lambda q: sorted(lat)[min(int(len(lat)*q), len(lat)-1)] if lat else 0
    print(f"  {nome:<18} alvo={alvo_ops:>4}/s  real={real:>6.1f}/s  "
          f"ok={len(lat):>5}  erro={len(erros):>4}  "
          f"p50={statistics.median(lat) if lat else 0:>6.0f}ms  p99={p(.99):>7.0f}ms")
    if erros:
        for tipo, n in Counter(erros).most_common(3):
            print(f"  {'':<18} → {n}× {tipo}")
    return {"real": real, "ok": len(lat), "erros": Counter(erros),
            "p50": statistics.median(lat) if lat else 0, "p99": p(.99)}

def main():
    c = mongo.client(maxPoolSize=200)
    d = c[DB_TESTE]
    col = d["carga"]
    col.drop()
    col.create_index("k")
    print(f"\nMSPIKE-6 — comportamento do M0 acima de 100 ops/s (multiplicador {MULT})")
    print("The documented free tier limit is 100 operations/second.\n")

    inserir = lambda col, i: col.insert_one({"k": i, "v": "x" * 200})
    ler     = lambda col, i: col.find_one({"k": i % 500})

    # ---- um watch aberto durante todo o teste, para ver se o throttle o atinge
    section("Change Stream aberto antes da carga")
    eventos = []
    parar = threading.Event()
    erro_cs = []
    def observar():
        try:
            with col.watch() as cs:
                for ev in cs:
                    eventos.append(time.perf_counter())
                    if parar.is_set():
                        break
        except Exception as e:
            erro_cs.append(f"{type(e).__name__}: {str(e)[:120]}")
    th = threading.Thread(target=observar, daemon=True)
    th.start()
    time.sleep(3)
    print(f"  stream opened (errors so far: {erro_cs or 'none'})")

    section("Escalonando ESCRITA acima do teto")
    res = {}
    for nome, taxa, dur in [("50/s (metade)", 50, 10), ("100/s (no teto)", 100, 10),
                            ("250/s (2,5x)", 250, 10), ("600/s (6x)", 600, 10)]:
        res[nome] = phase(col, nome, int(taxa*MULT), dur, inserir)
        time.sleep(2)

    section("Escalonando LEITURA acima do teto")
    for nome, taxa, dur in [("100/s leitura", 100, 8), ("600/s leitura", 600, 8)]:
        res[nome] = phase(col, nome, int(taxa*MULT), dur, ler)
        time.sleep(2)

    section("O Change Stream sobreviveu ao pico?")
    n_ev = len(eventos)
    print(f"  events received durante todo o teste: {n_ev:,}")
    print(f"  erros no stream: {erro_cs or 'none'}")
    if n_ev:
        # did the stream keep up, or fall far behind?
        atraso = time.perf_counter() - eventos[-1]
        print(f"  last event received {atraso:.1f}s")
        print(f"  => {'✓ o stream continuou entregando' if atraso < 30 else '⚠ o stream parou de entregar'}")
    else:
        print("  ⚠ no event received — the stream did not keep up with the load")
    parar.set()

    section("Depois do pico: a performance volta ao normal?")
    time.sleep(5)
    recup = phase(col, "50/s post-spike", int(50*MULT), 10, inserir)
    base = res.get("50/s (metade)", {})
    if base.get("p50") and recup.get("p50"):
        fator = recup["p50"] / base["p50"]
        print(f"  p50 antes={base['p50']:.0f}ms  depois={recup['p50']:.0f}ms  ({fator:.2f}x)")
        print(f"  => {'✓ recuperou' if fator < 2 else '⚠ ainda degradado'}")

    section("VEREDITO")
    todos_erros = Counter()
    for r in res.values():
        todos_erros.update(r["erros"])
    if not todos_erros:
        print("  ✓ NO errors at any rate, up to 6x the documented ceiling.")
        print("    Excess became latency, not failure - the pivot's premise holds.")
    else:
        print("  ⚠ houve erros:")
        for tipo, n in todos_erros.most_common():
            print(f"      {n}× {tipo}")
        print("\n    If these are throttling errors, the driver must handle them with backoff;")
        print("    the apiserver must not receive the raw failure.")
    print(f"\n  latency by rate (p50 -> p99):")
    for nome, r in res.items():
        print(f"    {nome:<18} {r['p50']:>6.0f} → {r['p99']:>7.0f} ms   ({r['real']:.0f} ops/s reais)")

    col.drop()
    c.drop_database(DB_TESTE)
    print("\ncleaned up")

if __name__ == "__main__":
    main()
