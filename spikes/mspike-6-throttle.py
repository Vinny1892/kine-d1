#!/usr/bin/env python3
"""MSPIKE-6 — o que acontece ao estourar o teto de 100 ops/s do M0.

É a premissa que justificou abandonar o Cloudflare D1: excesso de carga precisa
virar LENTIDÃO, não fatura. Se o Atlas responder com erro em vez de throttle, a
premissa está parcialmente errada — não vira conta caríssima, vira cluster
quebrado, e o driver precisa tratar isso.

Mede também se o throttle atinge os Change Streams: se o watch parar junto, ele
pode ficar para trás da janela do oplog (4,4h, MSPIKE-8) durante um pico.

Uso:  spikes/mspike-6-throttle.py [multiplicador]
"""
import os, sys, threading, time, statistics
import concurrent.futures as cf
from collections import Counter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import titulo
from pymongo.errors import OperationFailure, PyMongoError

MULT = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
DB_TESTE = "kine_throttle"

def fase(col, nome, alvo_ops, dur, operacao):
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
    print("O limite documentado do free tier é 100 operações/segundo.\n")

    inserir = lambda col, i: col.insert_one({"k": i, "v": "x" * 200})
    ler     = lambda col, i: col.find_one({"k": i % 500})

    # ---- um watch aberto durante todo o teste, para ver se o throttle o atinge
    titulo("Change Stream aberto antes da carga")
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
    print(f"  stream aberto (erros até agora: {erro_cs or 'nenhum'})")

    titulo("Escalonando ESCRITA acima do teto")
    res = {}
    for nome, taxa, dur in [("50/s (metade)", 50, 10), ("100/s (no teto)", 100, 10),
                            ("250/s (2,5x)", 250, 10), ("600/s (6x)", 600, 10)]:
        res[nome] = fase(col, nome, int(taxa*MULT), dur, inserir)
        time.sleep(2)

    titulo("Escalonando LEITURA acima do teto")
    for nome, taxa, dur in [("100/s leitura", 100, 8), ("600/s leitura", 600, 8)]:
        res[nome] = fase(col, nome, int(taxa*MULT), dur, ler)
        time.sleep(2)

    titulo("O Change Stream sobreviveu ao pico?")
    n_ev = len(eventos)
    print(f"  eventos recebidos durante todo o teste: {n_ev:,}")
    print(f"  erros no stream: {erro_cs or 'nenhum'}")
    if n_ev:
        # o stream acompanhou ou ficou muito para trás?
        atraso = time.perf_counter() - eventos[-1]
        print(f"  último evento recebido há {atraso:.1f}s")
        print(f"  => {'✓ o stream continuou entregando' if atraso < 30 else '⚠ o stream parou de entregar'}")
    else:
        print("  ⚠ nenhum evento recebido — o stream não acompanhou a carga")
    parar.set()

    titulo("Depois do pico: a performance volta ao normal?")
    time.sleep(5)
    recup = fase(col, "50/s pós-pico", int(50*MULT), 10, inserir)
    base = res.get("50/s (metade)", {})
    if base.get("p50") and recup.get("p50"):
        fator = recup["p50"] / base["p50"]
        print(f"  p50 antes={base['p50']:.0f}ms  depois={recup['p50']:.0f}ms  ({fator:.2f}x)")
        print(f"  => {'✓ recuperou' if fator < 2 else '⚠ ainda degradado'}")

    titulo("VEREDITO")
    todos_erros = Counter()
    for r in res.values():
        todos_erros.update(r["erros"])
    if not todos_erros:
        print("  ✓ NENHUM erro em nenhuma taxa, até 6x o teto documentado.")
        print("    O excesso virou latência, não falha — a premissa do pivot se confirma.")
    else:
        print("  ⚠ houve erros:")
        for tipo, n in todos_erros.most_common():
            print(f"      {n}× {tipo}")
        print("\n    Se são erros de throttle, o driver precisa tratá-los com backoff;")
        print("    o apiserver não deve receber a falha crua.")
    print(f"\n  latência por taxa (p50 → p99):")
    for nome, r in res.items():
        print(f"    {nome:<18} {r['p50']:>6.0f} → {r['p99']:>7.0f} ms   ({r['real']:.0f} ops/s reais)")

    col.drop()
    c.drop_database(DB_TESTE)
    print("\nlimpo")

if __name__ == "__main__":
    main()
