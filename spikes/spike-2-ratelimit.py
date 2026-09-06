#!/usr/bin/env python3
"""SPIKE-2 — mede o rate limit real dos endpoints /query e /raw do D1.

Pergunta: o limite global da API v4 (1.200 req / 5 min por usuário, ~4 req/s)
se aplica aos endpoints de query do D1? Se sim, a Opção A (REST direta) morre,
porque um cluster k3s precisa de 6-16 req/s.

Método: escalonar a taxa e acumular bem mais de 1.200 requisições dentro de uma
janela de 5 minutos, observando 429. Usa SELECT 1 para não gerar custo nem
escrita (rows_read = 0).

Uso:  ./spikes/spike-2-ratelimit.py [--endpoint raw|query] [--write]
"""
import argparse, http.client, json, os, statistics, sys, threading, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

ENV = os.environ.get("KINE_D1_ENV", os.path.expanduser("~/.config/kine-d1/env"))

def load_env():
    cfg = {}
    with open(ENV) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    token = cfg.get("CLOUDFLARE_API_TOKEN") or cfg.get("ApiToken") or cfg.get("CF_API_TOKEN")
    acct  = cfg.get("CLOUDFLARE_ACCOUNT_ID") or cfg.get("AccountId") or cfg.get("CF_ACCOUNT_ID")
    dbid  = cfg.get("D1_DATABASE_ID")
    if not (token and acct and dbid):
        sys.exit(f"credenciais incompletas em {ENV} — rode spikes/spike-1-setup.sh")
    return acct, dbid, token

# headers que podem carregar informação de rate limit
RL_HEADERS = ("retry-after", "x-ratelimit-limit", "x-ratelimit-remaining",
              "x-ratelimit-reset", "ratelimit-limit", "ratelimit-remaining", "cf-ray")

class Worker(threading.local):
    conn = None

_tls = Worker()

def request(path, body, hdr):
    """Uma requisição, reusando a conexão da thread (keep-alive)."""
    for attempt in (1, 2):
        try:
            if _tls.conn is None:
                _tls.conn = http.client.HTTPSConnection("api.cloudflare.com", timeout=30)
            t = time.perf_counter()
            _tls.conn.request("POST", path, body, hdr)
            r = _tls.conn.getresponse()
            payload = r.read()
            dt = (time.perf_counter() - t) * 1000
            hdrs = {k.lower(): v for k, v in r.getheaders() if k.lower() in RL_HEADERS}
            return r.status, dt, hdrs, payload
        except Exception as e:                    # conexão morta: reabre uma vez
            try: _tls.conn.close()
            except Exception: pass
            _tls.conn = None
            if attempt == 2:
                return 0, 0.0, {"erro": type(e).__name__}, b""

def fase(nome, rate, dur, path, body, hdr, acumulado):
    """Dispara `rate` req/s por `dur` segundos, com concorrência suficiente."""
    workers = max(4, int(rate * 0.6) + 2)         # latência ~270ms => rate*0.27 em voo
    resultados, lock = [], threading.Lock()
    inicio = time.perf_counter()
    total = int(rate * dur)

    def tarefa(i):
        alvo = inicio + i / rate                  # agenda para manter a taxa
        atraso = alvo - time.perf_counter()
        if atraso > 0: time.sleep(atraso)
        res = request(path, body, hdr)
        with lock: resultados.append(res)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(tarefa, range(total)))

    decorrido = time.perf_counter() - inicio
    codigos = Counter(r[0] for r in resultados)
    lat = sorted(r[1] for r in resultados if r[0] == 200)
    real = len(resultados) / decorrido
    acumulado += len(resultados)

    p = lambda q: lat[min(int(len(lat) * q), len(lat) - 1)] if lat else 0
    print(f"  {nome:<22} alvo={rate:>3}/s  real={real:>5.1f}/s  n={len(resultados):>4}  "
          f"acum={acumulado:>4}  {dict(codigos)}", flush=True)
    if lat:
        print(f"  {'':<22} latência p50={statistics.median(lat):>5.0f}ms  "
              f"p95={p(.95):>5.0f}ms  max={lat[-1]:>5.0f}ms", flush=True)

    # qualquer 429 é o achado principal — mostra tudo que a resposta disse
    for st, _, hd, payload in resultados:
        if st == 429:
            print(f"  \033[31m429 DETECTADO\033[0m headers={hd}", flush=True)
            print(f"    body: {payload[:400].decode('utf-8', 'replace')}", flush=True)
            break
    return acumulado, codigos, real

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", choices=["raw", "query"], default="raw")
    ap.add_argument("--write", action="store_true", help="usa INSERT em vez de SELECT 1")
    args = ap.parse_args()

    acct, dbid, token = load_env()
    path = f"/client/v4/accounts/{acct}/d1/database/{dbid}/{args.endpoint}"
    hdr = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if args.write:
        sql = "INSERT INTO t_rl(v) VALUES('x')"
        setup = json.dumps({"sql": "CREATE TABLE IF NOT EXISTS t_rl(id INTEGER PRIMARY KEY, v TEXT)"})
        request(path, setup, hdr)
    else:
        sql = "SELECT 1"
    body = json.dumps({"sql": sql})

    print(f"\nSPIKE-2 — rate limit de /{args.endpoint} ({'INSERT' if args.write else 'SELECT 1'})")
    print(f"Limite global documentado da API v4: 1.200 req / 5 min (~4 req/s)\n")

    acum = 0
    fases = [("A: baseline", 5, 20), ("B: 2,5x o limite", 10, 20),
             ("C: 5x o limite", 20, 20), ("D: 10x o limite", 40, 30)]
    inicio_janela = time.time()
    houve_429 = False
    for nome, rate, dur in fases:
        acum, codigos, real = fase(nome, rate, dur, path, body, hdr, acum)
        if 429 in codigos:
            houve_429 = True
            print("\n  parando: 429 encontrado — o limite existe e foi atingido.")
            break

    janela = time.time() - inicio_janela
    print(f"\n  {acum} requisições em {janela:.0f}s "
          f"({acum/janela:.1f}/s médio; limite documentado equivaleria a {int(4*janela)} req)")
    print("\n" + "="*70)
    if houve_429:
        print("VEREDITO: existe rate limit. Ver os headers acima para o valor real.")
    elif janela <= 300 and acum > 1200:
        print(f"VEREDITO: {acum} requisições em {janela:.0f}s (< 5 min) SEM nenhum 429.")
        print("O limite global de 1.200/5min NÃO se aplica aos endpoints de query do D1.")
        print("=> Opção A (REST direta) é viável.")
    else:
        print("INCONCLUSIVO: não acumulou requisições suficientes na janela de 5 min.")
    print("="*70)

if __name__ == "__main__":
    main()
