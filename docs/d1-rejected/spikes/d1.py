"""Cliente mínimo do D1 para os spikes. Nunca imprime o token."""
import http.client, json, os, sys, threading, time

ENV = os.environ.get("KINE_D1_ENV", os.path.expanduser("~/.config/kine-d1/env"))

def _load():
    cfg = {}
    for line in open(ENV):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); cfg[k.strip()] = v.strip()
    t = cfg.get("CLOUDFLARE_API_TOKEN") or cfg.get("ApiToken")
    a = cfg.get("CLOUDFLARE_ACCOUNT_ID") or cfg.get("AccountId")
    d = cfg.get("D1_DATABASE_ID")
    if not (t and a and d):
        sys.exit(f"credenciais incompletas em {ENV} — rode spikes/spike-1-setup.sh")
    return a, d, t

ACCT, DBID, TOKEN = _load()
HDR = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
_tls = threading.local()

def call(body_obj, endpoint="raw", extra_headers=None):
    """Devolve (ok, result_list, erro, ms, response_headers)."""
    path = f"/client/v4/accounts/{ACCT}/d1/database/{DBID}/{endpoint}"
    hdr = dict(HDR)
    if extra_headers: hdr.update(extra_headers)
    body = json.dumps(body_obj)
    for attempt in (1, 2):
        try:
            if getattr(_tls, "conn", None) is None:
                _tls.conn = http.client.HTTPSConnection("api.cloudflare.com", timeout=60)
            t0 = time.perf_counter()
            _tls.conn.request("POST", path, body, hdr)
            r = _tls.conn.getresponse(); payload = r.read()
            ms = (time.perf_counter() - t0) * 1000
            rh = {k.lower(): v for k, v in r.getheaders()}
            d = json.loads(payload)
            if not d.get("success"):
                return False, None, json.dumps(d.get("errors"))[:250], ms, rh
            return True, d.get("result", []), None, ms, rh
        except Exception as e:
            try: _tls.conn.close()
            except Exception: pass
            _tls.conn = None
            if attempt == 2:
                return False, None, f"{type(e).__name__}: {e}", 0.0, {}

def q(sql, params=None, endpoint="raw"):
    return call({"sql": sql, "params": params or []}, endpoint)

def batch(stmts, endpoint="raw"):
    return call({"batch": [{"sql": s, "params": p or []} for s, p in stmts]}, endpoint)

def rows(item):
    r = item["results"]
    return r["rows"] if isinstance(r, dict) else r

def cols(item):
    r = item["results"]
    return r["columns"] if isinstance(r, dict) else []

def one(sql, params=None):
    """Primeira célula da primeira linha, ou None."""
    ok, res, err, _, _ = q(sql, params)
    if not ok: return None
    r = rows(res[0])
    return r[0][0] if r else None

def erro_de(sql, params=None):
    ok, _, err, _, _ = q(sql, params)
    return None if ok else err
