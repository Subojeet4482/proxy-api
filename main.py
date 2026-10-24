import asyncio
import bisect
import json
import os
import random
import re
import time
from contextlib import asynccontextmanager

import aiohttp
import orjson
from aiohttp_socks import ProxyConnector
from fastapi import FastAPI
from fastapi.responses import Response

# ---------------- CONFIG (env se change kar sakte ho) ----------------
BASE = "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/"
REFRESH_SEC = int(os.getenv("REFRESH_SEC", "1800"))      # 30 min
CONCURRENCY = int(os.getenv("CONCURRENCY", "300"))       # ek saath kitne test
TEST_TIMEOUT = float(os.getenv("TEST_TIMEOUT", "8"))     # test timeout (sec)
MAX_CANDIDATES = int(os.getenv("MAX_CANDIDATES", "10000"))
INCLUDE_ALL = os.getenv("INCLUDE_ALL", "0") == "1"       # all.txt bhi test kare (heavy)
DATA_FILE = os.getenv("DATA_FILE", "data.json")
KEEPALIVE_SEC = int(os.getenv("KEEPALIVE_SEC", "420"))   # har 7 min self-ping (Render 15 min me sota hai)
KEEPALIVE_URL = os.getenv("KEEPALIVE_URL") or os.getenv("RENDER_EXTERNAL_URL", "")
PING = {"last": 0, "ok": None, "count": 0}

HTTP_URLS = ["http://api.ipify.org", "http://icanhazip.com", "http://ifconfig.me/ip"]
HTTPS_URLS = ["https://api.ipify.org", "https://icanhazip.com", "https://ifconfig.me/ip"]

TYPES = ("http", "https", "both", "socks4", "socks5")
IP_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
PROXY_RE = re.compile(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$")


# ---------------- STORE (memory me, isliye response super fast) ----------------
class Store:
    def __init__(self):
        self.idx = {t: ([], []) for t in TYPES}  # type -> (latencies sorted, items)
        self.counts = {t: 0 for t in TYPES}
        self.updated_at = 0
        self.next_at = 0
        self.status = "warming_up"
        self.complete = False
        self.tested = 0
        self.total = 0

    def publish(self, results):
        buckets = {t: [] for t in TYPES}
        for r in results:
            h, s = r["http"], r["https"]
            if h is not None:
                buckets["http"].append((h, r["proxy"], r["t"]))
            if s is not None:
                buckets["https"].append((s, r["proxy"], r["t"]))
            if h is not None and s is not None:
                buckets["both"].append((max(h, s), r["proxy"], r["t"]))
            if r["socks4"] is not None:
                buckets["socks4"].append((r["socks4"], r["proxy"], r["t"]))
            if r["socks5"] is not None:
                buckets["socks5"].append((r["socks5"], r["proxy"], r["t"]))
        new_idx = {}
        for t, items in buckets.items():
            items.sort(key=lambda x: x[0])
            new_idx[t] = ([x[0] for x in items], items)
        self.idx = new_idx  # atomic swap: purani list yahin delete ho jati hai
        self.counts = {t: len(v[1]) for t, v in new_idx.items()}
        self.updated_at = int(time.time())


STORE = Store()


# ---------------- LOADER ----------------
async def fetch_lines(session, name):
    try:
        async with session.get(BASE + name, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                return set()
            text = await r.text()
    except Exception:
        return set()
    out = set()
    for line in text.splitlines():
        line = line.strip().split("://")[-1]
        if PROXY_RE.match(line):
            out.add(line)
    return out


async def load_jobs(session):
    names = ["http.txt", "https.txt", "socks4.txt", "socks5.txt", "live.txt"]
    if INCLUDE_ALL:
        names.append("all.txt")
    res = await asyncio.gather(*(fetch_lines(session, n) for n in names))
    d = dict(zip(names, res))
    socks4, socks5 = d["socks4.txt"], d["socks5.txt"]
    web = d["http.txt"] | d["https.txt"] | (d["live.txt"] - socks4 - socks5)
    if INCLUDE_ALL:
        web |= d["all.txt"] - socks4 - socks5

    jobs = {}
    for p in web:
        jobs.setdefault(p, set()).add("web")
    for p in socks4:
        jobs.setdefault(p, set()).add("socks4")
    for p in socks5:
        jobs.setdefault(p, set()).add("socks5")

    if len(jobs) > MAX_CANDIDATES:
        keys = random.sample(list(jobs), MAX_CANDIDATES)
        jobs = {k: jobs[k] for k in keys}
    return jobs


# ---------------- TESTER ----------------
async def probe_web(session, proxy, url):
    t0 = time.perf_counter()
    try:
        async with session.get(url, proxy=f"http://{proxy}") as r:
            if r.status != 200:
                return None
            body = (await r.content.read(64)).decode(errors="ignore").strip()
        if IP_RE.match(body):
            return round(time.perf_counter() - t0, 3)
    except Exception:
        pass
    return None


async def probe_socks(proxy, kind, url):
    t0 = time.perf_counter()
    try:
        conn = ProxyConnector.from_url(f"{kind}://{proxy}", rdns=True)
        async with aiohttp.ClientSession(
            connector=conn, timeout=aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        ) as s:
            async with s.get(url) as r:
                if r.status != 200:
                    return None
                body = (await r.content.read(64)).decode(errors="ignore").strip()
        if IP_RE.match(body):
            return round(time.perf_counter() - t0, 3)
    except Exception:
        pass
    return None


async def test_job(session, proxy, kinds):
    coros = {}
    if "web" in kinds:
        coros["http"] = probe_web(session, proxy, random.choice(HTTP_URLS))
        coros["https"] = probe_web(session, proxy, random.choice(HTTPS_URLS))
    for k in ("socks4", "socks5"):
        if k in kinds:
            coros[k] = probe_socks(proxy, k, random.choice(HTTPS_URLS))
    vals = await asyncio.gather(*coros.values())
    r = {"proxy": proxy, "http": None, "https": None, "socks4": None, "socks5": None,
         "t": int(time.time())}
    for k, v in zip(coros, vals):
        r[k] = v
    if any(r[k] is not None for k in ("http", "https", "socks4", "socks5")):
        return r
    return None


def save_disk(results):
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({"updated_at": int(time.time()), "results": results}, f)
    except Exception:
        pass


def load_disk():
    try:
        with open(DATA_FILE) as f:
            data = json.load(f)
        STORE.publish(data["results"])
        STORE.updated_at = data["updated_at"]
        STORE.complete = True
        STORE.status = "ready"
    except Exception:
        pass


async def run_cycle():
    STORE.status = "refreshing" if STORE.complete else "warming_up"
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600, force_close=True)
    timeout = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        jobs = await load_jobs(session)
        STORE.total, STORE.tested = len(jobs), 0
        results = []
        sem = asyncio.Semaphore(CONCURRENCY)
        last_pub = time.time()

        async def one(proxy, kinds):
            nonlocal last_pub
            async with sem:
                r = await test_job(session, proxy, kinds)
            STORE.tested += 1
            if r:
                results.append(r)
            # pehli baar: jaise jaise working milte hain, turant serve karna shuru
            if not STORE.complete and time.time() - last_pub > 2:
                last_pub = time.time()
                STORE.publish(results)

        await asyncio.gather(*(one(p, k) for p, k in jobs.items()))

    if results:
        STORE.publish(results)  # naya list; purana delete
        STORE.complete = True
        await asyncio.to_thread(save_disk, results)
    STORE.status = "ready"


async def refresher():
    while True:
        try:
            await run_cycle()
        except Exception as e:
            print("cycle error:", e)
            STORE.status = "ready" if STORE.complete else "warming_up"
        STORE.next_at = int(time.time()) + REFRESH_SEC
        await asyncio.sleep(REFRESH_SEC)


async def keepalive():
    """Khud ko public URL se ping karta hai taaki Render free plan so na jaye."""
    if not KEEPALIVE_URL:
        return
    url = KEEPALIVE_URL.rstrip("/") + "/health"
    await asyncio.sleep(45)
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        while True:
            try:
                async with s.get(url, headers={"User-Agent": "self-keepalive"}) as r:
                    PING["ok"] = r.status == 200
            except Exception:
                PING["ok"] = False
            PING["last"] = int(time.time())
            PING["count"] += 1
            await asyncio.sleep(KEEPALIVE_SEC)


@asynccontextmanager
async def lifespan(app):
    load_disk()
    tasks = [asyncio.create_task(refresher()), asyncio.create_task(keepalive())]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)


def jr(data, status=200):
    return Response(orjson.dumps(data), status_code=status, media_type="application/json")


def list_info():
    now = int(time.time())
    return {
        "status": STORE.status,
        "updated_at": STORE.updated_at,
        "age_sec": now - STORE.updated_at if STORE.updated_at else None,
        "next_refresh_in_sec": max(0, STORE.next_at - now) if STORE.next_at else None,
        "testing_progress": f"{STORE.tested}/{STORE.total}",
        "available": STORE.counts,
        "keepalive": {"enabled": bool(KEEPALIVE_URL), "last_ping": PING["last"],
                      "last_ok": PING["ok"], "pings": PING["count"]},
    }


# ---------------- API ----------------
def serve(ptype, sec, count, shuffle, fmt):
    t_start = time.perf_counter()
    ptype = ptype.lower()
    if ptype not in TYPES:
        return jr({"error": f"type galat hai. use: {', '.join(TYPES)}"}, 400)
    sec = min(max(sec, 0.01), 30.0)
    count = min(max(count, 1), 1000)

    lats, items = STORE.idx[ptype]
    n = bisect.bisect_right(lats, sec)  # sec se kam wale (list pehle se sorted hai)
    pool = items[:n]
    chosen = random.sample(pool, min(count, n)) if shuffle else pool[:count]
    chosen.sort(key=lambda x: x[0])

    if fmt == "txt":
        return Response("\n".join(x[1] for x in chosen), media_type="text/plain")

    if not STORE.complete and not chosen:
        msg = "list abhi ban rahi hai (warming up), 30-60 sec baad try karo"
    elif len(chosen) < count:
        msg = f"sirf {len(chosen)} proxy mile {sec}s se kam me (mange the {count})"
    else:
        msg = "ok"

    out = {
        "proxies": [x[1] for x in chosen],               # TOP: sirf proxies
        "count": len(chosen),
        "requested": count,
        "type": ptype,
        "max_sec": sec,
        "max_ms": int(sec * 1000),
        "available_under_sec": n,
        "message": msg,
        "details": [                                      # NECHE: time ke saath
            {"proxy": x[1], "latency_sec": x[0], "latency_ms": int(x[0] * 1000),
             "checked_at": x[2]} for x in chosen
        ],
        "list": list_info(),
    }
    ms_taken = round((time.perf_counter() - t_start) * 1000, 3)
    out["response_ms"] = ms_taken
    resp = jr(out)
    resp.headers["X-Response-ms"] = str(ms_taken)
    return resp


@app.get("/get")
async def get(type: str = "both", sec: float = 3.0, ms: float = 0, count: int = 10,
              shuffle: bool = False, format: str = "json"):
    if ms and ms > 0:        # ms diya to wahi use hoga (500 = 0.5 sec)
        sec = ms / 1000.0
    return serve(type, sec, count, shuffle, format)


@app.get("/stats")
async def stats():
    return jr(list_info())


@app.get("/health")
async def health():
    return jr({"ok": True, "status": STORE.status})


@app.get("/")
async def home():
    return jr({
        "usage": {
            "/get?type=https&sec=2&count=10": "https wale, 2 sec se kam, 10 proxy",
            "/get?type=http&sec=1.5&count=5": "http wale",
            "/get?type=https&sec=0.1&count=5": "0.1 sec (100 ms) se kam wale",
            "/get?type=http&ms=500&count=5": "milliseconds me: 500 ms se kam",
            "/get?type=both&sec=3&count=20": "jo http aur https dono me chalte hain",
            "/get?type=socks5&sec=3&count=5": "socks4 / socks5 bhi",
            "&shuffle=true": "random proxy (sab ko same top proxy na mile)",
            "&format=txt": "sirf ip:port lines",
            "/stats": "list ka status",
        },
        "list": list_info(),
    })
