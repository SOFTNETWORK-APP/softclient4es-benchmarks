#!/usr/bin/env python3.12
"""Deterministic 10M-doc generator + bulk loader for bench_events_10m.

Loads via raw Elasticsearch REST (stdlib only) so NEITHER stack under test owns
the data path. Fixed seed => byte-identical dataset on every regeneration.

    python generator/generate_and_load.py              # the real 10,000,000-doc load
    python generator/generate_and_load.py --total 100000   # smoke load, NOT publishable

A smoke load is safe: every runner independently asserts its exact expected row
count, so a short index aborts the run instead of producing a quietly wrong number.
"""
import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request

ES = "http://localhost:9200"
INDEX = "bench_events_10m"
TOTAL = 10_000_000
BULK = 10_000
SEED = 20260831

STATUSES = ["created", "paid", "shipped", "delivered",
            "cancelled", "returned", "refunded", "pending"]
COUNTRIES = ["US", "FR", "DE", "GB", "ES", "IT", "NL", "BE", "SE", "NO",
             "DK", "FI", "PL", "PT", "IE", "AT", "CH", "CZ", "SK", "HU",
             "RO", "BG", "GR", "HR", "SI", "EE", "LV", "LT", "LU", "MT",
             "CY", "CA", "MX", "BR", "AR", "CL", "CO", "PE", "JP", "KR",
             "CN", "IN", "AU", "NZ", "ZA", "NG", "EG", "MA", "TR", "IL"]
CATEGORIES = [f"cat_{i:03d}" for i in range(100)]   # S3 GROUP BY key -> 100 groups
EPOCH_2026 = 1_767_225_600_000                       # 2026-01-01T00:00:00Z, epoch millis

MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0,
                 "refresh_interval": "-1"},
    "mappings": {"properties": {
        "id":       {"type": "long"},
        "event_ts": {"type": "date"},
        "amount":   {"type": "double"},
        "qty":      {"type": "integer"},
        "status":   {"type": "keyword"},
        "country":  {"type": "keyword"},
        "category": {"type": "keyword"},
        "name":     {"type": "keyword"},
    }},
}


def req(method, path, body=None, ctype="application/json", timeout=3600):
    data = body.encode() if isinstance(body, str) else body
    r = urllib.request.Request(f"{ES}{path}", data=data, method=method)
    if body is not None:
        r.add_header("Content-Type", ctype)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode() or "{}")


def bulk(body, attempts=5):
    """POST _bulk with backoff on Elasticsearch backpressure.

    A transient 429/503 -- or a dropped connection, which urllib surfaces as
    URLError rather than HTTPError -- must not kill a ~20-minute load.
    """
    for i in range(attempts):
        try:
            return req("POST", f"/{INDEX}/_bulk", body, "application/x-ndjson")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and i < attempts - 1:
                time.sleep(2 ** i)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if i < attempts - 1:
                time.sleep(2 ** i)
                continue
            raise


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--total", type=int, default=TOTAL,
                   help=f"documents to load (default {TOTAL:,})")
    total = p.parse_args().total
    if total != TOTAL:
        print(f"WARNING: loading {total:,} docs, not the benchmark's {TOTAL:,}. "
              "Runner asserts will fail on S1/S2 -- smoke use only, never publish.",
              file=sys.stderr)

    try:
        req("DELETE", f"/{INDEX}")
    except Exception:
        pass
    req("PUT", f"/{INDEX}", json.dumps(MAPPING))

    rng = random.Random(SEED)
    action = json.dumps({"index": {}})
    t0 = time.time()
    buf = []
    for i in range(total):
        doc = {
            "id": i,
            "event_ts": EPOCH_2026 + rng.randrange(365 * 24 * 3600) * 1000,
            "amount": round(rng.uniform(1.0, 999.99), 2),
            "qty": rng.randrange(1, 20),
            "status": rng.choice(STATUSES),
            "country": rng.choice(COUNTRIES),
            "category": rng.choice(CATEGORIES),
            "name": f"user_{rng.randrange(100_000):06d}",
        }
        buf.append(action)
        buf.append(json.dumps(doc, separators=(",", ":")))
        if (i + 1) % BULK == 0:
            resp = bulk("\n".join(buf) + "\n")
            if resp.get("errors"):
                print("bulk errors -- aborting", file=sys.stderr)
                sys.exit(1)
            buf.clear()
            if (i + 1) % 500_000 == 0:
                rate = (i + 1) / (time.time() - t0)
                print(f"{i + 1:,}/{total:,} docs ({rate:,.0f} docs/s)", flush=True)
    if buf:
        resp = bulk("\n".join(buf) + "\n")
        if resp.get("errors"):
            print("bulk errors -- aborting", file=sys.stderr)
            sys.exit(1)

    req("POST", f"/{INDEX}/_refresh")
    # One segment => stable, comparable read performance for BOTH stacks.
    req("POST", f"/{INDEX}/_forcemerge?max_num_segments=1")
    req("PUT", f"/{INDEX}/_settings",
        json.dumps({"index": {"refresh_interval": "1s"}}))
    count = req("GET", f"/{INDEX}/_count")["count"]
    assert count == total, f"expected {total} docs, got {count}"
    print(f"Loaded {count:,} docs in {time.time() - t0:,.0f}s")


if __name__ == "__main__":
    main()
