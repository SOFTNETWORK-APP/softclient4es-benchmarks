#!/usr/bin/env python3
"""Attribute Flight SQL connect latency to a layer, one probe per layer.

⚠️ This RE-RUNS a settled diagnosis; it does not make one. softclient4es-arrow#151
root-caused connect latency on 2026-08-10 with this same layer decomposition
(raw TCP 0.04 ms, pyarrow C++ 1.6 ms, ADBC-Go by literal 3.1 ms, ADBC-Go by name
92 ms with 5.2/10.1 s outliers) and closed COMPLETED. Read its closing comment
before citing it -- the issue BODY lists open questions that the comment answers,
and quoting the body alone is how this file came to claim the cause was unknown.

It is kept because a settled cause still needs a current measurement: the harness
changed (every stack now dials a literal), and #151's own prediction -- "the
residual true handshake is ~3 ms, comparable to Trino's lazy connect, so the S4
wall conclusion likely flips on re-measure" -- is a claim about a rerun, not a
finding already banked.

What this probe adds over #151's: it separates the FIRST connection in a fresh
process from the steady state. Every measured run is a fresh process, so the ~190 ms
first ADBC connect (driver load, not network) lands on S4 in full, and a
60-connects-in-one-process probe amortises it away.

Four layers against the SAME listening socket, so each line subtracts the one above:

  tcp            socket.create_connection      -- the server's accept path, nothing else
  flight-cpp-ip  pyarrow.flight (C++/gRPC)     -- + gRPC channel + Flight handshake
  adbc-go-ip     adbc_driver_flightsql (Go)    -- + the Go driver, no name to resolve
  adbc-go-name   same, target "localhost"      -- + grpc-go's resolver

Every connection is FRESH (no channel reuse) because that is the cost a short-lived
client actually pays; a pooled client pays it once and is the documented mitigation.

⚠️ This is a PROBE, not a scenario: it publishes no wall-clock for any query and is
therefore exempt from guard_environment(), which protects TIMINGS. It still records
host load and memory pressure so a reader can judge the conditions -- and it writes
its raw samples, because a figure quoted from an unrecorded probe is not publishable
(the rule this campaign adopted after quoting one).

    python runners/probe_connect.py --repeat 30 --out results/<session>/connect-probe.json
"""
import argparse
import json
import pathlib
import socket
import statistics as st
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from scenarios import HOST, host_load, memory_pressure  # noqa: E402

PORT = 32010
NAME = "localhost"


def probe_tcp(target):
    t0 = time.perf_counter()
    s = socket.create_connection((target, PORT))
    dt = time.perf_counter() - t0
    s.close()
    return dt


def probe_flight_cpp(target):
    import pyarrow.flight as fl
    t0 = time.perf_counter()
    c = fl.FlightClient(f"grpc://{target}:{PORT}")
    c.wait_for_available(timeout=10)      # force the channel to be READY, not lazy
    dt = time.perf_counter() - t0
    c.close()
    return dt


def probe_adbc(target):
    from adbc_driver_flightsql import dbapi
    t0 = time.perf_counter()
    conn = dbapi.connect(f"grpc://{target}:{PORT}")
    cur = conn.cursor()                   # the handshake the benchmark's connect_s covers
    dt = time.perf_counter() - t0
    cur.close()
    conn.close()
    return dt


LAYERS = [
    ("tcp",            lambda: probe_tcp(HOST)),
    ("flight-cpp-ip",  lambda: probe_flight_cpp(HOST)),
    ("adbc-go-ip",     lambda: probe_adbc(HOST)),
    ("adbc-go-name",   lambda: probe_adbc(NAME)),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repeat", type=int, default=30)
    p.add_argument("--out")
    a = p.parse_args()

    record = {"probe": "connect-latency", "port": PORT, "ip": HOST, "name": NAME,
              "repeat": a.repeat, "layers": {},
              "host_load": host_load(), "mem_pressure": memory_pressure()}

    print(f"{'layer':16}{'n':>4}{'median':>10}{'min':>10}{'max':>10}{'p90':>10}   (ms)")
    for name, fn in LAYERS:
        samples, errors = [], []
        for _ in range(a.repeat):
            try:
                samples.append(fn() * 1000.0)
            except Exception as e:                  # a layer that cannot run is recorded,
                errors.append(f"{type(e).__name__}: {e}")   # never silently absent
        if samples:
            ordered = sorted(samples)
            p90 = ordered[min(len(ordered) - 1, int(round(0.9 * (len(ordered) - 1))))]
            record["layers"][name] = {"n": len(samples), "samples_ms": samples,
                                      "median_ms": st.median(samples),
                                      "min_ms": min(samples), "max_ms": max(samples),
                                      "p90_ms": p90, "errors": errors}
            print(f"{name:16}{len(samples):>4}{st.median(samples):>10.2f}"
                  f"{min(samples):>10.2f}{max(samples):>10.2f}{p90:>10.2f}")
        else:
            record["layers"][name] = {"n": 0, "errors": errors}
            print(f"{name:16}{0:>4}   FAILED  {errors[:1]}")

    if a.out:
        out = pathlib.Path(a.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(record, indent=2))
        print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
