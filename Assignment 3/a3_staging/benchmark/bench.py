# Benchmark throughput and latency across 1, 2, and 3 KV nodes using consistent hashing.
# Routes requests directly to node host ports (8081, 8082, 8083) using a Python SHA-256 ring.
# Run: py benchmark/bench.py

import csv
import hashlib
import os
import queue
import struct
import sys
import threading
import time

import requests

NUM_THREADS   = 8
NUM_KEYS      = 500
WARMUP_KEYS   = 20
VIRTUAL_NODES = 100

ALL_BACKENDS = ["localhost:8081", "localhost:8082", "localhost:8083"]
CONFIGS      = {1: ALL_BACKENDS[:1], 2: ALL_BACKENDS[:2], 3: ALL_BACKENDS[:3]}


class HashRing:
    # SHA-256 consistent hash ring with virtual nodes, matching proxy/main.go logic.
    def __init__(self, backends):
        self._ring = []
        for addr in backends:
            for i in range(VIRTUAL_NODES):
                h    = hashlib.sha256(f"{addr}#{i}".encode()).digest()
                hval = struct.unpack(">Q", h[:8])[0]
                self._ring.append((hval, addr))
        self._ring.sort(key=lambda x: x[0])

    def lookup(self, key):
        hval = struct.unpack(">Q", hashlib.sha256(key.encode()).digest()[:8])[0]
        for entry_hash, addr in self._ring:
            if entry_hash >= hval:
                return addr
        return self._ring[0][1]


_local    = threading.local()
_log_lock = threading.Lock()
_logged   = 0


def log_problem(msg):
    global _logged
    with _log_lock:
        _logged += 1
        if _logged <= 5:
            print(msg, file=sys.stderr)
        elif _logged == 6:
            print("  (further errors suppressed)", file=sys.stderr)


def session():
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
    return _local.s


def kv_op(op, host_port, key, value):
    url = f"http://{host_port}/{key}"
    try:
        if op == "set":
            session().post(url, json={"value": value}, timeout=10).raise_for_status()
            return "ok"
        if op == "get":
            r   = session().get(url, timeout=10)
            r.raise_for_status()
            got = r.json().get("value")
            if got == value:
                return "ok"
            log_problem(f"Mismatch {key}: expected {value!r}, got {got!r}")
            return "mismatch"
    except Exception as e:
        log_problem(f"Error {op} {key} -> {host_port}: {e}")
        return "error"


class Stats:
    def __init__(self):
        self.lock      = threading.Lock()
        self.latencies = []
        self.mismatch  = 0
        self.error     = 0


def worker(q, stats, barrier):
    barrier.wait()
    while True:
        try:
            op, host_port, key, value = q.get_nowait()
        except queue.Empty:
            return
        t0     = time.perf_counter()
        status = kv_op(op, host_port, key, value)
        lat    = time.perf_counter() - t0
        with stats.lock:
            if status == "ok":
                stats.latencies.append(lat)
            elif status == "mismatch":
                stats.mismatch += 1
            else:
                stats.error += 1


def run_phase(op, routed_ops):
    q       = queue.Queue()
    for item in routed_ops:
        q.put((op,) + item)
    stats   = Stats()
    barrier = threading.Barrier(NUM_THREADS + 1)
    workers = [threading.Thread(target=worker, args=(q, stats, barrier))
               for _ in range(NUM_THREADS)]
    for t in workers:
        t.start()
    barrier.wait()
    t0 = time.perf_counter()
    for t in workers:
        t.join()
    return stats, time.perf_counter() - t0


def pct(lats, p):
    return lats[int(p / 100 * (len(lats) - 1))] if lats else float("nan")


def wait_for(backends):
    for addr in backends:
        for _ in range(20):
            try:
                requests.get(f"http://{addr}/", timeout=1)
                break
            except Exception:
                time.sleep(0.5)


def benchmark(n_nodes):
    backends = CONFIGS[n_nodes]
    ring     = HashRing(backends)
    print(f"\n{'='*60}")
    print(f"  Benchmarking with {n_nodes} node(s): {backends}")
    print(f"{'='*60}")
    wait_for(backends)

    kvs     = [(f"key_{i}", f"value_{i}") for i in range(NUM_KEYS)]
    route   = lambda pairs: [(ring.lookup(k), k, v) for k, v in pairs]

    # Warmup
    run_phase("set", route([(f"warm_{n_nodes}_{i}", f"v_{i}") for i in range(WARMUP_KEYS)]))

    set_ops            = route(kvs)
    set_stats, set_t   = run_phase("set", set_ops)
    get_stats, get_t   = run_phase("get", route(kvs))

    dist = {}
    for hp, _, _ in set_ops:
        dist[hp] = dist.get(hp, 0) + 1
    print(f"  Key distribution: {dict(sorted(dist.items()))}")

    results = []
    for phase, stats, elapsed in [("SET", set_stats, set_t), ("GET", get_stats, get_t)]:
        lats = sorted(stats.latencies)
        ok   = len(lats)
        tput = ok / elapsed if elapsed > 0 else 0
        avg  = 1e3 * sum(lats) / ok if ok else float("nan")
        p50, p95, p99 = 1e3 * pct(lats, 50), 1e3 * pct(lats, 95), 1e3 * pct(lats, 99)
        print(
            f"  {phase}: ok={ok} err={stats.error} mismatch={stats.mismatch} "
            f"time={elapsed:.2f}s tput={tput:.1f} ops/s "
            f"avg={avg:.2f}ms p50={p50:.2f}ms p95={p95:.2f}ms p99={p99:.2f}ms"
        )
        results.append({
            "nodes": n_nodes, "phase": phase, "ok": ok,
            "error": stats.error, "mismatch": stats.mismatch,
            "time_s": round(elapsed, 4), "tput": round(tput, 2),
            "avg_ms": round(avg, 3), "p50_ms": round(p50, 3),
            "p95_ms": round(p95, 3), "p99_ms": round(p99, 3),
        })
    return results


def main():
    all_rows = []
    for n in [1, 2, 3]:
        all_rows.extend(benchmark(n))

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv")
    fields = ["nodes", "phase", "ok", "error", "mismatch",
              "time_s", "tput", "avg_ms", "p50_ms", "p95_ms", "p99_ms"]
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\nResults written to {out}")
    print("Run: py benchmark/plot.py")


if __name__ == "__main__":
    main()
