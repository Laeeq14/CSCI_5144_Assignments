# KV Store — CSCI 5144 Assignment 1

A single-server key-value store written in Go. Supports GET, PUT (POST), and DELETE over HTTP.

## Build and run

```bash
go build -o kvserver.exe -ldflags="-s -w" .
./kvserver.exe
```

The server listens on `http://127.0.0.1:8080`.

## API

| Method | Path | Body | Success |
|--------|------|------|---------|
| POST | `/{key}` | `{"value":"..."}` | 201 (new key) / 200 (update) |
| GET | `/{key}` | | 200 `{"value":"..."}` |
| DELETE | `/{key}` | | 200 `{"message":"..."}` |
| GET | `/` | | 200 health check |

Error responses use `{"error":"..."}` with an appropriate status code (400, 404, 405, 413).

## Design

**Concurrency.** An `sync.RWMutex` protects the in-memory map. GET operations hold a read lock; PUT and DELETE hold a write lock. Multiple GETs run in parallel.

**Persistence.** The store is saved to `kv_store_data.json` on two triggers: a periodic flush every 10 seconds, and a debounce flush within 2 seconds of any write. The file is written atomically (encode to a temp file, fsync, rename). On graceful shutdown (Ctrl+C or SIGTERM) a final forced flush runs before exit, so no writes are lost. A hard kill within the 2-second debounce window can lose the most recent writes; that is the documented loss window.

**Logging.** Every GET, PUT, and DELETE is logged to `kv_store.log` with an ISO-8601 timestamp. Logging is asynchronous: the handler sends to a buffered channel (65,536 entries) and a background goroutine writes to disk every 200ms. If the channel fills, entries are dropped and counted; the count is reported at shutdown. No drops were observed in any benchmark run.

**Response encoding.** JSON responses are built directly into pooled byte buffers with no intermediate allocations on the common case (pure ASCII values).

## Files

| File | Purpose |
|------|---------|
| `main.go` | Server implementation |
| `go.mod` | Go module file |
| `kv_bench.py` | Official benchmark (3 threads, 300 keys, read-back verification) |
| `edge_cases.py` | Correctness tests (18 cases, state-independent) |
| `durability_test.py` | Automated durability tests (graceful shutdown, force-kill scenarios) |
| `loadgen/` | Go load generator for measuring raw server throughput |

## Benchmark results

Run on Windows 11, Go 1.27.0, Python 3.14.4, laptop plugged in.

```
8 runs:  2062  2074  2082  2092  2110  2112  2121  2221  ops/s
Median:  ~2101 ops/s     Avg latency: ~1.41 ms
```

Per-phase (typical single run):
```
SET: ok=300 mismatch=0 error=0  throughput=2020 ops/s  avg=1.48ms  p99=2.37ms
GET: ok=300 mismatch=0 error=0  throughput=2136 ops/s  avg=1.40ms  p99=2.07ms
```

Data survived a Ctrl+C restart (`mismatch=0` after `set`, Ctrl+C, restart, `get`).

On battery the same benchmark gave 897--1,144 ops/s due to CPU and timer throttling.

The bottleneck is the Python benchmark client: the `requests` library adds ~0.37ms of CPU overhead per call, and three threads share the GIL. The server itself handles ~130,000 ops/s under a Go load generator with 50 goroutines (see `loadgen/`).
