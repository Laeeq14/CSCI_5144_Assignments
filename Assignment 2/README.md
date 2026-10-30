# Assignment 2 — Dockerized Key-Value Store

**CSCI 5144 — Cloud Computing**

This assignment wraps the Go key-value store from Assignment 1 in a Docker
container using a multi-stage build so the final image is small (~30 MB) and
ships no Go toolchain.

---

## Directory layout

```
Assignment 2/
├── Dockerfile            ← multi-stage build (Go builder + Python Alpine runtime)
├── .dockerignore         ← keeps the build context lean
├── docker-compose.yml    ← one-command run with volume persistence & health check
└── kv_store/
    ├── main.go           ← server (patched: LISTEN_ADDR env var, default 0.0.0.0:8080)
    ├── go.mod
    ├── loadgen/
    │   └── main.go
    ├── kv_bench.py
    ├── edge_cases.py
    ├── durability_test.py
    └── requirements.txt
```

---

## Quick start

### Option A — Docker Compose (recommended)

```bash
# Build the image and start the container in the foreground
docker compose up --build

# Or detached (background)
docker compose up --build -d

# Stop and remove the container (data volume is preserved)
docker compose down

# Stop AND delete the data volume (wipes stored keys)
docker compose down -v
```

### Option B — Plain `docker` commands

```bash
# 1. Build the image
docker build -t csci5144-kvstore:latest .

# 2. Run with a named volume for persistence
docker run --rm \
  -p 8080:8080 \
  -v kv_data:/data \
  --name kvstore \
  csci5144-kvstore:latest

# 3. Stop the running container (triggers graceful flush)
docker stop kvstore
```

---

## Verifying the server

Once the container is running, the server is reachable at `http://localhost:8080`.

### PowerShell (Windows)

PowerShell's `curl` is an alias for `Invoke-WebRequest` and uses different flags. Use the commands below instead.

**Health check**
```powershell
Invoke-WebRequest -Uri http://localhost:8080/ -UseBasicParsing
```

**POST a key**
```powershell
Invoke-WebRequest -Uri http://localhost:8080/hello -Method POST -ContentType "application/json" -Body '{"value":"world"}' -UseBasicParsing
```

Output:
```
StatusCode        : 201
StatusDescription : Created
Content           : {"key":"hello","value":"world","created":true}
```

**GET a key**
```powershell
Invoke-WebRequest -Uri http://localhost:8080/hello -Method GET -UseBasicParsing
```

Output:
```
StatusCode        : 200
StatusDescription : OK
Content           : {"value":"world"}
```

**DELETE a key**
```powershell
Invoke-WebRequest -Uri http://localhost:8080/hello -Method DELETE -UseBasicParsing
```

Output:
```
StatusCode        : 200
StatusDescription : OK
Content           : {"message":"key \"hello\" deleted"}
```

### curl.exe (Windows 10+ or Git Bash)

If you prefer curl syntax, call `curl.exe` explicitly to bypass the PowerShell alias:

```bash
curl.exe http://localhost:8080/
curl.exe -X POST http://localhost:8080/hello -H "Content-Type: application/json" -d "{\"value\":\"world\"}"
curl.exe http://localhost:8080/hello
curl.exe -X DELETE http://localhost:8080/hello
```

---

## Running the Python benchmark inside the container

```bash
# Open a shell in the running container
docker exec -it kvstore /bin/sh

# Inside the container — run the benchmark against the local server
cd /app
python kv_bench.py
python edge_cases.py
```

Or run it directly from the host (the container exposes port 8080):

```bash
pip install requests
python kv_store/kv_bench.py
```

---

## Design notes

### Why multi-stage?

| Stage   | Base image            | Purpose                                         |
|---------|-----------------------|-------------------------------------------------|
| builder | `golang:1.27-alpine`  | Compile the Go binary with `CGO_ENABLED=0`      |
| runtime | `python:3.11-alpine`  | Tiny runtime; no Go toolchain shipped in image  |

The compiled binary is statically linked (`CGO_ENABLED=0`), so it runs on any
Linux without a C library.  Python 3.11-alpine is chosen because the benchmark
scripts need Python, and Alpine keeps the image small.

### Listen address

Assignment 1 bound the server to `127.0.0.1:8080` (loopback only).  That
address is unreachable from outside a Docker container, so `main.go` was
patched to read the `LISTEN_ADDR` environment variable at startup:

```
default (Docker): LISTEN_ADDR=0.0.0.0:8080  (all interfaces)
local dev:        LISTEN_ADDR=127.0.0.1:8080 (loopback, Assignment 1 default)
```

### Persistence

The server writes `kv_store_data.json` and `kv_store.log` to `/data` (the
`PERSIST_DIR` environment variable).  The compose file mounts a named Docker
volume at `/data`, so data survives container restarts.

### Graceful shutdown

`docker stop` sends `SIGTERM`.  The server catches it, finishes in-flight
requests (5 s timeout), and flushes the store to disk before exiting.

---

## Environment variables

| Variable      | Default           | Description                              |
|---------------|-------------------|------------------------------------------|
| `LISTEN_ADDR` | `0.0.0.0:8080`    | TCP address the server binds to          |
| `PERSIST_DIR` | `/data`           | Directory for data and log files         |

---

## API reference

| Method | Path     | Body                  | Success                        |
|--------|----------|-----------------------|--------------------------------|
| POST   | `/{key}` | `{"value":"..."}`     | 201 (new key) / 200 (update)   |
| GET    | `/{key}` |                       | 200 `{"value":"..."}`          |
| DELETE | `/{key}` |                       | 200 `{"message":"..."}`        |
| GET    | `/`      |                       | 200 health check               |

Error responses use `{"error":"..."}` with status 400, 404, or 413.
Unsupported methods return 405 with a plain-text body.

