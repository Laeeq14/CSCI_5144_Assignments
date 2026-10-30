# Stage 1: build the Go binary using an Alpine-based Go image
FROM golang:1.27-alpine AS builder

WORKDIR /build

# Copy go.mod first so this layer is cached separately from source changes
COPY kv_store/go.mod ./
RUN go mod download

# Copy Go source files
COPY kv_store/main.go ./
COPY kv_store/loadgen/ ./loadgen/

# Build a static, stripped binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o kvserver .

# Stage 2: minimal runtime image with Python for the benchmark scripts
FROM python:3.11-alpine

# Install CA certs and create a non-root user
RUN apk --no-cache add ca-certificates && \
    addgroup -S kvgroup && \
    adduser -S kvuser -G kvgroup

WORKDIR /app

# Copy the compiled server binary from the builder stage
COPY --from=builder /build/kvserver ./kvserver

# Copy Python scripts and their dependency list
COPY kv_store/kv_bench.py ./kv_bench.py
COPY kv_store/edge_cases.py ./edge_cases.py
COPY kv_store/durability_test.py ./durability_test.py
COPY kv_store/requirements.txt ./requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create the data directory and assign ownership to the non-root user
RUN mkdir -p /data && chown kvuser:kvgroup /data

USER kvuser

# Data files are written to PERSIST_DIR so they can be mounted as a volume
ENV PERSIST_DIR=/data

# Bind on all interfaces so Docker port mapping works
ENV LISTEN_ADDR=0.0.0.0:8080

EXPOSE 8080

# cd into the data dir then start the server
ENTRYPOINT ["/bin/sh", "-c", "cd ${PERSIST_DIR} && exec /app/kvserver"]
