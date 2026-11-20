// Consistent-hashing reverse proxy for CSCI 5144 Assignment 3.
// Routes requests to backend KV nodes using a SHA-256 ring with 100 virtual nodes per backend.
// BACKENDS env var: comma-separated host:port list (e.g. "kv1:8080,kv2:8080,kv3:8080").
// Endpoints: POST/GET/DELETE /{key}, GET / (health), GET /ring (debug).

package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const virtualNodes = 100

type ringEntry struct {
	hash uint64
	addr string
}

type ring struct {
	mu      sync.RWMutex
	entries []ringEntry
}

// addNode inserts virtualNodes entries for addr and re-sorts the ring.
func (r *ring) addNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < virtualNodes; i++ {
		h    := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", addr, i)))
		hash := binary.BigEndian.Uint64(h[:8])
		r.entries = append(r.entries, ringEntry{hash, addr})
	}
	sort.Slice(r.entries, func(i, j int) bool {
		return r.entries[i].hash < r.entries[j].hash
	})
}

// lookup returns the backend responsible for key using binary search with wrap-around.
func (r *ring) lookup(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return ""
	}
	h    := sha256.Sum256([]byte(key))
	hash := binary.BigEndian.Uint64(h[:8])
	idx  := sort.Search(len(r.entries), func(i int) bool {
		return r.entries[i].hash >= hash
	})
	if idx == len(r.entries) {
		idx = 0
	}
	return r.entries[idx].addr
}

// dump returns virtual-node counts per backend for the /ring endpoint.
func (r *ring) dump() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	counts := make(map[string]int)
	for _, e := range r.entries {
		counts[e.addr]++
	}
	return counts
}

type proxy struct {
	ring     *ring
	backends []string
	proxies  map[string]*httputil.ReverseProxy
}

func newProxy(backends []string) *proxy {
	r       := &ring{}
	proxies := make(map[string]*httputil.ReverseProxy, len(backends))
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}
	for _, addr := range backends {
		r.addNode(addr)
		target, _ := url.Parse("http://" + addr)
		rp        := httputil.NewSingleHostReverseProxy(target)
		rp.Transport = transport
		rp.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
			log.Printf("proxy error -> %s: %v", addr, err)
			http.Error(w, `{"error":"backend unavailable"}`, http.StatusBadGateway)
		}
		proxies[addr] = rp
	}
	return &proxy{ring: r, backends: backends, proxies: proxies}
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	// Health: aggregate status from all backends.
	if path == "" && r.Method == http.MethodGet {
		p.aggregateHealth(w)
		return
	}

	// Ring dump for debugging.
	if path == "ring" && r.Method == http.MethodGet {
		body, _ := json.Marshal(p.ring.dump())
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
		return
	}

	if path == "" {
		http.Error(w, `{"error":"key must not be empty"}`, http.StatusBadRequest)
		return
	}

	backend := p.ring.lookup(path)
	if backend == "" {
		http.Error(w, `{"error":"no backends configured"}`, http.StatusServiceUnavailable)
		return
	}
	rp, ok := p.proxies[backend]
	if !ok {
		http.Error(w, `{"error":"backend not found"}`, http.StatusInternalServerError)
		return
	}
	rp.ServeHTTP(w, r)
}

func (p *proxy) aggregateHealth(w http.ResponseWriter) {
	type nodeStatus struct {
		Addr   string `json:"addr"`
		Status string `json:"status"`
		Keys   int    `json:"keys,omitempty"`
		Error  string `json:"error,omitempty"`
	}
	nodes  := make([]nodeStatus, len(p.backends))
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 2 * time.Second}
	for i, addr := range p.backends {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			resp, err := client.Get("http://" + addr + "/")
			if err != nil {
				nodes[i] = nodeStatus{Addr: addr, Status: "unreachable", Error: err.Error()}
				return
			}
			defer resp.Body.Close()
			var data map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				nodes[i] = nodeStatus{Addr: addr, Status: "error", Error: err.Error()}
				return
			}
			keys := 0
			if v, ok := data["keys"]; ok {
				if f, ok := v.(float64); ok {
					keys = int(f)
				}
			}
			nodes[i] = nodeStatus{Addr: addr, Status: "ok", Keys: keys}
		}(i, addr)
	}
	wg.Wait()
	total := 0
	for _, n := range nodes {
		total += n.Keys
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status":     "ok",
		"nodes":      nodes,
		"total_keys": total,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func listenAddr() string {
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		return v
	}
	return "0.0.0.0:8000"
}

func backendList() []string {
	raw := os.Getenv("BACKENDS")
	if raw == "" {
		log.Fatal("BACKENDS env var is required (comma-separated host:port list)")
	}
	var addrs []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			addrs = append(addrs, p)
		}
	}
	if len(addrs) == 0 {
		log.Fatal("BACKENDS must contain at least one address")
	}
	return addrs
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC)
	backends := backendList()
	log.Printf("Starting proxy with %d backend(s): %v", len(backends), backends)

	p    := newProxy(backends)
	addr := listenAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler:           p,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Printf("Proxy listening on http://%s -> %v", addr, backends)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down proxy...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("Proxy stopped.")
}
