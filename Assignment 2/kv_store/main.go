// Single-server key-value store, CSCI 5144 Assignment 2 (Dockerized).
// Listen address is read from LISTEN_ADDR env var (default 0.0.0.0:8080).
// API: POST /{key}, GET /{key}, DELETE /{key}, GET / (health).

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultListenAddr = "0.0.0.0:8080"
	persistFile       = "kv_store_data.json"
	persistInterval   = 10 * time.Second
	writeDelay        = 2 * time.Second
	maxBodyBytes      = 1 << 20
	logChanSize       = 65536
)

// listenAddr returns LISTEN_ADDR env var or the default address.
func listenAddr() string {
	if addr := os.Getenv("LISTEN_ADDR"); addr != "" {
		return addr
	}
	return defaultListenAddr
}

// asyncLogger writes op records to a file on a background goroutine.
type asyncLogger struct {
	ch      chan string
	done    chan struct{}
	dropped int64
}

func newAsyncLogger(path string) *asyncLogger {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("cannot open log file: %v", err)
	}
	al := &asyncLogger{
		ch:   make(chan string, logChanSize),
		done: make(chan struct{}),
	}
	go func() {
		defer f.Close()
		bw := bufio.NewWriterSize(f, 128*1024)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case msg, ok := <-al.ch:
				if !ok {
					bw.Flush()
					close(al.done)
					return
				}
				bw.WriteString(msg)
				bw.WriteByte('\n')
			case <-tick.C:
				bw.Flush()
			}
		}
	}()
	return al
}

func (al *asyncLogger) logOp(op, key, result string) {
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	msg := ts + " | " + op + " key=" + key + " " + result
	select {
	case al.ch <- msg:
	default:
		atomic.AddInt64(&al.dropped, 1)
	}
}

func (al *asyncLogger) close() {
	close(al.ch)
	<-al.done
	if d := atomic.LoadInt64(&al.dropped); d > 0 {
		log.Printf("WARNING: logger dropped %d entries", d)
	}
}

// store is a concurrent in-memory map protected by an RWMutex.
type store struct {
	mu        sync.RWMutex
	data      map[string]string
	dirty     int32
	writeHint chan struct{}
}

func newStore() *store {
	return &store{
		data:      make(map[string]string, 1024),
		writeHint: make(chan struct{}, 1),
	}
}

func (s *store) get(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

// put stores a value and returns true if the key is new.
func (s *store) put(key, value string) bool {
	s.mu.Lock()
	_, exists := s.data[key]
	s.data[key] = value
	s.mu.Unlock()
	atomic.StoreInt32(&s.dirty, 1)
	select {
	case s.writeHint <- struct{}{}:
	default:
	}
	return !exists
}

func (s *store) del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	if ok {
		atomic.StoreInt32(&s.dirty, 1)
		select {
		case s.writeHint <- struct{}{}:
		default:
		}
	}
	return ok
}

func (s *store) size() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

func (s *store) snapshot() map[string]string {
	s.mu.RLock()
	snap := make(map[string]string, len(s.data))
	for k, v := range s.data {
		snap[k] = v
	}
	s.mu.RUnlock()
	return snap
}

// save atomically writes the store to disk using a temp file and rename.
func (s *store) save(path string, force bool) {
	if !force && atomic.LoadInt32(&s.dirty) == 0 {
		return
	}
	// Clear before snapshot; a concurrent write will set it again.
	atomic.StoreInt32(&s.dirty, 0)

	snap := s.snapshot()

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Printf("ERROR persist create: %v", err)
		atomic.StoreInt32(&s.dirty, 1)
		return
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	encErr := enc.Encode(snap)
	syncErr := f.Sync()
	f.Close()

	if encErr != nil {
		log.Printf("ERROR persist encode: %v", encErr)
		atomic.StoreInt32(&s.dirty, 1)
		return
	}
	if syncErr != nil {
		log.Printf("WARN persist sync: %v", syncErr)
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("ERROR persist rename: %v", err)
		atomic.StoreInt32(&s.dirty, 1)
	}
}

func (s *store) load(path string) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		log.Printf("No persistence file %q, starting fresh.", path)
		return
	}
	if err != nil {
		log.Printf("WARN load: %v", err)
		return
	}
	defer f.Close()
	var snap map[string]string
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		log.Printf("WARN decode: %v", err)
		return
	}
	s.mu.Lock()
	for k, v := range snap {
		s.data[k] = v
	}
	s.mu.Unlock()
	atomic.StoreInt32(&s.dirty, 0)
	log.Printf("Loaded %d key(s) from %q.", len(snap), path)
}

// persistLoop flushes on a periodic ticker, a write hint, or a stop signal.
func (s *store) persistLoop(path string, interval time.Duration, stop <-chan struct{}) {
	periodic := time.NewTicker(interval)
	defer periodic.Stop()
	var debounce <-chan time.Time
	for {
		select {
		case <-s.writeHint:
			if debounce == nil {
				debounce = time.After(writeDelay)
			}
		case <-debounce:
			debounce = nil
			s.save(path, false)
		case <-periodic.C:
			debounce = nil
			s.save(path, false)
		case <-stop:
			s.save(path, true)
			return
		}
	}
}

// bufPool reduces GC pressure by reusing response byte slices.
var bufPool = sync.Pool{New: func() interface{} {
	b := make([]byte, 0, 256)
	return &b
}}

func getbuf() *[]byte  { return bufPool.Get().(*[]byte) }
func putbuf(b *[]byte) { *b = (*b)[:0]; bufPool.Put(b) }

// jsonAppendString appends a quoted JSON string; fast path for printable ASCII.
func jsonAppendString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7E || c == '"' || c == '\\' {
			b, _ := json.Marshal(s)
			return append(dst, b...)
		}
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	dst = append(dst, '"')
	return dst
}

func buildValueResp(dst []byte, v string) []byte {
	dst = append(dst, `{"value":`...)
	dst = jsonAppendString(dst, v)
	return append(dst, '}')
}

func buildErrResp(dst []byte, msg string) []byte {
	dst = append(dst, `{"error":`...)
	dst = jsonAppendString(dst, msg)
	return append(dst, '}')
}

func buildMsgResp(dst []byte, msg string) []byte {
	dst = append(dst, `{"message":`...)
	dst = jsonAppendString(dst, msg)
	return append(dst, '}')
}

func buildPutResp(dst []byte, key, value string, created bool) []byte {
	dst = append(dst, `{"key":`...)
	dst = jsonAppendString(dst, key)
	dst = append(dst, `,"value":`...)
	dst = jsonAppendString(dst, value)
	if created {
		return append(dst, `,"created":true}`...)
	}
	return append(dst, `,"created":false}`...)
}

// cachedDate holds a pre-formatted Date header, updated once per second.
const ctJSON = "application/json"

var cachedDate atomic.Value

func startDateCache() {
	update := func() { cachedDate.Store(time.Now().UTC().Format(http.TimeFormat)) }
	update()
	go func() {
		for range time.NewTicker(time.Second).C {
			update()
		}
	}()
}

func sendJSON(w http.ResponseWriter, code int, body []byte) {
	h := w.Header()
	h.Set("Content-Type", ctJSON)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h["Date"] = []string{cachedDate.Load().(string)}
	w.WriteHeader(code)
	w.Write(body) //nolint:errcheck
}

type handler struct {
	s   *store
	log *asyncLogger
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	// Health endpoint.
	if path == "" && r.Method == http.MethodGet {
		body, _ := json.Marshal(map[string]interface{}{
			"status":    "ok",
			"keys":      h.s.size(),
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
		sendJSON(w, http.StatusOK, body)
		return
	}

	if path == "" {
		buf := getbuf()
		defer putbuf(buf)
		*buf = buildErrResp(*buf, "key must not be empty")
		sendJSON(w, http.StatusBadRequest, *buf)
		return
	}

	key := path
	buf := getbuf()
	defer putbuf(buf)

	switch r.Method {

	case http.MethodGet:
		val, ok := h.s.get(key)
		if !ok {
			h.log.logOp("GET", key, "NOT_FOUND")
			*buf = buildErrResp(*buf, fmt.Sprintf("key %q not found", key))
			sendJSON(w, http.StatusNotFound, *buf)
			return
		}
		h.log.logOp("GET", key, "found")
		*buf = buildValueResp(*buf, val)
		sendJSON(w, http.StatusOK, *buf)

	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		// Use *string to distinguish a missing field from an empty string.
		var body struct {
			Value *string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			if err.Error() == "http: request body too large" {
				*buf = buildErrResp(*buf, "request body too large (max 1 MiB)")
				sendJSON(w, http.StatusRequestEntityTooLarge, *buf)
			} else {
				*buf = buildErrResp(*buf, "invalid JSON body: "+err.Error())
				sendJSON(w, http.StatusBadRequest, *buf)
			}
			return
		}
		if body.Value == nil {
			*buf = buildErrResp(*buf, `body must contain a string "value" field`)
			sendJSON(w, http.StatusBadRequest, *buf)
			return
		}
		value := *body.Value
		isNew := h.s.put(key, value)
		status := http.StatusOK
		if isNew {
			status = http.StatusCreated
			h.log.logOp("PUT", key, "created")
		} else {
			h.log.logOp("PUT", key, "updated")
		}
		*buf = buildPutResp(*buf, key, value, isNew)
		sendJSON(w, status, *buf)

	case http.MethodDelete:
		ok := h.s.del(key)
		if !ok {
			h.log.logOp("DEL", key, "NOT_FOUND")
			*buf = buildErrResp(*buf, fmt.Sprintf("key %q not found", key))
			sendJSON(w, http.StatusNotFound, *buf)
			return
		}
		h.log.logOp("DEL", key, "deleted")
		*buf = buildMsgResp(*buf, fmt.Sprintf("key %q deleted", key))
		sendJSON(w, http.StatusOK, *buf)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// tcpKeepAlive wraps a listener to enable TCP keep-alive on every connection.
type tcpKeepAlive struct{ net.Listener }

func (l tcpKeepAlive) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}
	return c, nil
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC)
	startDateCache()

	addr := listenAddr()

	al := newAsyncLogger("kv_store.log")
	kv := newStore()
	kv.load(persistFile)

	stopPersist := make(chan struct{})
	persistDone := make(chan struct{})
	go func() {
		kv.persistLoop(persistFile, persistInterval, stopPersist)
		close(persistDone)
	}()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler:           &handler{s: kv, log: al},
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("KV-store listening on http://%s", addr)
		if err := srv.Serve(tcpKeepAlive{ln}); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	close(stopPersist)
	<-persistDone

	al.close()
	log.Println("Done.")
}

