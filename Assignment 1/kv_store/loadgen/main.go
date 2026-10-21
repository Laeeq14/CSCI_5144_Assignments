// loadgen/main.go — Go goroutine-based load generator for the KV store
// Measures true server throughput, independent of the Python GIL.
//
// Usage (from kv_store/ directory):
//   go run ./loadgen                              # 50 goroutines, 2000 ops each
//   go run ./loadgen -goroutines 100 -ops 5000
//   go run ./loadgen -goroutines 200 -phase set

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	goroutines := flag.Int("goroutines", 50, "number of concurrent goroutines")
	opsPerG := flag.Int("ops", 2000, "operations per goroutine")
	phase := flag.String("phase", "both", "set | get | both")
	baseURL := flag.String("url", "http://127.0.0.1:8080", "server base URL")
	flag.Parse()

	// Shared transport — pre-warmed connection pool, no dial overhead on hot path
	transport := &http.Transport{
		MaxIdleConns:        *goroutines * 2,
		MaxIdleConnsPerHost: *goroutines * 2,
		IdleConnTimeout:     120 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}

	numKeys := *goroutines * *opsPerG

	run := func(op string) (throughput, avgMs, p99Ms float64, ok int64) {
		var (
			errCount  int64
			mu        sync.Mutex
			latencies = make([]float64, 0, numKeys)
			wg        sync.WaitGroup
			barrier   = make(chan struct{})
		)

		workerFn := func(start, end int) {
			defer wg.Done()
			<-barrier
			lats := make([]float64, 0, end-start)
			buf := []byte(`{"value":"bench_value"}`)

			for i := start; i < end; i++ {
				key := fmt.Sprintf("gkey_%d", i)
				t0 := time.Now()
				var reqErr error

				switch op {
				case "set":
					req, _ := http.NewRequest(http.MethodPost, *baseURL+"/"+key, bytes.NewReader(buf))
					req.Header.Set("Content-Type", "application/json")
					resp, err := client.Do(req)
					if err == nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
					reqErr = err
				case "get":
					resp, err := client.Get(*baseURL + "/" + key)
					if err == nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
					reqErr = err
				}

				ms := float64(time.Since(t0).Microseconds()) / 1000.0
				if reqErr != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					lats = append(lats, ms)
				}
			}
			mu.Lock()
			latencies = append(latencies, lats...)
			mu.Unlock()
		}

		wg.Add(*goroutines)
		for g := 0; g < *goroutines; g++ {
			start := g * *opsPerG
			go workerFn(start, start+*opsPerG)
		}
		t0 := time.Now()
		close(barrier) // release all goroutines simultaneously
		wg.Wait()
		elapsed := time.Since(t0).Seconds()

		sort.Float64s(latencies)
		n := len(latencies)
		ok = int64(n)
		if n > 0 {
			var sum float64
			for _, v := range latencies {
				sum += v
			}
			avgMs = sum / float64(n)
			p99Ms = latencies[int(0.99*float64(n-1))]
		}
		throughput = float64(n) / elapsed
		return
	}

	fmt.Printf("Go load generator — %d goroutines × %d ops = %d total per phase\n",
		*goroutines, *opsPerG, numKeys)
	fmt.Println("NOTE: this measures SERVER capacity, not the Python-GIL-limited kv_bench.py client.")
	fmt.Println()

	phases := []string{"set", "get"}
	switch *phase {
	case "set":
		phases = []string{"set"}
	case "get":
		phases = []string{"get"}
	}

	var totalOK int64
	var totalTime float64
	for _, p := range phases {
		fmt.Printf("  Running %s phase...\n", p)
		tput, avg, p99, ok := run(p)
		fmt.Printf("  %-4s: ok=%-8d  throughput=%8.0f ops/s  avg=%.3f ms  p99=%.3f ms\n",
			p, ok, tput, avg, p99)
		totalOK += ok
		totalTime += float64(ok) / tput
	}

	fmt.Printf("\nSERVER CAPACITY: %d ops in %.2fs → %.0f ops/s\n",
		totalOK, totalTime, float64(totalOK)/totalTime)

	// Quick correctness round-trip
	body := []byte(`{"value":"verify_me"}`)
	req, _ := http.NewRequest(http.MethodPost, *baseURL+"/verify_key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	client.Do(req) //nolint:errcheck

	resp, err := client.Get(*baseURL + "/verify_key")
	if err == nil {
		defer resp.Body.Close()
		var v struct {
			Value string `json:"value"`
		}
		json.NewDecoder(resp.Body).Decode(&v) //nolint:errcheck
		if v.Value == "verify_me" {
			fmt.Println("Correctness: PASS ✓")
		} else {
			fmt.Printf("Correctness: FAIL (got %q)\n", v.Value)
		}
	}
}
