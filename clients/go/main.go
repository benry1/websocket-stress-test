package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os/signal"
	"os"
	"bufio"
	"runtime"
	"runtime/metrics"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	json "github.com/goccy/go-json"
	"nhooyr.io/websocket"
)

type config struct {
	serverURL    string
	connections  int
	queueSize    int
	logInterval  time.Duration
	backoffBase  time.Duration
	backoffMax   time.Duration
	dialTimeout  time.Duration
	csvPath      string
}

type inboundMessage struct {
	Ts     int64   `json:"ts"`
	Seq    int64   `json:"seq"`
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Size   float64 `json:"size"`
	Pad    string  `json:"pad,omitempty"`
}

type kafkaRecord struct {
	Ts     int64
	Seq    int64
	Symbol string
	Price  float64
	Size   float64
}

var (
	ingestMsgs      uint64
	parsedOK        uint64
	parseErrors     uint64
	validationError uint64
	produceDropped  uint64
	consumed        uint64
	reconnects      uint64
	sequenceErrors  uint64
	activeConns     int64
)

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("starting Go client -> %s (conns=%d queue=%d)\n", cfg.serverURL, cfg.connections, cfg.queueSize)

	queue := make(chan kafkaRecord, cfg.queueSize)
	lastSeqs := make([]int64, cfg.connections)

	var wg sync.WaitGroup
	latencyCh := make(chan time.Duration, 100000)

	// Kafka mock consumer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		kafkaMock(ctx, queue)
	}()

	// Connection manager.
	for i := 0; i < cfg.connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			manageConnection(ctx, cfg, queue, latencyCh, &lastSeqs[id], id)
		}(i)
	}

	// Metrics loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		metricsLoop(ctx, queue, latencyCh, cfg.logInterval, cfg.csvPath)
	}()

	<-ctx.Done()
	stop()

	// Give goroutines a moment to exit.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-shutdownCtx.Done():
		log.Printf("shutdown timed out")
	}

	log.Printf("client stopped")
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.serverURL, "server", "ws://localhost:8080/stream", "WebSocket server URL (without seed param)")
	flag.IntVar(&cfg.connections, "connections", 200, "number of concurrent connections")
	flag.IntVar(&cfg.queueSize, "queue", 10000, "bounded queue size")
	flag.DurationVar(&cfg.logInterval, "logInterval", 1*time.Second, "metrics log interval")
	flag.DurationVar(&cfg.backoffBase, "backoffBase", 200*time.Millisecond, "reconnect backoff base")
	flag.DurationVar(&cfg.backoffMax, "backoffMax", 5*time.Second, "reconnect backoff max")
	flag.DurationVar(&cfg.dialTimeout, "dialTimeout", 5*time.Second, "dial timeout")
	flag.StringVar(&cfg.csvPath, "csv", "", "optional path to append CSV metrics")
	flag.Parse()
	return cfg
}

func manageConnection(ctx context.Context, cfg config, queue chan<- kafkaRecord, latencyCh chan<- time.Duration, lastSeq *int64, id int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*97))

	backoff := cfg.backoffBase
	targetURL := withSeed(cfg.serverURL, id+1)

	for {
		if ctx.Err() != nil {
			return
		}

		dialCtx, cancel := context.WithTimeout(ctx, cfg.dialTimeout)
		c, _, err := websocket.Dial(dialCtx, targetURL, nil)
		cancel()
		if err != nil {
			sleepWithJitter(ctx, backoff, rng)
			backoff = minDuration(cfg.backoffMax, backoff*2)
			continue
		}

		backoff = cfg.backoffBase // reset on successful dial
		atomic.AddInt64(&activeConns, 1)

		if err := readLoop(ctx, c, queue, latencyCh, lastSeq); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[conn %d] read loop ended: %v", id, err)
		}

		_ = c.Close(websocket.StatusNormalClosure, "closing")
		atomic.AddInt64(&activeConns, -1)

		if ctx.Err() != nil {
			return
		}

		atomic.AddUint64(&reconnects, 1)
		sleepWithJitter(ctx, backoff, rng)
		backoff = minDuration(cfg.backoffMax, backoff*2)
	}
}

func readLoop(ctx context.Context, c *websocket.Conn, queue chan<- kafkaRecord, latencyCh chan<- time.Duration, lastSeq *int64) error {
	for {
		start := time.Now()
		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}

		atomic.AddUint64(&ingestMsgs, 1)

		var msg inboundMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			atomic.AddUint64(&parseErrors, 1)
			continue
		}

		if err := validateMsg(msg); err != nil {
			atomic.AddUint64(&validationError, 1)
			continue
		}

		checkSeq(lastSeq, msg.Seq)
		atomic.AddUint64(&parsedOK, 1)

		record := kafkaRecord{
			Ts:     msg.Ts,
			Seq:    msg.Seq,
			Symbol: msg.Symbol,
			Price:  msg.Price,
			Size:   msg.Size,
		}

		var latency time.Duration
		select {
		case queue <- record:
			latency = time.Since(start)
		default:
			atomic.AddUint64(&produceDropped, 1)
			latency = time.Since(start)
		}

		select {
		case latencyCh <- latency:
		default:
			// drop latency samples if buffer is full to avoid backpressure
		}
	}
}

func validateMsg(m inboundMessage) error {
	if m.Ts == 0 {
		return errors.New("ts missing")
	}
	if m.Seq <= 0 {
		return errors.New("seq invalid")
	}
	if m.Symbol == "" {
		return errors.New("symbol missing")
	}
	if m.Price <= 0 {
		return errors.New("price invalid")
	}
	if m.Size <= 0 {
		return errors.New("size invalid")
	}
	return nil
}

func checkSeq(lastSeq *int64, seq int64) {
	prev := *lastSeq
	if prev != 0 && seq != prev+1 {
		atomic.AddUint64(&sequenceErrors, 1)
	}
	*lastSeq = seq
}

func kafkaMock(ctx context.Context, queue <-chan kafkaRecord) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-queue:
			// Optional lightweight serialization to exercise CPU path.
			_, _ = json.Marshal(rec)
			atomic.AddUint64(&consumed, 1)
		}
	}
}

func withSeed(base string, seed int) string {
	u, err := url.Parse(base)
	if err != nil {
		return fmt.Sprintf("%s?seed=%d", base, seed)
	}
	q := u.Query()
	q.Set("seed", fmt.Sprintf("%d", seed))
	u.RawQuery = q.Encode()
	return u.String()
}

func sleepWithJitter(ctx context.Context, d time.Duration, rng *rand.Rand) {
	if d <= 0 {
		return
	}
	window := int64(d) / 2
	if window <= 0 {
		window = 1
	}
	jitter := time.Duration(rng.Int63n(window))
	delay := d + jitter
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func metricsLoop(ctx context.Context, queue chan kafkaRecord, latencyCh <-chan time.Duration, interval time.Duration, csvPath string) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var csvWriter *os.File
	var csvBuf *bufio.Writer
	headerWritten := false
	if csvPath != "" {
		f, err := os.OpenFile(csvPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Printf("csv open error: %v", err)
		} else {
			csvWriter = f
			csvBuf = bufio.NewWriter(f)
			info, err := f.Stat()
			if err == nil && info.Size() > 0 {
				headerWritten = true
			}
		}
	}
	defer func() {
		if csvBuf != nil {
			csvBuf.Flush()
		}
		if csvWriter != nil {
			csvWriter.Close()
		}
	}()

	var prevIngest uint64
	var prevCPU time.Duration
	prevCPU = processCPUTime()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			ing := atomic.LoadUint64(&ingestMsgs)
			deltaIngest := ing - prevIngest
			prevIngest = ing

			latencies := drainLatencies(latencyCh)
			p50, p95 := percentile(latencies, 0.5), percentile(latencies, 0.95)

			cpuNow := processCPUTime()
			cpuDelta := cpuNow - prevCPU
			prevCPU = cpuNow
			cpuPct := float64(cpuDelta) / float64(interval) * 100

			rssMB := rssMegabytes()

			out := map[string]any{
				"ts":                   now.UTC().Format(time.RFC3339Nano),
				"connections_active":   atomic.LoadInt64(&activeConns),
				"msgs_in_total":        ing,
				"msgs_in_per_sec":      deltaIngest,
				"parse_errors_total":   atomic.LoadUint64(&parseErrors),
				"validation_errors":    atomic.LoadUint64(&validationError),
				"sequence_errors":      atomic.LoadUint64(&sequenceErrors),
				"reconnects_total":     atomic.LoadUint64(&reconnects),
				"queue_depth":          len(queue),
				"queue_capacity":       cap(queue),
				"queue_dropped_total":  atomic.LoadUint64(&produceDropped),
				"consumed_total":       atomic.LoadUint64(&consumed),
				"cpu_pct":              cpuPct,
				"rss_mb":               rssMB,
				"latency_ms_p50":       float64(p50.Microseconds()) / 1000,
				"latency_ms_p95":       float64(p95.Microseconds()) / 1000,
			}
			b, err := json.Marshal(out)
			if err != nil {
				log.Printf("metrics marshal err: %v", err)
				continue
			}
			fmt.Println(string(b))

			if csvBuf != nil {
				if !headerWritten {
					fmt.Fprintf(csvBuf, "ts,connections_active,msgs_in_total,msgs_in_per_sec,parse_errors_total,validation_errors,sequence_errors,reconnects_total,queue_depth,queue_capacity,queue_dropped_total,consumed_total,cpu_pct,rss_mb,latency_ms_p50,latency_ms_p95\n")
					headerWritten = true
				}
				fmt.Fprintf(csvBuf, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.4f,%.2f,%.4f,%.4f\n",
					out["ts"],
					out["connections_active"],
					out["msgs_in_total"],
					out["msgs_in_per_sec"],
					out["parse_errors_total"],
					out["validation_errors"],
					out["sequence_errors"],
					out["reconnects_total"],
					out["queue_depth"],
					out["queue_capacity"],
					out["queue_dropped_total"],
					out["consumed_total"],
					out["cpu_pct"],
					out["rss_mb"],
					out["latency_ms_p50"],
					out["latency_ms_p95"],
				)
				csvBuf.Flush()
			}
		}
	}
}

func drainLatencies(ch <-chan time.Duration) []time.Duration {
	// Non-blocking drain of available samples.
	var buf []time.Duration
	for {
		select {
		case v := <-ch:
			buf = append(buf, v)
		default:
			return buf
		}
	}
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	n := len(values)
	// simple selection by sorting for small buffers
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	k := int(p * float64(n-1))
	return values[k]
}

func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return user + sys
}

func rssMegabytes() float64 {
	// Best-effort: try runtime/metrics for current RSS; fallback to MemStats.Sys.
	const rssSample = "/memory/physical-memory:rss"
	samples := make([]metrics.Sample, 1)
	samples[0].Name = rssSample
	metrics.Read(samples)
	
	// metrics.Value has a Kind() method to determine type
	if samples[0].Value.Kind() == metrics.KindUint64 {
		if v := samples[0].Value.Uint64(); v > 0 {
			return float64(v) / (1024 * 1024)
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Sys) / (1024 * 1024)
}
