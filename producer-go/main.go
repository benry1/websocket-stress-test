package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type config struct {
	port             int
	maxConnections   int64
	msgRatePerConn   int
	payloadBytes     int
	invalidRate      float64
	disconnectRate   float64
	logInterval      time.Duration
	sendQueue        int
	burstMultiplier  float64
}

type metrics struct {
	activeConnections int64
	totalConnections  int64
	messagesSent      uint64
	invalidMessages   uint64
	disconnects       uint64
	backpressureDrops uint64
	startedAt         time.Time
}

type connectionState struct {
	seq          int64
	symbol       string
	msgRate      int
	invalidRate  float64
	disconnectP  float64
	payloadBytes int
	padTemplate  string
	rng          *rand.Rand
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	cfg := parseFlags()
	m := &metrics{startedAt: time.Now()}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		out := map[string]any{
			"active_connections": atomic.LoadInt64(&m.activeConnections),
			"messages_sent":      atomic.LoadUint64(&m.messagesSent),
			"invalid_sent":       atomic.LoadUint64(&m.invalidMessages),
			"disconnects":        atomic.LoadUint64(&m.disconnects),
			"backpressure_drops": atomic.LoadUint64(&m.backpressureDrops),
			"started_at":         m.startedAt.Format(time.RFC3339Nano),
			"config": map[string]any{
				"port":            cfg.port,
				"max_connections": cfg.maxConnections,
				"msg_rate_per_conn": cfg.msgRatePerConn,
				"payload_bytes":   cfg.payloadBytes,
				"invalid_rate":    cfg.invalidRate,
				"disconnect_rate": cfg.disconnectRate,
				"send_queue":      cfg.sendQueue,
				"burst_multiplier": cfg.burstMultiplier,
			},
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		handleWebsocket(w, r, cfg, m, false)
	})
	mux.HandleFunc("/burst", func(w http.ResponseWriter, r *http.Request) {
		handleWebsocket(w, r, cfg, m, true)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Go producer ready. Connect via /stream?seed=1\n"))
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go logLoop(cfg, m)

	log.Printf("[producer-go] listening on :%d | maxConns=%d | msgRatePerConn=%d/s | invalidRate=%.4f | disconnectRate=%.4f/s",
		cfg.port, cfg.maxConnections, cfg.msgRatePerConn, cfg.invalidRate, cfg.disconnectRate)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

func parseFlags() config {
	var cfg config
	flag.IntVar(&cfg.port, "port", 8081, "TCP port")
	flag.Int64Var(&cfg.maxConnections, "connections", 1000, "max concurrent connections")
	flag.IntVar(&cfg.msgRatePerConn, "msgRatePerConn", 500, "messages per second per connection")
	flag.IntVar(&cfg.payloadBytes, "payloadBytes", 0, "target payload size (adds pad)")
	flag.Float64Var(&cfg.invalidRate, "invalidRate", 0.001, "fraction of messages that are invalid JSON")
	flag.Float64Var(&cfg.disconnectRate, "disconnectRate", 0.01, "probability per second to force disconnect")
	flag.DurationVar(&cfg.logInterval, "logIntervalMs", 5*time.Second, "metrics log interval (ms)")
	flag.IntVar(&cfg.sendQueue, "sendQueue", 1024, "per-connection send queue (drops when full)")
	flag.Float64Var(&cfg.burstMultiplier, "burstMultiplier", 2.0, "rate multiplier for /burst")
	flag.Parse()
	return cfg
}

func handleWebsocket(w http.ResponseWriter, r *http.Request, cfg config, m *metrics, isBurst bool) {
	if atomic.LoadInt64(&m.activeConnections) >= cfg.maxConnections {
		http.Error(w, "max connections reached", http.StatusServiceUnavailable)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error: %v", err)
		return
	}

	atomic.AddInt64(&m.activeConnections, 1)
	atomic.AddInt64(&m.totalConnections, 1)

	seed := parseSeed(r, time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		symbol = fmt.Sprintf("SYM%d", seed%1000)
	}
	msgRate := cfg.msgRatePerConn
	if isBurst {
		msgRate = int(float64(msgRate) * cfg.burstMultiplier)
	}
	state := connectionState{
		seq:          0,
		symbol:       symbol,
		msgRate:      msgRate,
		invalidRate:  cfg.invalidRate,
		disconnectP:  cfg.disconnectRate,
		payloadBytes: cfg.payloadBytes,
		padTemplate:  strings.Repeat("X", 1_000_000),
		rng:          rng,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sendCh := make(chan []byte, cfg.sendQueue)

	go writerLoop(ctx, cancel, conn, sendCh)
	go generatorLoop(ctx, cancel, conn, sendCh, state, cfg, m)
	go readDiscard(ctx, cancel, conn)

	// wait for close
	<-ctx.Done()
	_ = conn.Close()
	atomic.AddInt64(&m.activeConnections, -1)
}

func generatorLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, sendCh chan<- []byte, state connectionState, cfg config, m *metrics) {
	tick := 100 * time.Millisecond
	disconnectPerTick := state.disconnectP * (float64(tick) / float64(time.Second))
	carry := 0.0
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if state.rng.Float64() < disconnectPerTick {
				atomic.AddUint64(&m.disconnects, 1)
				cancel()
				return
			}

			target := float64(state.msgRate)*(float64(tick)/float64(time.Second)) + carry
			toSend := int(math.Floor(target))
			carry = target - float64(toSend)

			for i := 0; i < toSend; i++ {
				msg, invalid := buildMessage(&state)
				if invalid {
					atomic.AddUint64(&m.invalidMessages, 1)
				}
				select {
				case sendCh <- msg:
				default:
					atomic.AddUint64(&m.backpressureDrops, 1)
				}
				atomic.AddUint64(&m.messagesSent, 1)
			}
		}
	}
}

func writerLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, sendCh <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sendCh:
			if !ok {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				cancel()
				return
			}
		}
	}
}

func readDiscard(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	conn.SetReadLimit(1024)
	conn.SetPongHandler(func(string) error { return nil })
	for {
		if ctx.Err() != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			cancel()
			return
		}
	}
}

func buildMessage(state *connectionState) ([]byte, bool) {
	state.seq++
	if state.rng.Float64() < state.invalidRate {
		return []byte(`{"invalid":`), true
	}

	base := map[string]any{
		"ts":     time.Now().UnixMilli(),
		"seq":    state.seq,
		"symbol": state.symbol,
		"price":  100 + float64(state.seq%10) + float64(len(state.symbol)%5),
		"size":   1 + float64(state.seq%50),
	}

	if state.payloadBytes > 0 {
		raw, _ := json.Marshal(base)
		padLen := state.payloadBytes - len(raw) - len(`,"pad":""`)
		if padLen > 0 {
			if padLen > len(state.padTemplate) {
				padLen = len(state.padTemplate)
			}
			base["pad"] = state.padTemplate[:padLen]
		}
	}
	msg, _ := json.Marshal(base)
	return msg, false
}

func parseSeed(r *http.Request, fallback int64) int64 {
	seedStr := r.URL.Query().Get("seed")
	if seedStr == "" {
		return fallback
	}
	var seed int64
	if _, err := fmt.Sscan(seedStr, &seed); err != nil {
		return fallback
	}
	return seed
}

func logLoop(cfg config, m *metrics) {
	ticker := time.NewTicker(cfg.logInterval)
	defer ticker.Stop()
	var prevSent uint64
	for range ticker.C {
		total := atomic.LoadUint64(&m.messagesSent)
		delta := total - prevSent
		prevSent = total
		log.Printf("[metrics] active=%d sent=%d (+%d) invalid=%d disconnects=%d drops=%d",
			atomic.LoadInt64(&m.activeConnections),
			total,
			delta,
			atomic.LoadUint64(&m.invalidMessages),
			atomic.LoadUint64(&m.disconnects),
			atomic.LoadUint64(&m.backpressureDrops),
		)
	}
}
