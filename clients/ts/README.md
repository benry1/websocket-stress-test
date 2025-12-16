# TypeScript Client

Node.js/TypeScript WebSocket listener that opens many connections, parses and validates messages, enqueues into a bounded queue (drops on full), and emits JSON-line metrics once per second.

## Prereqs

- Node.js 18+
- Install deps: `npm install`

## Run

```bash
cd clients/ts
npm start -- \
  --server ws://localhost:8080/stream \
  --connections 200 \
  --queue 10000 \
  --logIntervalMs 1000
```

Each connection appends `?seed=N` automatically; reconnects use jittered backoff.

## Metrics (JSON line per interval)

- ts
- connections_active
- msgs_in_total, msgs_in_per_sec
- parse_errors_total, validation_errors, sequence_errors
- reconnects_total
- queue_depth, queue_capacity, queue_dropped_total, consumed_total
- cpu_pct, rss_mb
- latency_ms_p50, latency_ms_p95
