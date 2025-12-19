# Go Client

Go WebSocket listener that opens many connections, parses messages, validates minimal fields, and enqueues into a bounded Kafka-mock queue (drops on full).

## Prereqs

- Go 1.21+

## Run

```bash
cd clients/go
go run . \
  -server ws://localhost:8080/stream \
  -connections 200 \
  -queue 10000 \
  -logInterval 1s \
  -csv metrics.csv
```

Each connection appends `?seed=N` automatically; reconnects use jittered backoff. Metrics print periodically to stdout.

## Metrics

JSON line per `-logInterval` (default 1s) with:
- ts, connections_active
- msgs_in_total, msgs_in_per_sec
- parse_errors_total, validation_errors, sequence_errors
- reconnects_total
- queue_depth, queue_capacity, queue_dropped_total, consumed_total
- cpu_pct, rss_mb
- latency_ms_p50/p95 (message handling)

Optional: `-csv <path>` also appends metrics as CSV.
