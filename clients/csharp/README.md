# C# Client

WebSocket listener in C# that opens many connections, parses/validates messages, enqueues into a bounded channel (drops on full), and emits JSON-line metrics once per second.

## Prereqs

- .NET 7.0 SDK+

## Run

```bash
cd clients/csharp
dotnet run -- \
  --server ws://localhost:8080/stream \
  --connections 200 \
  --queue 10000 \
  --logIntervalMs 1000 \
  --csv metrics.csv
```

Each connection appends `?seed=N` automatically; reconnects use jittered backoff. Metrics print as JSON lines to stdout.

## Metrics (JSON line per interval)

- ts
- connections_active
- msgs_in_total, msgs_in_per_sec
- parse_errors_total, validation_errors, sequence_errors
- reconnects_total
- queue_depth, queue_capacity, queue_dropped_total, consumed_total
- cpu_pct, rss_mb
- latency_ms_p50, latency_ms_p95

Optional: `--csv <path>` also appends metrics as CSV.
