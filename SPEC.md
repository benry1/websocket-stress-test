# Market Data Gateway Benchmark — Specification

This repository standardizes a reproducible benchmark for evaluating WebSocket-based data gateway listeners across languages (Go, Rust, TypeScript/Node, C#). All implementations must follow this spec to avoid comparing different problems.

## Components

- **Test Producer (WebSocket server)**: Accepts many client connections, pushes messages at a controlled rate, injects faults, and periodically drops connections.
- **Language Listeners**: Go, Rust, TypeScript (Node), C# stubs that open hundreds of WebSocket connections, consume messages, perform identical per-message work, and mock-produce into an in-process bounded queue.
- **Metrics Harness**: Collects throughput, error, drop, and resource metrics for apples-to-apples comparison.

## Message Schema (identical across all)

JSON object per message:

```json
{
  "ts": 1700000000000,   // int64, epoch millis
  "seq": 123456789,      // int64, monotonically increasing per connection
  "symbol": "AAPL",      // string, non-empty
  "price": 189.42,       // float64
  "size": 100            // float64
}
```

Constraints:
- Required fields only (no optionals); reject messages missing any field.
- `price` and `size` > 0.
- `symbol` non-empty ASCII.

## Required Work Per Message (all listeners)

1. Read bytes from WebSocket.
2. Parse JSON into native structure.
3. Validate required fields and simple constraints (above).
4. Convert into an internal struct/record.
5. Mock “produce to Kafka”: enqueue into an in-process bounded queue with optional light serialization (e.g., JSON re-encode or fixed-width binary). Do not run Kafka.
6. If queue is full, record a dropped-produce metric (non-blocking drop or drop-oldest; keep policy consistent across languages).

## Error Injection (producer behavior)

- **Invalid JSON rate**: Default 0.1% of messages replaced with malformed JSON blobs. Rate must be configurable.
- **Disconnects**: Random disconnect per connection at a low rate (e.g., mean every 60–120 seconds). Forceful close without close-frame to exercise reconnect logic.
- **Backpressure**: Allow configurable send rate; when clients fall behind, do not buffer unboundedly—drop on the server side as needed.

## Client Responsibilities (per language)

- Establish N WebSocket connections (default: 200; configurable).
- Reconnect on disconnect with small jittered backoff.
- Handle incoming frames:
  - Parse and validate as above.
  - Count parse/validation failures separately from producer-injected invalid JSON.
  - Enqueue to bounded queue; track drops when queue is full.
- Shutdown cleanly on signal; flush metrics snapshot.

## Bounded Queue Contract

- Size: default 10_000 elements (configurable).
- Behavior when full: non-blocking drop with counter (`produce_dropped`). Keep policy identical across languages.
- Optional serialization before enqueue: CPU-light and consistent (e.g., `struct -> JSON bytes`).

## Metrics (must be exposed per run)

- `ingest_msgs_total`: messages received from sockets (before parse).
- `parsed_ok_total`: successfully parsed and validated.
- `parse_errors_total`: JSON parse failures.
- `validation_errors_total`: schema/constraint failures.
- `produce_dropped_total`: queue drops due to fullness.
- `reconnects_total`: reconnect attempts.
- `end_to_end_latency_ms_p50/p90/p99`: from producer send timestamp to enqueue (derive from `ts`).
- Resource: process CPU %, max RSS memory.
- Runtime config snapshot: connection count, queue size, producer send rate, invalid JSON rate, disconnect rate.

Expose metrics via stdout JSON summary at end of run; optionally a Prometheus scrape endpoint for long runs.

## Benchmark Procedure (common across languages)

1. Start the test producer with fixed settings (suggested defaults):
   - Message rate per connection: 500 msgs/s.
   - Invalid JSON rate: 0.1%.
   - Disconnect mean interval: 90s.
2. Start language listener with identical config:
   - Connections: 200.
   - Queue size: 10_000.
   - Serialization: JSON re-encode before enqueue (if enabled, must be enabled everywhere).
3. Warm-up: 30s (exclude from metrics).
4. Measurement window: 180s steady state.
5. Collect metrics snapshot at end; ensure no unbounded memory growth and low drop rate.

## Repository Layout

- `producer/`: Shared WebSocket test producer.
- `clients/go/`, `clients/rust/`, `clients/ts/`, `clients/csharp/`: Language listeners with identical CLI flags and behavior.
- `benchmarks/`: Scripts/config to launch producer + client with same parameters and capture metrics artifacts.
- `docs/`: Additional notes; this `SPEC.md`.

## Success Criteria

- Stable throughput during measurement window.
- No unbounded memory growth (bounded queues respected).
- Low drop rate (`produce_dropped_total` stays near zero at target rates).
- Resilient to malformed messages and periodic disconnects.

## Non-Goals (v1)

- No real Kafka, TLS, auth, or market data business logic.
- No cross-region latency modeling.
- No persistence of queued data beyond process memory.
