# WebSocket Producer

Deterministic WebSocket load generator to exercise Market Data Gateway listeners.

## Prereqs

- Node.js 18+
- Install deps: `npm install`

## Run

```bash
cd producer
npm start -- --port 8080 --connections 200 --msgRatePerConn 500 --payloadBytes 0 --invalidRate 0.001 --disconnectRate 0.01
```

Connect clients to `ws://localhost:8080/stream?seed=1` (or `/burst?seed=1`). Optional `symbol` query overrides the generated symbol.

Metrics are exposed at `http://localhost:8080/metrics` and logged periodically to stdout.

## Flags

- `--port`: TCP port (default `8080`).
- `--connections`: max concurrent connections (default `1000`).
- `--msgRatePerConn`: target messages/sec per connection (default `500`).
- `--payloadBytes`: approximate payload size; adds `pad` string to each message (default `0` = no padding).
- `--invalidRate`: fraction of messages sent as invalid JSON (default `0.001`).
- `--disconnectRate`: probability per second to forcefully drop a connection (default `0.01`).
- `--logIntervalMs`: metrics log interval (default `5000` ms).
- `--backpressureBytes`: bufferedAmount threshold to count backpressure events (default `1_000_000` bytes).

## Notes

- Message shape: `{ts, seq, symbol, price, size, pad?}`. `pad` is included only when `payloadBytes > 0` and may slightly exceed the target size due to JSON overhead.
- `/burst` currently doubles the configured `msgRatePerConn` for that connection. TODO: refine burst semantics if a different profile is desired.
