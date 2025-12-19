# Go WebSocket Producer

High-throughput WebSocket producer rewritten in Go to drive benchmark clients.

## Prereqs

- Go 1.21+

## Run

```bash
cd producer-go
go run . \
  -port 8081 \
  -connections 2000 \
  -msgRatePerConn 500 \
  -payloadBytes 0 \
  -invalidRate 0.001 \
  -disconnectRate 0.01 \
  -sendQueue 1024 \
  -logIntervalMs 5s
```

Connect clients to `ws://localhost:8081/stream?seed=1` (or `/burst?seed=1`, which multiplies rate by `-burstMultiplier`).
Metrics endpoint: `http://localhost:8081/metrics`.

## Flags

- `-port`: TCP port (default 8081).
- `-connections`: max concurrent connections (default 1000).
- `-msgRatePerConn`: messages/sec per connection (default 500).
- `-payloadBytes`: pad messages to approximate size (default 0).
- `-invalidRate`: fraction of messages emitted as invalid JSON (default 0.001).
- `-disconnectRate`: probability per second to forcefully drop a connection (default 0.01).
- `-sendQueue`: per-connection send queue; drops when full (default 1024).
- `-burstMultiplier`: rate multiplier for `/burst` (default 2.0).
- `-logIntervalMs`: metrics log interval (default 5s).
