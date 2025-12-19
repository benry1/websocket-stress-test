# WebSocket Market Data Gateway Benchmarks

Purpose: provide a reproducible harness to compare WebSocket listeners across Go, Rust, TypeScript/Node, and C# for market data style workloads. All implementations share one message schema, identical per-message work (parse/validate/struct/enqueue to bounded queue), and common fault injection to avoid benchmarking different problems.

See `SPEC.md` for the detailed contract; this repo includes shared WebSocket test producers (Node in `producer/`, Go in `producer-go/`) plus language-specific clients and metrics scripts.***
