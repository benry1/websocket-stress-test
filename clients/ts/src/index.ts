import WebSocket from "ws";
import simdjson from "simdjson";
import fs from "fs";

type Config = {
  server: string;
  connections: number;
  queueSize: number;
  logIntervalMs: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  csvPath?: string;
};

type InboundMessage = {
  ts: number;
  seq: number;
  symbol: string;
  price: number;
  size: number;
  pad?: string;
};

type KafkaRecord = {
  ts: number;
  seq: number;
  symbol: string;
  price: number;
  size: number;
};

const config: Config = parseArgs(process.argv.slice(2));

let ingestMsgs = 0;
let parsedOk = 0;
let parseErrors = 0;
let validationErrors = 0;
let sequenceErrors = 0;
let reconnects = 0;
let produceDropped = 0;
let consumed = 0;
let activeConnections = 0;

const queue: KafkaRecord[] = [];
const latencySamples: number[] = [];
const lastSeq: number[] = new Array(config.connections).fill(0);
const stopSignals = new AbortController();

// Start Kafka mock consumer.
setImmediate(function consumeLoop() {
  if (stopSignals.signal.aborted) return;
  let iterations = 0;
  while (queue.length > 0 && iterations < 2000) {
    const rec = queue.shift();
    if (rec) {
      // Optional lightweight serialization to mimic CPU path.
      JSON.stringify(rec);
      consumed += 1;
    }
    iterations += 1;
  }
  setTimeout(consumeLoop, queue.length > 0 ? 0 : 1);
});

// Connection manager.
for (let i = 0; i < config.connections; i++) {
  manageConnection(i);
}

// Metrics loop (JSON line per interval).
metricsLoop();

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function manageConnection(idx: number) {
  let backoff = config.backoffBaseMs;
  const rng = randomSource(Date.now() + idx * 97);
  const url = withSeed(config.server, idx + 1);

  const connect = () => {
    if (stopSignals.signal.aborted) return;
    const ws = new WebSocket(url);

    ws.on("open", () => {
      activeConnections += 1;
      backoff = config.backoffBaseMs;
    });

    ws.on("message", (data: WebSocket.RawData) => {
      const start = process.hrtime.bigint();
      ingestMsgs += 1;
      const text = typeof data === "string" ? data : data.toString("utf8");
      let msg: InboundMessage;
      try {
        msg = simdjson.parse(text);
      } catch {
        parseErrors += 1;
        return;
      }

      if (!validate(msg)) {
        validationErrors += 1;
        return;
      }

      checkSeq(idx, msg.seq);
      parsedOk += 1;

      const record: KafkaRecord = {
        ts: msg.ts,
        seq: msg.seq,
        symbol: msg.symbol,
        price: msg.price,
        size: msg.size,
      };

      if (queue.length >= config.queueSize) {
        produceDropped += 1;
      } else {
        queue.push(record);
      }

      const end = process.hrtime.bigint();
      const latencyMs = Number(end - start) / 1_000_000;
      if (latencySamples.length < 200000) {
        latencySamples.push(latencyMs);
      }
    });

    ws.on("close", () => {
      activeConnections -= 1;
      scheduleReconnect();
    });

    ws.on("error", () => {
      ws.close();
    });
  };

  const scheduleReconnect = () => {
    if (stopSignals.signal.aborted) return;
    reconnects += 1;
    const jitter = rng() * backoff * 0.5;
    const delay = backoff + jitter;
    backoff = Math.min(config.backoffMaxMs, backoff * 2);
    setTimeout(connect, delay);
  };

  connect();
}

function metricsLoop() {
  let prevIngest = 0;
  let prevCPU = process.cpuUsage();
  let prevHrtime = process.hrtime.bigint();

  let csvStream: fs.WriteStream | null = null;
  let headerWritten = false;
  if (config.csvPath) {
    try {
      const exists = fs.existsSync(config.csvPath);
      csvStream = fs.createWriteStream(config.csvPath, { flags: "a" });
      headerWritten = exists && fs.statSync(config.csvPath).size > 0;
    } catch (err) {
      console.error("csv open error", err);
      csvStream = null;
    }
  }

  const emit = () => {
    if (stopSignals.signal.aborted) return;
    const now = new Date();
    const ing = ingestMsgs;
    const delta = ing - prevIngest;
    prevIngest = ing;

    const usage = process.cpuUsage(prevCPU);
    prevCPU = process.cpuUsage();
    const nowHr = process.hrtime.bigint();
    const intervalNs = Number(nowHr - prevHrtime);
    prevHrtime = nowHr;
    const intervalSec = intervalNs / 1e9;
    const cpuPct =
      ((usage.user + usage.system) / 1e6 / (intervalSec || 1)) * 100;

    const rssMb = process.memoryUsage().rss / (1024 * 1024);

    const samples = drainLatencies();
    const p50 = percentile(samples, 0.5);
    const p95 = percentile(samples, 0.95);

    const out = {
      ts: now.toISOString(),
      connections_active: activeConnections,
      msgs_in_total: ing,
      msgs_in_per_sec: delta,
      parse_errors_total: parseErrors,
      validation_errors: validationErrors,
      sequence_errors: sequenceErrors,
      reconnects_total: reconnects,
      queue_depth: queue.length,
      queue_capacity: config.queueSize,
      queue_dropped_total: produceDropped,
      consumed_total: consumed,
      cpu_pct: cpuPct,
      rss_mb: rssMb,
      latency_ms_p50: p50,
      latency_ms_p95: p95,
    };
    process.stdout.write(JSON.stringify(out) + "\n");

    if (csvStream) {
      if (!headerWritten) {
        csvStream.write(
          "ts,connections_active,msgs_in_total,msgs_in_per_sec,parse_errors_total,validation_errors,sequence_errors,reconnects_total,queue_depth,queue_capacity,queue_dropped_total,consumed_total,cpu_pct,rss_mb,latency_ms_p50,latency_ms_p95\n"
        );
        headerWritten = true;
      }
      csvStream.write(
        `${out.ts},${out.connections_active},${out.msgs_in_total},${
          out.msgs_in_per_sec
        },${out.parse_errors_total},${out.validation_errors},${
          out.sequence_errors
        },${out.reconnects_total},${out.queue_depth},${out.queue_capacity},${
          out.queue_dropped_total
        },${out.consumed_total},${out.cpu_pct.toFixed(4)},${out.rss_mb.toFixed(
          2
        )},${out.latency_ms_p50.toFixed(4)},${out.latency_ms_p95.toFixed(4)}\n`
      );
    }
    setTimeout(emit, config.logIntervalMs);
  };

  setTimeout(emit, config.logIntervalMs);
}

function drainLatencies(): number[] {
  if (latencySamples.length === 0) return [];
  const copy = latencySamples.splice(0, latencySamples.length);
  return copy;
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = arr.slice().sort((a, b) => a - b);
  const idx = Math.floor(p * (sorted.length - 1));
  return sorted[idx];
}

function validate(m: InboundMessage): boolean {
  if (!m || typeof m !== "object") return false;
  if (!Number.isFinite(m.ts) || m.ts === 0) return false;
  if (!Number.isFinite(m.seq) || m.seq <= 0) return false;
  if (typeof m.symbol !== "string" || m.symbol.length === 0) return false;
  if (!Number.isFinite(m.price) || m.price <= 0) return false;
  if (!Number.isFinite(m.size) || m.size <= 0) return false;
  return true;
}

function checkSeq(idx: number, seq: number) {
  const prev = lastSeq[idx];
  if (prev !== 0 && seq !== prev + 1) {
    sequenceErrors += 1;
  }
  lastSeq[idx] = seq;
}

function withSeed(base: string, seed: number): string {
  const url = new URL(base);
  url.searchParams.set("seed", String(seed));
  return url.toString();
}

function shutdown() {
  stopSignals.abort();
  setTimeout(() => process.exit(0), 500);
}

function parseArgs(argv: string[]): Config {
  const out: Partial<Config> = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const [k, vInline] = arg.split("=");
    const key = k.replace(/^--/, "");
    let val = vInline;
    if (
      val === undefined &&
      i + 1 < argv.length &&
      !argv[i + 1].startsWith("--")
    ) {
      val = argv[++i];
    }
    if (!val) continue;
    switch (key) {
      case "server":
        out.server = val;
        break;
      case "connections":
        out.connections = Number(val);
        break;
      case "queue":
      case "queueSize":
        out.queueSize = Number(val);
        break;
      case "logInterval":
      case "logIntervalMs":
        out.logIntervalMs = Number(val);
        break;
      case "backoffBase":
      case "backoffBaseMs":
        out.backoffBaseMs = Number(val);
        break;
      case "backoffMax":
      case "backoffMaxMs":
        out.backoffMaxMs = Number(val);
        break;
      case "csv":
        out.csvPath = val;
        break;
    }
  }
  return {
    server: out.server ?? "ws://localhost:8080/stream",
    connections: out.connections ?? 200,
    queueSize: out.queueSize ?? 10000,
    logIntervalMs: out.logIntervalMs ?? 1000,
    backoffBaseMs: out.backoffBaseMs ?? 200,
    backoffMaxMs: out.backoffMaxMs ?? 5000,
    csvPath: out.csvPath,
  };
}

function randomSource(seed: number) {
  let s = seed >>> 0 || 1;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0xffffffff;
  };
}
