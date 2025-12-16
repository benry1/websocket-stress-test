/**
 * WebSocket producer to drive deterministic market-data-like load.
 *
 * Endpoints:
 *   - /stream?seed=X : steady stream per connection.
 *   - /burst?seed=X  : TODO: differentiate burst behavior; currently same as /stream. NOTE: refine if burst semantics change.
 *
 * CLI flags (all optional):
 *   --port            : TCP port (default 8080)
 *   --connections     : max concurrent connections (default 1000)
 *   --msgRatePerConn  : target messages per second per connection (default 500)
 *   --payloadBytes    : approximate payload size (adds pad field) (default 0 = no padding)
 *   --invalidRate     : fraction of messages sent as invalid JSON (default 0.001 = 0.1%)
 *   --disconnectRate  : probability per second to forcefully disconnect a connection (default 0.01)
 *   --logIntervalMs   : metrics log interval (default 5000)
 *   --backpressureBytes: bufferedAmount threshold to count backpressure (default 1_000_000 bytes)
 */

const http = require('http');
const { URL } = require('url');
const { WebSocketServer, WebSocket } = require('ws');

const defaultConfig = {
  port: 8080,
  connections: 1000,
  msgRatePerConn: 500,
  payloadBytes: 0,
  invalidRate: 0.001,
  disconnectRate: 0.01, // probability per second
  logIntervalMs: 5000,
  backpressureBytes: 1_000_000,
};

const metrics = {
  activeConnections: 0,
  totalConnections: 0,
  messagesSent: 0,
  invalidMessagesSent: 0,
  disconnects: 0,
  backpressureEvents: 0,
  startTime: Date.now(),
};

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/metrics')) {
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify(withRuntimeConfig(metrics)));
    return;
  }

  res.statusCode = 200;
  res.setHeader('content-type', 'text/plain');
  res.end('WebSocket producer ready. Connect via /stream?seed=1\n');
});

const config = { ...defaultConfig, ...parseArgs(process.argv.slice(2)) };
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const { pathname } = url;
  if (pathname !== '/stream' && pathname !== '/burst') {
    socket.destroy();
    return;
  }
  if (metrics.activeConnections >= config.connections) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    const params = {
      seed: Number(url.searchParams.get('seed')) || Date.now(),
      symbol: url.searchParams.get('symbol'),
      isBurst: pathname === '/burst',
    };
    wss.emit('connection', ws, req, params);
  });
});

wss.on('connection', (ws, _req, params) => {
  metrics.activeConnections += 1;
  metrics.totalConnections += 1;

  const seed = params.seed;
  const rng = createRng(seed);
  const symbol = params.symbol || `SYM${seed % 1000}`;
  const msgRate = params.isBurst ? config.msgRatePerConn * 2 : config.msgRatePerConn; // NOTE: burst doubles rate for now.

  const state = {
    seq: 0,
    symbol,
    rng,
    msgRatePerConn: msgRate,
    payloadBytes: config.payloadBytes,
    invalidRate: config.invalidRate,
    disconnectRate: config.disconnectRate,
    backpressureBytes: config.backpressureBytes,
    padTemplate: buildPadTemplate(Math.max(0, config.payloadBytes)),
  };

  const cleanup = startSendLoop(ws, state);

  ws.on('close', () => {
    metrics.activeConnections -= 1;
    cleanup();
  });

  ws.on('error', (err) => {
    console.error('client error:', err.message);
  });
});

server.listen(config.port, () => {
  console.log(
    `[producer] listening on :${config.port} | maxConns=${config.connections} | msgRatePerConn=${config.msgRatePerConn}/s | invalidRate=${config.invalidRate} | disconnectRate=${config.disconnectRate}/s`,
  );
});

setInterval(() => {
  const uptimeSec = (Date.now() - metrics.startTime) / 1000;
  const messagesPerSec = metrics.messagesSent / Math.max(1, uptimeSec);
  console.log(
    `[metrics] active=${metrics.activeConnections} sent=${metrics.messagesSent} (${messagesPerSec.toFixed(
      1,
    )}/s) invalid=${metrics.invalidMessagesSent} disconnects=${metrics.disconnects} backpressure=${metrics.backpressureEvents}`,
  );
}, config.logIntervalMs);

function startSendLoop(ws, state) {
  const tickMs = 100; // coarse-grained scheduling for rate control
  const disconnectRatePerTick = state.disconnectRate * (tickMs / 1000);
  let carry = 0;

  const timer = setInterval(() => {
    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }

    if (state.rng() < disconnectRatePerTick) {
      metrics.disconnects += 1;
      ws.close(1011, 'forced disconnect');
      return;
    }

    const target = state.msgRatePerConn * (tickMs / 1000) + carry;
    const toSend = Math.floor(target);
    carry = target - toSend;

    for (let i = 0; i < toSend; i += 1) {
      sendMessage(ws, state);
    }
  }, tickMs);

  return () => clearInterval(timer);
}

function sendMessage(ws, state) {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }

  if (ws.bufferedAmount > state.backpressureBytes) {
    metrics.backpressureEvents += 1;
    return;
  }

  state.seq += 1;
  const base = {
    ts: Date.now(),
    seq: state.seq,
    symbol: state.symbol,
    price: 100 + (state.seq % 10) + (state.symbol.length % 5), // deterministic-ish pricing
    size: 1 + (state.seq % 50),
  };

  let message;
  if (state.rng() < state.invalidRate) {
    message = '{"invalid":'; // intentionally broken JSON
    metrics.invalidMessagesSent += 1;
  } else {
    if (state.payloadBytes > 0) {
      const baseLen = JSON.stringify(base).length;
      const padLen = Math.max(0, state.payloadBytes - baseLen - 8); // 8 chars overhead for "pad":""
      if (padLen > 0) {
        base.pad = state.padTemplate.slice(0, padLen);
      }
    }
    message = JSON.stringify(base);
  }

  try {
    ws.send(message);
    metrics.messagesSent += 1;
  } catch (err) {
    console.error('send error:', err.message);
  }
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith('--')) continue;
    const [flag, inline] = arg.split('=');
    const key = flag.replace(/^--/, '');
    let val = inline;
    if (val === undefined) {
      const next = argv[i + 1];
      if (next && !next.startsWith('--')) {
        val = next;
        i += 1;
      } else {
        val = 'true';
      }
    }
    out[key] = coerce(val);
  }
  return out;
}

function coerce(val) {
  if (val === 'true') return true;
  if (val === 'false') return false;
  const num = Number(val);
  return Number.isNaN(num) ? val : num;
}

function createRng(seed) {
  let s = seed >>> 0 || 1;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0; // LCG
    return s / 0xffffffff;
  };
}

function buildPadTemplate(size) {
  return 'X'.repeat(Math.min(size, 1024 * 1024));
}

function withRuntimeConfig(meta) {
  return {
    ...meta,
    uptimeSec: ((Date.now() - meta.startTime) / 1000).toFixed(1),
    config,
  };
}
