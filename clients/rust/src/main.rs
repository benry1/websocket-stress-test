use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use sysinfo::System;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Rust WebSocket listener for benchmarks", long_about = None)]
struct Config {
    #[arg(long, default_value = "ws://localhost:8080/stream")]
    server: String,
    #[arg(long, default_value_t = 200)]
    connections: usize,
    #[arg(long, default_value_t = 10_000, alias = "queueSize")]
    queue: usize,
    #[arg(long = "logIntervalMs", default_value_t = 1_000, alias = "log-interval-ms")]
    log_interval_ms: u64,
    #[arg(long = "backoffBaseMs", default_value_t = 200, alias = "backoff-base-ms")]
    backoff_base_ms: u64,
    #[arg(long = "backoffMaxMs", default_value_t = 5_000, alias = "backoff-max-ms")]
    backoff_max_ms: u64,
    #[arg(long = "dialTimeoutMs", default_value_t = 5_000, alias = "dial-timeout-ms")]
    dial_timeout_ms: u64,
    #[arg(long, default_value = "", alias = "csv")]
    csv: String,
}

#[derive(Deserialize)]
struct InboundMessage {
    ts: i64,
    seq: i64,
    symbol: String,
    price: f64,
    size: f64,
}

#[derive(Clone, Copy, Serialize)]
struct KafkaRecord {
    ts: i64,
    seq: i64,
    symbol_len: usize,
    price: f64,
    size: f64,
}

#[derive(Serialize)]
struct MetricsOut {
    ts: String,
    connections_active: i64,
    msgs_in_total: u64,
    msgs_in_per_sec: u64,
    parse_errors_total: u64,
    validation_errors: u64,
    sequence_errors: u64,
    reconnects_total: u64,
    queue_depth: i64,
    queue_capacity: usize,
    queue_dropped_total: u64,
    consumed_total: u64,
    cpu_pct: f64,
    rss_mb: f64,
    latency_ms_p50: f64,
    latency_ms_p95: f64,
}

#[tokio::main]
async fn main() {
    let cfg = Config::parse();

    let ingest_msgs = Arc::new(AtomicU64::new(0));
    let parsed_ok = Arc::new(AtomicU64::new(0));
    let parse_errors = Arc::new(AtomicU64::new(0));
    let validation_errors = Arc::new(AtomicU64::new(0));
    let sequence_errors = Arc::new(AtomicU64::new(0));
    let reconnects = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let active_conns = Arc::new(AtomicI64::new(0));
    let queue_depth = Arc::new(AtomicI64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(100_000)));
    let last_seqs: Arc<Vec<AtomicI64>> = Arc::new((0..cfg.connections).map(|_| AtomicI64::new(0)).collect());

    let (tx, mut rx) = mpsc::channel::<KafkaRecord>(cfg.queue);

    let cancel = CancellationToken::new();
    let cancel_consumer = cancel.clone();
    let consumed_clone = Arc::clone(&consumed);
    let queue_depth_consumer = Arc::clone(&queue_depth);
    let csv_path = cfg.csv.clone();

    tokio::spawn(async move {
        kafka_mock(&mut rx, consumed_clone, queue_depth_consumer, cancel_consumer).await;
    });

    for i in 0..cfg.connections {
        let cfg_conn = cfg.clone();
        let tx_conn = tx.clone();
        let ingest_conn = Arc::clone(&ingest_msgs);
        let parsed_conn = Arc::clone(&parsed_ok);
        let parse_err_conn = Arc::clone(&parse_errors);
        let val_err_conn = Arc::clone(&validation_errors);
        let seq_err_conn = Arc::clone(&sequence_errors);
        let recon_conn = Arc::clone(&reconnects);
        let active_conn = Arc::clone(&active_conns);
        let drop_conn = Arc::clone(&dropped);
        let lat_conn = Arc::clone(&latencies);
        let queue_depth_conn = Arc::clone(&queue_depth);
        let last_seq_conn = Arc::clone(&last_seqs);
        let cancel_conn = cancel.clone();

        tokio::spawn(async move {
            manage_connection(
                i,
                cfg_conn,
                tx_conn,
                ingest_conn,
                parsed_conn,
                parse_err_conn,
                val_err_conn,
                seq_err_conn,
                recon_conn,
                active_conn,
                drop_conn,
                lat_conn,
                queue_depth_conn,
                last_seq_conn,
                cancel_conn,
            )
            .await;
        });
    }
    drop(tx);

    let metrics_cancel = cancel.clone();
    let metrics_cfg = cfg.clone();
    let metrics_handle = tokio::spawn(async move {
        metrics_loop(
            metrics_cfg,
            ingest_msgs,
            parse_errors,
            validation_errors,
            sequence_errors,
            reconnects,
            dropped,
            consumed,
            active_conns,
            queue_depth,
            latencies,
            metrics_cancel,
            csv_path,
        )
        .await;
    });

    // Wait for shutdown signal.
    let _ = tokio::signal::ctrl_c().await;
    cancel.cancel();
    let _ = metrics_handle.await;
}

async fn manage_connection(
    idx: usize,
    cfg: Config,
    mut tx: mpsc::Sender<KafkaRecord>,
    ingest: Arc<AtomicU64>,
    parsed: Arc<AtomicU64>,
    parse_errors: Arc<AtomicU64>,
    validation_errors: Arc<AtomicU64>,
    sequence_errors: Arc<AtomicU64>,
    reconnects: Arc<AtomicU64>,
    active_conns: Arc<AtomicI64>,
    dropped: Arc<AtomicU64>,
    latencies: Arc<Mutex<Vec<f64>>>,
    queue_depth: Arc<AtomicI64>,
    last_seqs: Arc<Vec<AtomicI64>>,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_millis(cfg.backoff_base_ms);
    let backoff_max = Duration::from_millis(cfg.backoff_max_ms);
    let mut rng_state = 1u64.wrapping_add(idx as u64 * 97);

    let base_url = cfg.server.clone();
    let target_url = with_seed(&base_url, idx + 1);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let connect_fut = connect_async(&target_url);
        let conn_result = tokio::time::timeout(Duration::from_millis(cfg.dial_timeout_ms), connect_fut).await;
        let (ws_stream, _) = match conn_result {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => {
                sleep_with_jitter(backoff, &mut rng_state, &cancel).await;
                backoff = std::cmp::min(backoff * 2, backoff_max);
                continue;
            }
            Err(_) => {
                sleep_with_jitter(backoff, &mut rng_state, &cancel).await;
                backoff = std::cmp::min(backoff * 2, backoff_max);
                continue;
            }
        };

        backoff = Duration::from_millis(cfg.backoff_base_ms);
        active_conns.fetch_add(1, Ordering::Relaxed);

        if let Err(e) = read_loop(
            ws_stream,
            &mut tx,
            ingest.as_ref(),
            parsed.as_ref(),
            parse_errors.as_ref(),
            validation_errors.as_ref(),
            sequence_errors.as_ref(),
            dropped.as_ref(),
            latencies.clone(),
            queue_depth.as_ref(),
            &last_seqs,
            idx,
            &cancel,
        )
        .await
        {
            eprintln!("[conn {idx}] read loop ended: {e}");
        }

        active_conns.fetch_sub(1, Ordering::Relaxed);

        if cancel.is_cancelled() {
            break;
        }
        reconnects.fetch_add(1, Ordering::Relaxed);
        sleep_with_jitter(backoff, &mut rng_state, &cancel).await;
        backoff = std::cmp::min(backoff * 2, backoff_max);
    }
}

async fn read_loop(
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    tx: &mut mpsc::Sender<KafkaRecord>,
    ingest: &AtomicU64,
    parsed: &AtomicU64,
    parse_errors: &AtomicU64,
    validation_errors: &AtomicU64,
    sequence_errors: &AtomicU64,
    dropped: &AtomicU64,
    latencies: Arc<Mutex<Vec<f64>>>,
    queue_depth: &AtomicI64,
    last_seqs: &Vec<AtomicI64>,
    idx: usize,
    cancel: &CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut ws = ws;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = ws.next() => {
                let Some(msg) = msg else { break };
                let msg = msg?;
                let start = Instant::now();
                match msg {
                    Message::Text(txt) => {
                        ingest.fetch_add(1, Ordering::Relaxed);
                        handle_message(&txt, parsed, parse_errors, validation_errors, sequence_errors, dropped, latencies.clone(), queue_depth, last_seqs, idx, tx, start).await;
                    }
                    Message::Binary(bin) => {
                        ingest.fetch_add(1, Ordering::Relaxed);
                        if let Ok(txt) = String::from_utf8(bin) {
                            handle_message(&txt, parsed, parse_errors, validation_errors, sequence_errors, dropped, latencies.clone(), queue_depth, last_seqs, idx, tx, start).await;
                        } else {
                            parse_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
    let _ = ws.close(None).await;
    Ok(())
}

async fn handle_message(
    txt: &str,
    parsed: &AtomicU64,
    parse_errors: &AtomicU64,
    validation_errors: &AtomicU64,
    sequence_errors: &AtomicU64,
    dropped: &AtomicU64,
    latencies: Arc<Mutex<Vec<f64>>>,
    queue_depth: &AtomicI64,
    last_seqs: &Vec<AtomicI64>,
    idx: usize,
    tx: &mut mpsc::Sender<KafkaRecord>,
    start: Instant,
) {
    let msg: InboundMessage = match serde_json::from_str(txt) {
        Ok(m) => m,
        Err(_) => {
            parse_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    if !validate_msg(&msg) {
        validation_errors.fetch_add(1, Ordering::Relaxed);
        return;
    }

    check_seq(idx, msg.seq, last_seqs, sequence_errors);
    parsed.fetch_add(1, Ordering::Relaxed);

    let record = KafkaRecord {
        ts: msg.ts,
        seq: msg.seq,
        symbol_len: msg.symbol.len(),
        price: msg.price,
        size: msg.size,
    };

    match tx.try_send(record) {
        Ok(_) => {
            queue_depth.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut guard = latencies.lock().await;
    if guard.len() < 200_000 {
        guard.push(elapsed_ms);
    }
}

fn validate_msg(msg: &InboundMessage) -> bool {
    if msg.ts == 0 || msg.seq <= 0 {
        return false;
    }
    if msg.symbol.is_empty() {
        return false;
    }
    if msg.price <= 0.0 || msg.size <= 0.0 {
        return false;
    }
    true
}

fn check_seq(idx: usize, seq: i64, last_seqs: &Vec<AtomicI64>, sequence_errors: &AtomicU64) {
    let prev = last_seqs[idx].load(Ordering::Relaxed);
    if prev != 0 && seq != prev + 1 {
        sequence_errors.fetch_add(1, Ordering::Relaxed);
    }
    last_seqs[idx].store(seq, Ordering::Relaxed);
}

async fn kafka_mock(
    rx: &mut mpsc::Receiver<KafkaRecord>,
    consumed: Arc<AtomicU64>,
    queue_depth: Arc<AtomicI64>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            maybe = rx.recv() => {
                if let Some(rec) = maybe {
                    // Lightweight serialization to simulate CPU
                    let _ = serde_json::to_vec(&rec);
                    consumed.fetch_add(1, Ordering::Relaxed);
                    queue_depth.fetch_sub(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }
        }
    }
}

async fn metrics_loop(
    cfg: Config,
    ingest: Arc<AtomicU64>,
    parse_errors: Arc<AtomicU64>,
    validation_errors: Arc<AtomicU64>,
    sequence_errors: Arc<AtomicU64>,
    reconnects: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    consumed: Arc<AtomicU64>,
    active_conns: Arc<AtomicI64>,
    queue_depth: Arc<AtomicI64>,
    latencies: Arc<Mutex<Vec<f64>>>,
    cancel: CancellationToken,
    csv_path: String,
) {
    let mut ticker = interval(Duration::from_millis(cfg.log_interval_ms.max(10)));
    let mut prev_ingest = 0;

    let mut sys = System::new_all();
    let pid = sysinfo::get_current_pid().ok();

    let mut csv_writer: Option<BufWriter<tokio::fs::File>> = None;
    let mut csv_header_written = false;
    if !csv_path.is_empty() {
        let meta = tokio::fs::metadata(&csv_path).await.ok();
        csv_header_written = meta.map(|m| m.len() > 0).unwrap_or(false);
        match OpenOptions::new().create(true).append(true).open(&csv_path).await {
            Ok(file) => {
                csv_writer = Some(BufWriter::new(file));
            }
            Err(e) => {
                eprintln!("csv open error: {e}");
            }
        }
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {
                sys.refresh_all();
                let (cpu_pct, rss_mb) = if let Some(p) = pid {
                    if let Some(proc) = sys.process(p) {
                        (proc.cpu_usage() as f64, proc.memory() as f64 / (1024.0 * 1024.0))
                    } else {
                        (0.0, 0.0)
                    }
                } else {
                    (0.0, 0.0)
                };

                let ing = ingest.load(Ordering::Relaxed);
                let delta_ing = ing.saturating_sub(prev_ingest);
                prev_ingest = ing;

                let samples = {
                    let mut guard = latencies.lock().await;
                    let mut buf = Vec::with_capacity(guard.len());
                    buf.append(&mut *guard);
                    buf
                };
                let (p50, p95) = percentiles(&samples);

                let out = MetricsOut {
                    ts: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                    connections_active: active_conns.load(Ordering::Relaxed),
                    msgs_in_total: ing,
                    msgs_in_per_sec: delta_ing,
                    parse_errors_total: parse_errors.load(Ordering::Relaxed),
                    validation_errors: validation_errors.load(Ordering::Relaxed),
                    sequence_errors: sequence_errors.load(Ordering::Relaxed),
                    reconnects_total: reconnects.load(Ordering::Relaxed),
                    queue_depth: queue_depth.load(Ordering::Relaxed),
                    queue_capacity: cfg.queue,
                    queue_dropped_total: dropped.load(Ordering::Relaxed),
                    consumed_total: consumed.load(Ordering::Relaxed),
                    cpu_pct,
                    rss_mb,
                    latency_ms_p50: p50,
                    latency_ms_p95: p95,
                };
                if let Ok(line) = serde_json::to_string(&out) {
                    println!("{line}");
                }
                if let Some(writer) = csv_writer.as_mut() {
                    if !csv_header_written {
                        let _ = writer.write_all(b"ts,connections_active,msgs_in_total,msgs_in_per_sec,parse_errors_total,validation_errors,sequence_errors,reconnects_total,queue_depth,queue_capacity,queue_dropped_total,consumed_total,cpu_pct,rss_mb,latency_ms_p50,latency_ms_p95\n").await;
                        csv_header_written = true;
                    }
                    let line = format!(
                        "{},{},{},{},{},{},{},{},{},{},{},{},{:.4},{:.2},{:.4},{:.4}\n",
                        out.ts,
                        out.connections_active,
                        out.msgs_in_total,
                        out.msgs_in_per_sec,
                        out.parse_errors_total,
                        out.validation_errors,
                        out.sequence_errors,
                        out.reconnects_total,
                        out.queue_depth,
                        out.queue_capacity,
                        out.queue_dropped_total,
                        out.consumed_total,
                        out.cpu_pct,
                        out.rss_mb,
                        out.latency_ms_p50,
                        out.latency_ms_p95
                    );
                    let _ = writer.write_all(line.as_bytes()).await;
                    let _ = writer.flush().await;
                }
            }
        }
    }
}

fn percentiles(samples: &[f64]) -> (f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0);
    }
    let mut v = samples.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = |p: f64| -> f64 {
        let idx = ((v.len() - 1) as f64 * p).round() as usize;
        v[idx]
    };
    (p(0.5), p(0.95))
}

async fn sleep_with_jitter(base: Duration, rng_state: &mut u64, cancel: &CancellationToken) {
    let jitter = Duration::from_millis((*rng_state % base.as_millis().max(1) as u64 / 2).max(1));
    *rng_state = rng_next(*rng_state);
    let dur = base + jitter;
    tokio::select! {
        _ = cancel.cancelled() => {},
        _ = tokio::time::sleep(dur) => {}
    }
}

fn rng_next(state: u64) -> u64 {
    state.wrapping_mul(1664525).wrapping_add(1013904223)
}

fn with_seed(base: &str, seed: usize) -> String {
    if let Ok(mut url) = url::Url::parse(base) {
        url.query_pairs_mut().append_pair("seed", &seed.to_string());
        url.to_string()
    } else {
        format!("{base}?seed={seed}")
    }
}
