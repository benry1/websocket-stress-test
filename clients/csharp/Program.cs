using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

record Config(
    Uri Server,
    int Connections,
    int QueueSize,
    int LogIntervalMs,
    int BackoffBaseMs,
    int BackoffMaxMs,
    int DialTimeoutMs,
    string? CsvPath
);

record InboundMessage(long ts, long seq, string symbol, double price, double size);

record KafkaRecord(long ts, long seq, string symbol, double price, double size);

class Program
{
    private static long _ingest;
    private static long _parsed;
    private static long _parseErrors;
    private static long _validationErrors;
    private static long _sequenceErrors;
    private static long _reconnects;
    private static long _dropped;
    private static long _consumed;
    private static long _activeConns;
    private static long _queueDepth;

    private static readonly object LatencyLock = new();
    private static readonly List<double> Latencies = new(capacity: 100_000);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    static async Task Main(string[] args)
    {
        var cfg = ParseArgs(args);
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        Console.WriteLine($"starting C# client -> {cfg.Server} (conns={cfg.Connections} queue={cfg.QueueSize})");

        var queue = Channel.CreateBounded<KafkaRecord>(new BoundedChannelOptions(cfg.QueueSize)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropWrite
        });

        var consumerTask = Task.Run(() => KafkaMock(queue.Reader, cts.Token));

        var tasks = new List<Task>();
        var lastSeqs = new long[cfg.Connections];
        for (int i = 0; i < cfg.Connections; i++)
        {
            var idx = i;
            tasks.Add(Task.Run(() => ManageConnection(idx, cfg, queue.Writer, lastSeqs, cts.Token)));
        }

        var metricsTask = Task.Run(() => MetricsLoop(cfg, queue, cts.Token));

        tasks.Add(consumerTask);
        tasks.Add(metricsTask);

        await Task.WhenAll(tasks);
    }

    private static async Task ManageConnection(int idx, Config cfg, ChannelWriter<KafkaRecord> writer, long[] lastSeqs, CancellationToken token)
    {
        var rng = new Random(unchecked(Environment.TickCount + idx * 97));
        var backoff = TimeSpan.FromMilliseconds(cfg.BackoffBaseMs);
        var target = WithSeed(cfg.Server, idx + 1);

        while (!token.IsCancellationRequested)
        {
            using var cws = new ClientWebSocket();
            cws.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(cfg.DialTimeoutMs));
                await cws.ConnectAsync(target, CancellationTokenSource.CreateLinkedTokenSource(token, cts.Token).Token);
                Interlocked.Add(ref _activeConns, 1);
                backoff = TimeSpan.FromMilliseconds(cfg.BackoffBaseMs);
                await ReadLoop(cws, writer, lastSeqs, idx, token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[conn {idx}] dial/read error: {ex.Message}");
            }
            finally
            {
                Interlocked.Add(ref _activeConns, -1);
                try { cws.Abort(); cws.Dispose(); } catch { }
            }

            if (token.IsCancellationRequested) return;
            Interlocked.Increment(ref _reconnects);
            await SleepWithJitter(backoff, rng, token);
            backoff = TimeSpan.FromMilliseconds(Math.Min(cfg.BackoffMaxMs, backoff.TotalMilliseconds * 2));
        }
    }

    private static async Task ReadLoop(ClientWebSocket ws, ChannelWriter<KafkaRecord> writer, long[] lastSeqs, int idx, CancellationToken token)
    {
        var buffer = new byte[64 * 1024];
        using var ms = new MemoryStream();

        while (!token.IsCancellationRequested && ws.State == WebSocketState.Open)
        {
            ms.SetLength(0);
            while (true)
            {
                var result = await ws.ReceiveAsync(buffer, token);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", token);
                    return;
                }

                ms.Write(buffer, 0, result.Count);
                if (result.EndOfMessage) break;
            }

            var start = Stopwatch.GetTimestamp();
            Interlocked.Increment(ref _ingest);

            try
            {
                var msg = JsonSerializer.Deserialize<InboundMessage>(ms.GetBuffer().AsSpan(0, (int)ms.Length), JsonOptions);
                if (msg is null)
                {
                    Interlocked.Increment(ref _parseErrors);
                    continue;
                }

                if (!Validate(msg))
                {
                    Interlocked.Increment(ref _validationErrors);
                    continue;
                }

                CheckSeq(idx, msg.seq, lastSeqs);
                Interlocked.Increment(ref _parsed);

                var record = new KafkaRecord(msg.ts, msg.seq, msg.symbol, msg.price, msg.size);

                if (!writer.TryWrite(record))
                {
                    Interlocked.Increment(ref _dropped);
                }
                else
                {
                    Interlocked.Increment(ref _queueDepth);
                }
            }
            catch (JsonException)
            {
                Interlocked.Increment(ref _parseErrors);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[conn {idx}] message error: {ex.Message}");
            }
            finally
            {
                RecordLatency(start);
            }
        }
    }

    private static async Task KafkaMock(ChannelReader<KafkaRecord> reader, CancellationToken token)
    {
        try
        {
            await foreach (var rec in reader.ReadAllAsync(token))
            {
                // Optional lightweight serialization to mimic CPU path.
                _ = JsonSerializer.Serialize(rec);
                Interlocked.Increment(ref _consumed);
                Interlocked.Decrement(ref _queueDepth);
            }
        }
        catch (OperationCanceledException)
        {
            // shutdown
        }
    }

    private static async Task MetricsLoop(Config cfg, Channel<KafkaRecord> queue, CancellationToken token)
    {
        var interval = TimeSpan.FromMilliseconds(cfg.LogIntervalMs <= 0 ? 1000 : cfg.LogIntervalMs);
        var ticker = new PeriodicTimer(interval);
        var prevIngest = 0L;
        var proc = Process.GetCurrentProcess();
        var prevCpu = proc.TotalProcessorTime;
        var prevWall = Stopwatch.GetTimestamp();

        StreamWriter? csv = null;
        var headerWritten = false;
        if (!string.IsNullOrWhiteSpace(cfg.CsvPath))
        {
            try
            {
                var exists = File.Exists(cfg.CsvPath);
                csv = new StreamWriter(new FileStream(cfg.CsvPath, FileMode.Append, FileAccess.Write, FileShare.Read))
                {
                    AutoFlush = true
                };
                headerWritten = exists && new FileInfo(cfg.CsvPath).Length > 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"csv open error: {ex.Message}");
            }
        }

        try
        {
            while (await ticker.WaitForNextTickAsync(token))
            {
                var nowWall = Stopwatch.GetTimestamp();
                proc.Refresh();
                var cpuNow = proc.TotalProcessorTime;
                var cpuDelta = cpuNow - prevCpu;
                var elapsedSec = (nowWall - prevWall) / (double)Stopwatch.Frequency;
                prevCpu = cpuNow;
                prevWall = nowWall;
                var cpuPct = elapsedSec > 0 ? (cpuDelta.TotalSeconds / elapsedSec) * 100 : 0;

                var ing = Interlocked.Read(ref _ingest);
                var deltaIngest = ing - prevIngest;
                prevIngest = ing;

                var latencies = DrainLatencies();
                var (p50, p95) = Percentiles(latencies);

                var output = new Dictionary<string, object?>
                {
                    ["ts"] = DateTime.UtcNow.ToString("O"),
                    ["connections_active"] = Interlocked.Read(ref _activeConns),
                    ["msgs_in_total"] = ing,
                    ["msgs_in_per_sec"] = deltaIngest,
                    ["parse_errors_total"] = Interlocked.Read(ref _parseErrors),
                    ["validation_errors"] = Interlocked.Read(ref _validationErrors),
                    ["sequence_errors"] = Interlocked.Read(ref _sequenceErrors),
                    ["reconnects_total"] = Interlocked.Read(ref _reconnects),
                    ["queue_depth"] = Interlocked.Read(ref _queueDepth),
                    ["queue_capacity"] = cfg.QueueSize,
                    ["queue_dropped_total"] = Interlocked.Read(ref _dropped),
                    ["consumed_total"] = Interlocked.Read(ref _consumed),
                    ["cpu_pct"] = cpuPct,
                    ["rss_mb"] = proc.WorkingSet64 / 1024.0 / 1024.0,
                    ["latency_ms_p50"] = p50,
                    ["latency_ms_p95"] = p95
                };

                Console.WriteLine(JsonSerializer.Serialize(output));
                if (csv != null)
                {
                    if (!headerWritten)
                    {
                        await csv.WriteLineAsync("ts,connections_active,msgs_in_total,msgs_in_per_sec,parse_errors_total,validation_errors,sequence_errors,reconnects_total,queue_depth,queue_capacity,queue_dropped_total,consumed_total,cpu_pct,rss_mb,latency_ms_p50,latency_ms_p95");
                        headerWritten = true;
                    }

                    await csv.WriteLineAsync(string.Format(
                        "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12:F4},{13:F2},{14:F4},{15:F4}",
                        output["ts"],
                        output["connections_active"],
                        output["msgs_in_total"],
                        output["msgs_in_per_sec"],
                        output["parse_errors_total"],
                        output["validation_errors"],
                        output["sequence_errors"],
                        output["reconnects_total"],
                        output["queue_depth"],
                        output["queue_capacity"],
                        output["queue_dropped_total"],
                        output["consumed_total"],
                        output["cpu_pct"],
                        output["rss_mb"],
                        output["latency_ms_p50"],
                        output["latency_ms_p95"]
                    ));
                }
            }
        }
        catch (OperationCanceledException)
        {
            // shutdown
        }
        finally
        {
            if (csv != null)
            {
                await csv.FlushAsync();
                await csv.DisposeAsync();
            }
        }
    }

    private static (double p50, double p95) Percentiles(List<double> samples)
    {
        if (samples.Count == 0) return (0, 0);
        samples.Sort();
        double P(double percentile)
        {
            var idx = (int)(percentile * (samples.Count - 1));
            return samples[idx];
        }
        return (P(0.5), P(0.95));
    }

    private static List<double> DrainLatencies()
    {
        lock (LatencyLock)
        {
            var copy = new List<double>(Latencies);
            Latencies.Clear();
            return copy;
        }
    }

    private static void RecordLatency(long startTicks)
    {
        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        lock (LatencyLock)
        {
            if (Latencies.Count < 200_000)
            {
                Latencies.Add(elapsedMs);
            }
        }
    }

    private static bool Validate(InboundMessage msg)
    {
        if (msg.ts == 0 || msg.seq <= 0) return false;
        if (string.IsNullOrEmpty(msg.symbol)) return false;
        if (msg.price <= 0 || msg.size <= 0) return false;
        return true;
    }

    private static void CheckSeq(int idx, long seq, long[] lastSeqs)
    {
        var prev = Interlocked.Read(ref lastSeqs[idx]);
        if (prev != 0 && seq != prev + 1)
        {
            Interlocked.Increment(ref _sequenceErrors);
        }
        Interlocked.Exchange(ref lastSeqs[idx], seq);
    }

    private static async Task SleepWithJitter(TimeSpan backoff, Random rng, CancellationToken token)
    {
        var jitter = TimeSpan.FromMilliseconds(rng.Next((int)Math.Max(1, backoff.TotalMilliseconds / 2)));
        var delay = backoff + jitter;
        try
        {
            await Task.Delay(delay, token);
        }
        catch (TaskCanceledException)
        {
            // ignore
        }
    }

    private static Uri WithSeed(Uri baseUri, int seed)
    {
        var builder = new UriBuilder(baseUri);
        var query = builder.Query;
        if (!string.IsNullOrEmpty(query))
        {
            query = query.TrimStart('?') + "&";
        }
        query += "seed=" + seed;
        builder.Query = query;
        return builder.Uri;
    }

    private static Config ParseArgs(string[] args)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--")) continue;
            var parts = args[i].Split('=', 2);
            var key = parts[0].TrimStart('-');
            if (parts.Length == 2)
            {
                dict[key] = parts[1];
            }
            else if (i + 1 < args.Length && !args[i + 1].StartsWith("--"))
            {
                dict[key] = args[i + 1];
                i++;
            }
            else
            {
                dict[key] = "true";
            }
        }

        Uri server = new(dict.TryGetValue("server", out var srv) ? srv : "ws://localhost:8080/stream");
        int connections = dict.TryGetValue("connections", out var c) && int.TryParse(c, out var ci) ? ci : 200;
        int queueSize = dict.TryGetValue("queue", out var q) && int.TryParse(q, out var qi) ? qi : 10000;
        int logIntervalMs = dict.TryGetValue("logIntervalMs", out var l) && int.TryParse(l, out var li) ? li : 1000;
        int backoffBase = dict.TryGetValue("backoffBaseMs", out var bb) && int.TryParse(bb, out var bbi) ? bbi : 200;
        int backoffMax = dict.TryGetValue("backoffMaxMs", out var bm) && int.TryParse(bm, out var bmi) ? bmi : 5000;
        int dialTimeout = dict.TryGetValue("dialTimeoutMs", out var dt) && int.TryParse(dt, out var dti) ? dti : 5000;
        string? csvPath = dict.TryGetValue("csv", out var csv) ? csv : null;

        return new Config(server, connections, queueSize, logIntervalMs, backoffBase, backoffMax, dialTimeout, csvPath);
    }
}
