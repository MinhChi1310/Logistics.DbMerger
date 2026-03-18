using System.Diagnostics;
using System.Text;
using Dapper;
using Microsoft.Data.SqlClient;
using Serilog;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Background resource monitor that periodically samples process and SQL Server metrics.
    /// </summary>
    static class ResourceMonitor
    {
        private record struct MetricSample(
            DateTime Timestamp,
            double RamMb,
            double CpuPercent,
            int Gen0,
            int Gen1,
            int Gen2,
            int ThreadCount,
            int SrcActiveRequests,
            long SrcLongestMs,
            string? SrcWaitType,
            int TgtActiveRequests,
            long TgtLongestMs,
            string? TgtWaitType);

        private class SqlMetricsResult
        {
            public int ActiveRequests { get; set; }
            public long LongestMs { get; set; }
            public string? LongestWaitType { get; set; }
        }

        private const int MaxSamples = 100_000;
        private const int SqlConnectTimeoutSeconds = 5;
        private const int SqlCommandTimeoutSeconds = 5;

        private static Timer? _timer;
        private static string? _sourceConnStr;
        private static string? _targetConnStr;
        private static Process? _process;
        private static TimeSpan _lastCpuTime;
        private static DateTime _lastSampleTime;
        private static DateTime _startTime;
        private static int _sampling; // 0 = idle, 1 = sampling (prevents overlapping callbacks)
        private static bool _maxSamplesWarned;
        private static bool _samplingErrorWarned;

        private static double _peakRamMb;
        private static double _peakCpuPercent;
        private static int _baselineGen0;
        private static int _baselineGen1;
        private static int _baselineGen2;

        private static readonly List<MetricSample> _samples = new();
        private static readonly object _lock = new();

        /// <summary>
        /// Start periodic resource monitoring. If intervalSeconds &lt;= 0, monitoring is disabled.
        /// </summary>
        public static void Start(int intervalSeconds, string? sourceConnStr, string? targetConnStr)
        {
            // P1: Stop any existing timer to prevent orphaned timers on double-start
            Stop();

            if (intervalSeconds <= 0)
            {
                Log.Information("[Metrics] Resource monitoring disabled (interval = {Interval})", intervalSeconds);
                return;
            }

            if (intervalSeconds > 3600) intervalSeconds = 3600;

            _sourceConnStr = AppendConnectTimeout(sourceConnStr);
            _targetConnStr = AppendConnectTimeout(targetConnStr);

            var proc = Process.GetCurrentProcess();
            proc.Refresh();

            lock (_lock)
            {
                _process = proc;
                _lastCpuTime = proc.TotalProcessorTime;
                _lastSampleTime = DateTime.UtcNow;
                _startTime = DateTime.UtcNow;
                _peakRamMb = 0;
                _peakCpuPercent = 0;
                // P7: Baselines written under lock
                _baselineGen0 = GC.CollectionCount(0);
                _baselineGen1 = GC.CollectionCount(1);
                _baselineGen2 = GC.CollectionCount(2);
                _samples.Clear();
                _maxSamplesWarned = false;
                _samplingErrorWarned = false;
            }

            Interlocked.Exchange(ref _sampling, 0);

            var intervalMs = intervalSeconds * 1000;
            _timer = new Timer(SampleCallback, null, intervalMs, intervalMs);

            Log.Information("[Metrics] Resource monitoring started (interval: {IntervalSeconds}s, source: {HasSource}, target: {HasTarget})",
                intervalSeconds, !string.IsNullOrEmpty(sourceConnStr), !string.IsNullOrEmpty(targetConnStr));
        }

        /// <summary>
        /// Stop the monitoring timer. Waits for in-flight callbacks. Idempotent.
        /// </summary>
        public static void Stop()
        {
            var timer = _timer;
            if (timer != null)
            {
                using var waitHandle = new ManualResetEvent(false);
                if (timer.Dispose(waitHandle))
                    waitHandle.WaitOne(TimeSpan.FromSeconds(10));
                _timer = null;
            }
            // P6: Null _process under lock so in-flight callbacks see a consistent state
            lock (_lock)
            {
                _process = null;
            }
        }

        /// <summary>
        /// Log a summary of peak resource usage.
        /// </summary>
        public static void LogSummary()
        {
            int totalGen0, totalGen1, totalGen2;
            double peakRam, peakCpu;

            lock (_lock)
            {
                totalGen0 = GC.CollectionCount(0) - _baselineGen0;
                totalGen1 = GC.CollectionCount(1) - _baselineGen1;
                totalGen2 = GC.CollectionCount(2) - _baselineGen2;
                peakRam = _peakRamMb;
                peakCpu = _peakCpuPercent;
            }

            Log.Information(
                "[Metrics] Summary — Peak RAM: {PeakRamMb:F1}MB | Peak CPU: {PeakCpuPercent:F1}% | GC totals: {Gen0}/{Gen1}/{Gen2}",
                peakRam, peakCpu, totalGen0, totalGen1, totalGen2);
        }

        /// <summary>
        /// Save a full resource report to output/resource-report-{date}.txt.
        /// </summary>
        public static void SaveReport(string outputDir = "output")
        {
            List<MetricSample> snapshot;
            double peakRam, peakCpu;

            lock (_lock)
            {
                snapshot = new List<MetricSample>(_samples);
                peakRam = _peakRamMb;
                peakCpu = _peakCpuPercent;
            }

            if (snapshot.Count == 0)
            {
                Log.Information("[Metrics] No samples collected — skipping report.");
                return;
            }

            Directory.CreateDirectory(outputDir);
            var endTime = DateTime.UtcNow;
            var fileName = $"resource-report-{endTime:yyyyMMdd-HHmmss}.txt";
            var filePath = Path.Combine(outputDir, fileName);

            var sb = new StringBuilder();
            sb.AppendLine("=== Resource Monitor Report ===");
            sb.AppendLine($"Start:    {_startTime:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine($"End:      {endTime:yyyy-MM-dd HH:mm:ss} UTC");
            var duration = endTime - _startTime;
            sb.AppendLine($"Duration: {(int)duration.TotalHours}:{duration:mm\\:ss}");
            sb.AppendLine($"Samples:  {snapshot.Count}");
            sb.AppendLine();

            sb.AppendFormat("{0,-20} | {1,8} | {2,7} | {3,14} | {4,7} | {5,10} | {6,12} | {7,-12} | {8,10} | {9,12} | {10}\n",
                "Timestamp", "RAM (MB)", "CPU (%)", "GC 0/1/2", "Threads",
                "Src Active", "Src Long(ms)", "Src Wait",
                "Tgt Active", "Tgt Long(ms)", "Tgt Wait");
            sb.AppendLine(new string('-', 160));

            foreach (var s in snapshot)
            {
                sb.AppendFormat("{0,-20} | {1,8:F1} | {2,7:F1} | {3,4}/{4,4}/{5,4} | {6,7} | {7,10} | {8,12} | {9,-12} | {10,10} | {11,12} | {12}\n",
                    s.Timestamp.ToString("HH:mm:ss.fff"), s.RamMb, s.CpuPercent,
                    s.Gen0, s.Gen1, s.Gen2, s.ThreadCount,
                    s.SrcActiveRequests, s.SrcLongestMs, s.SrcWaitType ?? "N/A",
                    s.TgtActiveRequests, s.TgtLongestMs, s.TgtWaitType ?? "N/A");
            }

            sb.AppendLine();
            sb.AppendLine("=== Summary ===");

            var avgRam = snapshot.Average(s => s.RamMb);
            var avgCpu = snapshot.Average(s => s.CpuPercent);
            int totalGen0, totalGen1, totalGen2;
            lock (_lock)
            {
                totalGen0 = GC.CollectionCount(0) - _baselineGen0;
                totalGen1 = GC.CollectionCount(1) - _baselineGen1;
                totalGen2 = GC.CollectionCount(2) - _baselineGen2;
            }

            sb.AppendLine($"Peak RAM:         {peakRam:F1} MB");
            sb.AppendLine($"Avg RAM:          {avgRam:F1} MB");
            sb.AppendLine($"Peak CPU:         {peakCpu:F1} %");
            sb.AppendLine($"Avg CPU:          {avgCpu:F1} %");
            sb.AppendLine($"Total GC:         Gen0={totalGen0}, Gen1={totalGen1}, Gen2={totalGen2}");
            sb.AppendLine($"Total Samples:    {snapshot.Count}");

            File.WriteAllText(filePath, sb.ToString());
            Log.Information("[Metrics] Resource report saved to {FilePath}", filePath);
        }

        private static void SampleCallback(object? state)
        {
            if (Interlocked.CompareExchange(ref _sampling, 1, 0) != 0)
                return;

            try
            {
                var sample = SampleProcessMetrics();
                if (sample.Timestamp == default)
                    return; // Process unavailable (Stop in progress), skip this tick

                SampleSqlMetrics(_sourceConnStr, "Source", ref sample, isSource: true);
                SampleSqlMetrics(_targetConnStr, "Target", ref sample, isSource: false);

                lock (_lock)
                {
                    if (_samples.Count < MaxSamples)
                    {
                        _samples.Add(sample);
                    }
                    // P4: Log warning once when sample limit is reached
                    else if (!_maxSamplesWarned)
                    {
                        _maxSamplesWarned = true;
                        Log.Warning("[Metrics] Sample limit reached ({MaxSamples}). New samples will not be recorded. Peak tracking continues.", MaxSamples);
                    }
                }
            }
            catch (Exception ex)
            {
                if (!_samplingErrorWarned)
                {
                    _samplingErrorWarned = true;
                    Log.Warning("[Metrics] Sampling error (further errors logged at Debug): {ErrorMessage}", ex.Message);
                }
                else
                {
                    Log.Debug("[Metrics] Sampling error: {ErrorMessage}", ex.Message);
                }
            }
            finally
            {
                Interlocked.Exchange(ref _sampling, 0);
            }
        }

        private static MetricSample SampleProcessMetrics()
        {
            Process? proc;
            TimeSpan lastCpu;
            DateTime lastTime;
            int baseGen0, baseGen1, baseGen2;

            lock (_lock)
            {
                proc = _process;
                if (proc == null)
                    return default;

                lastCpu = _lastCpuTime;
                lastTime = _lastSampleTime;
                // P7: Read baselines under lock
                baseGen0 = _baselineGen0;
                baseGen1 = _baselineGen1;
                baseGen2 = _baselineGen2;
            }

            proc.Refresh();

            var ramMb = proc.WorkingSet64 / (1024.0 * 1024.0);
            var now = DateTime.UtcNow;
            var currentCpuTime = proc.TotalProcessorTime;

            lock (_lock)
            {
                _lastCpuTime = currentCpuTime;
                _lastSampleTime = now;
            }

            var elapsedWall = now - lastTime;
            var cpuUsed = (currentCpuTime - lastCpu).TotalMilliseconds;
            var cpuPercent = elapsedWall.TotalMilliseconds > 0
                ? cpuUsed / (elapsedWall.TotalMilliseconds * Environment.ProcessorCount) * 100.0
                : 0;

            var gen0 = GC.CollectionCount(0) - baseGen0;
            var gen1 = GC.CollectionCount(1) - baseGen1;
            var gen2 = GC.CollectionCount(2) - baseGen2;
            var threadCount = ThreadPool.ThreadCount;

            lock (_lock)
            {
                if (ramMb > _peakRamMb) _peakRamMb = ramMb;
                if (cpuPercent > _peakCpuPercent) _peakCpuPercent = cpuPercent;
            }

            Log.Information(
                "[Metrics] RAM: {RamMb:F1}MB | CPU: {CpuPercent:F1}% | GC: {Gen0}/{Gen1}/{Gen2} | Threads: {ThreadCount}",
                ramMb, cpuPercent, gen0, gen1, gen2, threadCount);

            return new MetricSample(now, ramMb, cpuPercent, gen0, gen1, gen2, threadCount, 0, 0, null, 0, 0, null);
        }

        private static void SampleSqlMetrics(string? connStr, string label, ref MetricSample sample, bool isSource)
        {
            if (string.IsNullOrEmpty(connStr))
                return;

            try
            {
                using var conn = new SqlConnection(connStr);
                conn.Open();

                var result = conn.QueryFirstOrDefault<SqlMetricsResult>(
                    @"SELECT
                        (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id > 50 AND status = 'running') AS ActiveRequests,
                        ISNULL(r.total_elapsed_time, 0) AS LongestMs,
                        r.wait_type AS LongestWaitType
                    FROM (SELECT TOP 1 total_elapsed_time, wait_type
                          FROM sys.dm_exec_requests
                          WHERE session_id > 50 AND status = 'running'
                          ORDER BY total_elapsed_time DESC) r
                    UNION ALL
                    SELECT
                        (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id > 50 AND status = 'running'),
                        0, NULL
                    WHERE NOT EXISTS (SELECT 1 FROM sys.dm_exec_requests WHERE session_id > 50 AND status = 'running')",
                    commandTimeout: SqlCommandTimeoutSeconds);

                if (result != null)
                {
                    if (isSource)
                    {
                        sample = sample with
                        {
                            SrcActiveRequests = result.ActiveRequests,
                            SrcLongestMs = result.LongestMs,
                            SrcWaitType = result.LongestWaitType
                        };
                    }
                    else
                    {
                        sample = sample with
                        {
                            TgtActiveRequests = result.ActiveRequests,
                            TgtLongestMs = result.LongestMs,
                            TgtWaitType = result.LongestWaitType
                        };
                    }

                    if (result.ActiveRequests > 0)
                    {
                        Log.Information(
                            "[Metrics] SQL {Label}: {ActiveRequests} active requests, longest: {LongestMs}ms ({WaitType})",
                            label, result.ActiveRequests, result.LongestMs, result.LongestWaitType ?? "N/A");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Debug("[Metrics] SQL {Label} metrics unavailable: {ErrorMessage}", label, ex.Message);
            }
        }

        /// <summary>
        /// P3: Append a short connection timeout for monitoring queries.
        /// </summary>
        private static string? AppendConnectTimeout(string? connStr)
        {
            if (string.IsNullOrEmpty(connStr))
                return connStr;

            var builder = new SqlConnectionStringBuilder(connStr)
            {
                ConnectTimeout = SqlConnectTimeoutSeconds
            };
            return builder.ConnectionString;
        }
    }
}
