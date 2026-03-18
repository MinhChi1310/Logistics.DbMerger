using Serilog;
using System.Text;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Static utility that collects migration results per step and writes structured report files.
    /// Each migration run gets its own folder: output/reports/{Tenant}_{yyyyMMdd}_{HHmmss}/
    /// </summary>
    public static class ReportWriter
    {
        private static string? _reportDir;
        private static string? _tenantName;
        private static DateTime _runStart;
        private static int _sourceTenantId;
        private static int _targetTenantId;

        // Per-step collected data
        private static readonly List<(string Table, string Status, long ElapsedMs)> _schemaTables = new();
        private static readonly List<(string Table, string Column)> _schemaColumns = new();
        private static readonly List<(string Type, int Created, int Skipped, int Failed)> _schemaConstraints = new();
        private static readonly List<(string ObjectType, string ObjectName, string Status)> _objectResults = new();
        private static readonly List<(string SourceTable, string TargetTable, long Rows, long ElapsedMs, string Method, string? Error)> _dataSyncTables = new();
        private static readonly List<(string ChildTable, string ChildColumn, string RefTable, string RefColumn, long RowsUpdated)> _fkUpdates = new();
        private static readonly List<(string Table, long SourceCount, long TargetCount, string Method, string Status)> _validationRows = new();
        private static readonly List<string> _fkViolations = new();

        // Step verdicts for summary
        private static readonly Dictionary<string, (string Verdict, string Detail)> _stepVerdicts = new();

        public static bool IsInitialized => _reportDir != null;
        public static string? ReportDirectory => _reportDir;

        public static void Initialize(string tenantName, DateTime runStart)
        {
            _tenantName = tenantName ?? "ALL";
            _runStart = runStart;

            // Sanitize tenant name for use in folder path (remove invalid path characters)
            var safeName = string.Join("_", _tenantName.Split(Path.GetInvalidFileNameChars()));
            _reportDir = Path.Combine("output", "reports", $"{safeName}_{runStart:yyyyMMdd}_{runStart:HHmmss}");
            try
            {
                Directory.CreateDirectory(_reportDir);
            }
            catch (Exception ex)
            {
                Log.Warning("[Report] Failed to create report folder {ReportDir}: {ErrorMessage}. Reports will be disabled.", _reportDir, ex.Message);
                _reportDir = null; // IsInitialized = false, all Add*/Write* will no-op
                return;
            }

            // Clear any data from previous run in same process
            _schemaTables.Clear();
            _schemaColumns.Clear();
            _schemaConstraints.Clear();
            _objectResults.Clear();
            _dataSyncTables.Clear();
            _fkUpdates.Clear();
            _validationRows.Clear();
            _fkViolations.Clear();
            _stepVerdicts.Clear();
            _sourceTenantId = 0;
            _targetTenantId = 0;

            Log.Information("[Report] Initialized report folder: {ReportDir}", _reportDir);
        }

        // ─── PreFlight Report ───────────────────────────────────────────

        public static void WritePreFlightReport(
            int overlaps, int failed, int safe, int skipped, int remaps,
            bool collationMismatch, string sourceCollation, string targetCollation)
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            var totalIssues = failed + (collationMismatch ? 1 : 0);
            var verdict = totalIssues > 0 ? $"FAIL ({totalIssues} blocking issue(s))"
                        : (overlaps + remaps) > 0 ? $"WARNING ({overlaps + remaps} non-blocking issue(s))"
                        : "PASS";

            WriteHeader(sb, "PRE-FLIGHT CHECKS", verdict);

            sb.AppendLine("═══ Identity Range Overlap Check ═══");
            sb.AppendLine($"  Overlaps (non-blocking, IdMapping resolves): {overlaps}");
            sb.AppendLine($"  Safe (no overlap):                           {safe}");
            sb.AppendLine($"  Failed (check errors):                       {failed}");
            sb.AppendLine($"  Skipped:                                     {skipped}");
            sb.AppendLine();

            sb.AppendLine("═══ Partition Filegroup Check ═══");
            sb.AppendLine($"  Schemes needing remap to PRIMARY: {remaps}");
            sb.AppendLine();

            sb.AppendLine("═══ Database Collation Check ═══");
            sb.AppendLine($"  Source: {sourceCollation}");
            sb.AppendLine($"  Target: {targetCollation}");
            sb.AppendLine($"  Match:  {(collationMismatch ? "NO — MISMATCH" : "YES")}");

            WriteFile("1-preflight.txt", sb);
            _stepVerdicts["1. PreFlight"] = (verdict, $"{overlaps} overlaps, {remaps} remaps, collation {(collationMismatch ? "MISMATCH" : "OK")}");
        }

        // ─── Schema Report ──────────────────────────────────────────────

        public static void AddSchemaTable(string tableName, string status, long elapsedMs)
        {
            if (!IsInitialized) return;
            _schemaTables.Add((tableName, status, elapsedMs));
        }

        public static void AddSchemaColumn(string tableName, string columnName)
        {
            if (!IsInitialized) return;
            _schemaColumns.Add((tableName, columnName));
        }

        public static void AddSchemaConstraint(string type, int created, int skipped, int failed)
        {
            if (!IsInitialized) return;
            _schemaConstraints.Add((type, created, skipped, failed));
        }

        public static void WriteSchemaReport()
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            int totalFailed = _schemaConstraints.Sum(c => c.Failed);
            var verdict = totalFailed > 0 ? $"WARNING ({totalFailed} constraint failure(s))" : "PASS";

            WriteHeader(sb, "SCHEMA SYNC", verdict);

            // Tables
            sb.AppendLine("═══ Tables Created ═══");
            if (_schemaTables.Count == 0)
                sb.AppendLine("  (none)");
            else
            {
                sb.AppendLine($"  {"Table",-50} {"Status",-15} {"Time",-10}");
                sb.AppendLine($"  {new string('─', 50)} {new string('─', 15)} {new string('─', 10)}");
                foreach (var (table, status, ms) in _schemaTables)
                    sb.AppendLine($"  {table,-50} {status,-15} {ms}ms");
            }
            sb.AppendLine($"  Total: {_schemaTables.Count(t => t.Status == "Created")} created, {_schemaTables.Count(t => t.Status == "Skipped")} skipped");
            sb.AppendLine();

            // Columns
            sb.AppendLine("═══ Columns Added ═══");
            if (_schemaColumns.Count == 0)
                sb.AppendLine("  (none)");
            else
            {
                var byTable = _schemaColumns.GroupBy(c => c.Table).OrderBy(g => g.Key);
                foreach (var group in byTable)
                {
                    sb.AppendLine($"  {group.Key}:");
                    foreach (var (_, col) in group)
                        sb.AppendLine($"    + {col}");
                }
            }
            sb.AppendLine($"  Total: {_schemaColumns.Count} column(s) added across {_schemaColumns.Select(c => c.Table).Distinct().Count()} table(s)");
            sb.AppendLine();

            // Constraints
            sb.AppendLine("═══ Constraints ═══");
            sb.AppendLine($"  {"Type",-25} {"Created",-10} {"Skipped",-10} {"Failed",-10}");
            sb.AppendLine($"  {new string('─', 25)} {new string('─', 10)} {new string('─', 10)} {new string('─', 10)}");
            foreach (var (type, created, skipped, failed) in _schemaConstraints)
                sb.AppendLine($"  {type,-25} {created,-10} {skipped,-10} {failed,-10}");

            WriteFile("2-schema.txt", sb);
            _stepVerdicts["2. Schema"] = (verdict, $"{_schemaTables.Count(t => t.Status == "Created")} tables, {_schemaColumns.Count} columns, {_schemaConstraints.Sum(c => c.Created)} constraints");
        }

        // ─── Object Sync Report ─────────────────────────────────────────

        public static void AddObjectResult(string objectType, string objectName, string status)
        {
            if (!IsInitialized) return;
            _objectResults.Add((objectType, objectName, status));
        }

        public static void WriteObjectReport()
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            int failedCount = _objectResults.Count(o => o.Status == "Failed");
            var verdict = failedCount > 0 ? $"WARNING ({failedCount} failure(s))" : "PASS";

            WriteHeader(sb, "OBJECT SYNC", verdict);

            // Summary per type
            sb.AppendLine("═══ Summary by Type ═══");
            sb.AppendLine($"  {"Type",-25} {"Created",-10} {"Identical",-10} {"Diverged",-10} {"Skipped",-10} {"Failed",-10}");
            sb.AppendLine($"  {new string('─', 25)} {new string('─', 10)} {new string('─', 10)} {new string('─', 10)} {new string('─', 10)} {new string('─', 10)}");
            var byType = _objectResults.GroupBy(o => o.ObjectType);
            foreach (var group in byType)
            {
                int created = group.Count(o => o.Status == "Created");
                int identical = group.Count(o => o.Status == "Identical");
                int diverged = group.Count(o => o.Status == "Diverged");
                int skipped = group.Count(o => o.Status == "Skipped");
                int failed = group.Count(o => o.Status == "Failed");
                sb.AppendLine($"  {group.Key,-25} {created,-10} {identical,-10} {diverged,-10} {skipped,-10} {failed,-10}");
            }
            sb.AppendLine();

            // Detailed list (Created and Failed only to keep report focused)
            var notable = _objectResults.Where(o => o.Status == "Created" || o.Status == "Failed" || o.Status == "Diverged").ToList();
            if (notable.Count > 0)
            {
                sb.AppendLine("═══ Notable Objects ═══");
                sb.AppendLine($"  {"Status",-12} {"Type",-25} {"Name"}");
                sb.AppendLine($"  {new string('─', 12)} {new string('─', 25)} {new string('─', 50)}");
                foreach (var (type, name, status) in notable)
                    sb.AppendLine($"  {status,-12} {type,-25} {name}");
            }

            WriteFile("3-objects.txt", sb);
            _stepVerdicts["3. Objects"] = (verdict, $"{_objectResults.Count(o => o.Status == "Created")} created, {_objectResults.Count(o => o.Status == "Diverged")} diverged, {_objectResults.Count(o => o.Status == "Skipped")} skipped");
        }

        // ─── DataSync Report ────────────────────────────────────────────

        public static void SetDataSyncTenantInfo(int sourceTenantId, int targetTenantId, string tenantName)
        {
            if (!IsInitialized) return;
            _sourceTenantId = sourceTenantId;
            _targetTenantId = targetTenantId;
        }

        public static void AddDataSyncTable(string sourceTable, string targetTable, long rows, long elapsedMs, string method, string? error)
        {
            if (!IsInitialized) return;
            _dataSyncTables.Add((sourceTable, targetTable, rows, elapsedMs, method, error));
        }

        public static void WriteDataSyncReport(long totalRows, long totalElapsedMs)
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            int errorCount = _dataSyncTables.Count(t => t.Error != null);
            var verdict = errorCount > 0 ? $"WARNING ({errorCount} error(s))" : "PASS";

            WriteHeader(sb, "DATA SYNC", verdict);

            sb.AppendLine($"  Tenant Mapping: Source ID {_sourceTenantId} → Target ID {_targetTenantId}");
            sb.AppendLine($"  Total Rows:     {totalRows:N0}");
            sb.AppendLine($"  Total Time:     {FormatDuration(totalElapsedMs)}");
            sb.AppendLine($"  Tables:         {_dataSyncTables.Count}");
            sb.AppendLine();

            // Table details
            sb.AppendLine("═══ Per-Table Results ═══");
            sb.AppendLine($"  {"Source",-40} {"Target",-40} {"Rows",12} {"Time",10} {"Rows/s",10} {"Method",-20} {"Status"}");
            sb.AppendLine($"  {new string('─', 40)} {new string('─', 40)} {new string('─', 12)} {new string('─', 10)} {new string('─', 10)} {new string('─', 20)} {new string('─', 10)}");

            foreach (var (src, tgt, rows, ms, method, error) in _dataSyncTables.OrderBy(t => t.Error != null ? 0 : 1))
            {
                string rowsPerSec = ms > 0 ? $"{(rows * 1000 / ms):N0}" : "—";
                string status = error != null ? $"ERROR: {error}" : "OK";
                string timeStr = ms > 0 ? FormatDuration(ms) : "—";
                sb.AppendLine($"  {src,-40} {tgt,-40} {rows,12:N0} {timeStr,10} {rowsPerSec,10} {method,-20} {status}");
            }

            // Errors section
            if (errorCount > 0)
            {
                sb.AppendLine();
                sb.AppendLine("═══ Errors ═══");
                foreach (var (src, tgt, _, _, _, error) in _dataSyncTables.Where(t => t.Error != null))
                    sb.AppendLine($"  {src} → {tgt}: {error}");
            }

            // Top 10 by time
            sb.AppendLine();
            sb.AppendLine("═══ Slowest Tables (Top 10) ═══");
            foreach (var (src, tgt, rows, ms, method, _) in _dataSyncTables.OrderByDescending(t => t.ElapsedMs).Take(10))
            {
                string rowsPerSec = ms > 0 ? $"{(rows * 1000 / ms):N0}" : "—";
                sb.AppendLine($"  {src,-40} {rows,12:N0} rows  {FormatDuration(ms),10}  ({rowsPerSec} rows/sec)");
            }

            WriteFile("4-datasync.txt", sb);
            _stepVerdicts["4. DataSync"] = (verdict, $"{totalRows:N0} rows across {_dataSyncTables.Count} tables in {FormatDuration(totalElapsedMs)}");
        }

        // ─── FK Remap Report ────────────────────────────────────────────

        public static void AddFkUpdate(string childTable, string childColumn, string refTable, string refColumn, long rowsUpdated)
        {
            if (!IsInitialized) return;
            _fkUpdates.Add((childTable, childColumn, refTable, refColumn, rowsUpdated));
        }

        public static void WriteFkReport(int disabledCount, int enabledCount)
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            var verdict = enabledCount >= disabledCount ? "PASS" : $"WARNING ({disabledCount - enabledCount} FK(s) not re-enabled)";

            WriteHeader(sb, "FK CONSTRAINT OPERATIONS", verdict);

            sb.AppendLine($"  FK Constraints Disabled: {disabledCount}");
            sb.AppendLine($"  FK Constraints Enabled:  {enabledCount}");
            sb.AppendLine();

            // FK column remapping
            sb.AppendLine("═══ FK Column Remapping (IdMapping → New IDs) ═══");
            if (_fkUpdates.Count == 0)
                sb.AppendLine("  (none)");
            else
            {
                sb.AppendLine($"  {"Child Table.Column",-55} {"→ Ref Table.Column",-55} {"Rows Updated",12}");
                sb.AppendLine($"  {new string('─', 55)} {new string('─', 55)} {new string('─', 12)}");
                foreach (var (child, col, refTbl, refCol, rows) in _fkUpdates.OrderByDescending(f => f.RowsUpdated))
                    sb.AppendLine($"  {child + "." + col,-55} {"→ " + refTbl + "." + refCol,-55} {rows,12:N0}");
                sb.AppendLine();
                sb.AppendLine($"  Total FK updates: {_fkUpdates.Count}, Total rows remapped: {_fkUpdates.Sum(f => f.RowsUpdated):N0}");
            }

            WriteFile("5-fk-remap.txt", sb);
            _stepVerdicts["5. FK Remap"] = (verdict, $"{_fkUpdates.Count} FK columns, {_fkUpdates.Sum(f => f.RowsUpdated):N0} rows remapped");
        }

        // ─── Validation Report ──────────────────────────────────────────

        public static void AddValidationRow(string tableName, long sourceCount, long targetCount, string method, string status)
        {
            if (!IsInitialized) return;
            _validationRows.Add((tableName, sourceCount, targetCount, method, status));
        }

        public static void AddFkViolation(string table, string constraint, string where)
        {
            if (!IsInitialized) return;
            _fkViolations.Add($"{table} | {constraint} | {where}");
        }

        public static void WriteValidationReport(int matchCount, int idMappingMatchCount, int discrepancyCount)
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            var verdict = discrepancyCount == 0 && _fkViolations.Count == 0 ? "PASS"
                        : _fkViolations.Count > 0 ? $"WARNING ({discrepancyCount} row mismatches, {_fkViolations.Count} FK violations)"
                        : $"WARNING ({discrepancyCount} row mismatch(es))";

            WriteHeader(sb, "VALIDATION", verdict);

            sb.AppendLine($"  Row Count Matches (by tenant):    {matchCount}");
            sb.AppendLine($"  Row Count Matches (via IdMapping): {idMappingMatchCount}");
            sb.AppendLine($"  Row Count Mismatches:              {discrepancyCount}");
            sb.AppendLine($"  FK Violations:                     {_fkViolations.Count}");
            sb.AppendLine();

            // Row count details — matches
            var matches = _validationRows.Where(r => r.Status == "MATCH").ToList();
            if (matches.Count > 0)
            {
                sb.AppendLine("═══ Row Count Matches ═══");
                sb.AppendLine($"  {"Table",-45} {"Source",12} {"Target",12} {"Method",-20}");
                sb.AppendLine($"  {new string('─', 45)} {new string('─', 12)} {new string('─', 12)} {new string('─', 20)}");
                foreach (var (table, src, tgt, method, _) in matches)
                    sb.AppendLine($"  {table,-45} {src,12:N0} {tgt,12:N0} {method,-20}");
                sb.AppendLine();
            }

            // Row count details — mismatches
            var mismatches = _validationRows.Where(r => r.Status == "MISMATCH").ToList();
            if (mismatches.Count > 0)
            {
                sb.AppendLine("═══ Row Count MISMATCHES ═══");
                sb.AppendLine($"  {"Table",-45} {"Source",12} {"Target",12} {"Method",-20} {"Diff",12}");
                sb.AppendLine($"  {new string('─', 45)} {new string('─', 12)} {new string('─', 12)} {new string('─', 20)} {new string('─', 12)}");
                foreach (var (table, src, tgt, method, _) in mismatches)
                    sb.AppendLine($"  {table,-45} {src,12:N0} {tgt,12:N0} {method,-20} {tgt - src,12:N0}");
                sb.AppendLine();
            }

            // FK Violations
            if (_fkViolations.Count > 0)
            {
                sb.AppendLine("═══ FK Violations ═══");
                sb.AppendLine($"  {"Table",-40} {"Constraint",-50} {"Where"}");
                sb.AppendLine($"  {new string('─', 40)} {new string('─', 50)} {new string('─', 40)}");
                foreach (var v in _fkViolations)
                {
                    var parts = v.Split(" | ", 3);
                    if (parts.Length == 3)
                        sb.AppendLine($"  {parts[0],-40} {parts[1],-50} {parts[2]}");
                    else
                        sb.AppendLine($"  {v}");
                }
            }

            WriteFile("6-validation.txt", sb);
            _stepVerdicts["6. Validation"] = (verdict, $"{matchCount + idMappingMatchCount} matched, {discrepancyCount} mismatched, {_fkViolations.Count} FK violations");
        }

        // ─── Summary Report ─────────────────────────────────────────────

        public static void WriteSummaryReport()
        {
            if (!IsInitialized) return;
            var sb = new StringBuilder();
            var elapsed = DateTime.UtcNow - _runStart;
            bool anyFail = _stepVerdicts.Values.Any(v => v.Verdict.StartsWith("FAIL"));
            bool anyWarn = _stepVerdicts.Values.Any(v => v.Verdict.StartsWith("WARNING"));
            var overallVerdict = anyFail ? "FAIL" : anyWarn ? "WARNING" : "PASS";

            sb.AppendLine("╔══════════════════════════════════════════════════════════════════╗");
            sb.AppendLine("║              MIGRATION SUMMARY REPORT                            ║");
            sb.AppendLine("╚══════════════════════════════════════════════════════════════════╝");
            sb.AppendLine();
            sb.AppendLine($"  Tenant:   {_tenantName}");
            sb.AppendLine($"  Source:   TenantId {_sourceTenantId}");
            sb.AppendLine($"  Target:   TenantId {_targetTenantId}");
            sb.AppendLine($"  Started:  {_runStart:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"  Finished: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"  Duration: {FormatDuration((long)elapsed.TotalMilliseconds)}");
            sb.AppendLine();
            sb.AppendLine($"  ┌─────────────────────────────────────┐");
            sb.AppendLine($"  │  OVERALL RESULT: {overallVerdict,-20}│");
            sb.AppendLine($"  └─────────────────────────────────────┘");
            sb.AppendLine();

            sb.AppendLine("═══ Step Results ═══");
            sb.AppendLine($"  {"Step",-20} {"Verdict",-30} {"Detail"}");
            sb.AppendLine($"  {new string('─', 20)} {new string('─', 30)} {new string('─', 50)}");
            foreach (var (step, (verdict, detail)) in _stepVerdicts.OrderBy(kv => kv.Key))
                sb.AppendLine($"  {step,-20} {verdict,-30} {detail}");

            // Copy supporting files into report folder for self-contained audit
            CopyToReportFolder(Helper.MdcOnlyTablesFilePath, "mdc_only_tables.txt");
            CopyRollbackFolder();

            sb.AppendLine();
            sb.AppendLine("═══ Report Files ═══");
            sb.AppendLine($"  {_reportDir}/");
            var allFiles = Directory.Exists(_reportDir!) ? Directory.GetFiles(_reportDir!, "*", SearchOption.TopDirectoryOnly) : Array.Empty<string>();
            foreach (var file in allFiles.OrderBy(f => Path.GetFileName(f)))
                sb.AppendLine($"    {Path.GetFileName(file)}");
            var subDirs = Directory.Exists(_reportDir!) ? Directory.GetDirectories(_reportDir!) : Array.Empty<string>();
            foreach (var dir in subDirs)
                sb.AppendLine($"    {Path.GetFileName(dir)}/");

            WriteFile("summary.txt", sb);
            Log.Information("[Report] Summary written to {ReportDir}/summary.txt", _reportDir);
        }

        // ─── Helpers ────────────────────────────────────────────────────

        private static void WriteHeader(StringBuilder sb, string title, string verdict)
        {
            sb.AppendLine($"╔══════════════════════════════════════════════════════════════════╗");
            sb.AppendLine($"║  MIGRATION REPORT: {title,-46}║");
            sb.AppendLine($"╚══════════════════════════════════════════════════════════════════╝");
            sb.AppendLine();
            sb.AppendLine($"  Tenant:  {_tenantName} (Source: {_sourceTenantId} → Target: {_targetTenantId})");
            sb.AppendLine($"  Date:    {_runStart:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"  Result:  {verdict}");
            sb.AppendLine();
        }

        private static void WriteFile(string fileName, StringBuilder sb)
        {
            if (_reportDir == null) return;
            var path = Path.Combine(_reportDir, fileName);
            try
            {
                File.WriteAllText(path, sb.ToString());
                Log.Information("[Report] Written: {FileName}", path);
            }
            catch (Exception ex)
            {
                Log.Warning("[Report] Failed to write {FileName}: {ErrorMessage}", fileName, ex.Message);
            }
        }

        private static void CopyToReportFolder(string sourcePath, string destFileName)
        {
            if (_reportDir == null || !File.Exists(sourcePath)) return;
            try
            {
                File.Copy(sourcePath, Path.Combine(_reportDir, destFileName), overwrite: true);
            }
            catch (Exception ex)
            {
                Log.Warning("[Report] Failed to copy {Source} to report folder: {ErrorMessage}", sourcePath, ex.Message);
            }
        }

        private static void CopyRollbackFolder()
        {
            if (_reportDir == null) return;
            var rollbackDir = RollbackLogger.GetRunFolder();
            if (!Directory.Exists(rollbackDir)) return;
            try
            {
                var destDir = Path.Combine(_reportDir, "rollbacks");
                Directory.CreateDirectory(destDir);
                foreach (var file in Directory.GetFiles(rollbackDir))
                    File.Copy(file, Path.Combine(destDir, Path.GetFileName(file)), overwrite: true);
            }
            catch (Exception ex)
            {
                Log.Warning("[Report] Failed to copy rollback scripts to report folder: {ErrorMessage}", ex.Message);
            }
        }

        private static string FormatDuration(long ms)
        {
            if (ms < 1000) return $"{ms}ms";
            if (ms < 60000) return $"{ms / 1000.0:F1}s";
            if (ms < 3600000) return $"{ms / 60000}m {(ms % 60000) / 1000}s";
            return $"{ms / 3600000}h {(ms % 3600000) / 60000}m";
        }
    }
}
