using Dapper;
using Microsoft.Data.SqlClient;
using Serilog;
using System.Text;

namespace Logistics.DbMerger
{
    public class Validator
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;

        public Validator(string sourceConnStr, string targetConnStr)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
        }

        /// <summary>
        /// Runs validation using separate source and target tenant IDs.
        /// Source queries filter by sourceTenantId; target queries filter by targetTenantId.
        /// </summary>
        public async Task RunValidationAsync(int? sourceTenantId, int? targetTenantId)
        {
            Log.Information("\n[Validation] Starting Validation — Source TenantId: {SourceTenantId}, Target TenantId: {TargetTenantId}",
                sourceTenantId?.ToString() ?? "All", targetTenantId?.ToString() ?? "All");

            var (matchCount, idMappingMatchCount, discrepancyCount) = await ValidateRowCountsAsync(sourceTenantId, targetTenantId);
            await ValidateForeignKeysAsync();
            if (targetTenantId.HasValue)
            {
                await ValidateBusinessLogicAsync(targetTenantId.Value);
            }
            ReportWriter.WriteValidationReport(matchCount, idMappingMatchCount, discrepancyCount);
        }

        private async Task<(int MatchCount, int IdMappingMatchCount, int DiscrepancyCount)> ValidateRowCountsAsync(int? sourceTenantId, int? targetTenantId)
        {
            Log.Information("\n[1/3] Validating Row Counts...");
            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var tables = await target.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");
            var discrepancies = new List<string>();
            int matchCount = 0;
            int idMappingMatchCount = 0;

            foreach (var table in tables)
            {
                if (TableSkipRules.ShouldSkipTable(table))
                    continue;

                try
                {
                    // Check if table exists in source
                    var sourceExists = await source.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @Name", new { Name = table }) > 0;
                    if (!sourceExists) continue;

                    // Check for TenantID column (case-insensitive via INFORMATION_SCHEMA)
                    var hasTenantInTarget = await target.ExecuteScalarAsync<int>(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName AND COLUMN_NAME IN ('TenantId','TenantID')",
                        new { TableName = table }) > 0;
                    var hasTenantInSource = await source.ExecuteScalarAsync<int>(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName AND COLUMN_NAME IN ('TenantId','TenantID')",
                        new { TableName = table }) > 0;

                    var tableEsc = table.Replace("]", "]]");

                    if (hasTenantInSource && hasTenantInTarget && sourceTenantId.HasValue && targetTenantId.HasValue)
                    {
                        // Tables with TenantId: direct count comparison filtered by tenant
                        long sourceCount = await source.ExecuteScalarAsync<long>(
                            $"SELECT COUNT(*) FROM [{tableEsc}] WHERE TenantId = @TenantId",
                            new { TenantId = sourceTenantId });
                        long targetCount = await target.ExecuteScalarAsync<long>(
                            $"SELECT COUNT(*) FROM [{tableEsc}] WHERE TenantId = @TenantId",
                            new { TenantId = targetTenantId });

                        if (sourceCount != targetCount)
                        {
                            string msg = $"[MISMATCH] {table}: Source={sourceCount}, Target={targetCount}";
                            Log.Warning("{Message}", msg);
                            discrepancies.Add(msg);
                            ReportWriter.AddValidationRow(table, sourceCount, targetCount, "Tenant", "MISMATCH");
                        }
                        else
                        {
                            matchCount++;
                            ReportWriter.AddValidationRow(table, sourceCount, targetCount, "Tenant", "MATCH");
                        }
                    }
                    else if (!hasTenantInTarget && sourceTenantId.HasValue && targetTenantId.HasValue)
                    {
                        // Tables WITHOUT TenantId: use IdMapping to count migrated rows for this tenant
                        // Check all three IdMapping tables for this table+tenant
                        long idMappingCount = 0;
                        foreach (var mappingTable in new[] { "IdMappingInt", "IdMappingBigInt", "IdMappingGuid" })
                        {
                            var mappingExists = await target.ExecuteScalarAsync<int>(
                                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @Name",
                                new { Name = mappingTable });
                            if (mappingExists > 0)
                            {
                                idMappingCount += await target.ExecuteScalarAsync<long>(
                                    $"SELECT COUNT(*) FROM [{mappingTable}] WHERE TableName = @TableName AND TenantId = @TenantId",
                                    new { TableName = table, TenantId = targetTenantId });
                            }
                        }

                        if (idMappingCount == 0)
                        {
                            // No IdMapping entries — table may be global, composite-PK, or was skipped; skip validation
                            continue;
                        }

                        // Source count: filter by source tenant if source has TenantId, otherwise count all
                        long sourceCount;
                        if (hasTenantInSource)
                            sourceCount = await source.ExecuteScalarAsync<long>(
                                $"SELECT COUNT(*) FROM [{tableEsc}] WHERE TenantId = @TenantId",
                                new { TenantId = sourceTenantId });
                        else
                            sourceCount = await source.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM [{tableEsc}]");

                        if (sourceCount != idMappingCount)
                        {
                            string msg = $"[MISMATCH] {table} (via IdMapping): Source={sourceCount}, Migrated={idMappingCount}";
                            Log.Warning("{Message}", msg);
                            discrepancies.Add(msg);
                            ReportWriter.AddValidationRow(table, sourceCount, idMappingCount, "IdMapping", "MISMATCH");
                        }
                        else
                        {
                            Log.Information("[Validator] {Table} (via IdMapping): {Count} rows — MATCH", table, idMappingCount);
                            idMappingMatchCount++;
                            ReportWriter.AddValidationRow(table, sourceCount, idMappingCount, "IdMapping", "MATCH");
                        }
                    }
                    else
                    {
                        // ALL tenants mode or no tenant filter: full table count comparison
                        long sourceCount = await source.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM [{tableEsc}]");
                        long targetCount = await target.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM [{tableEsc}]");

                        if (sourceCount != targetCount)
                        {
                            string msg = $"[MISMATCH] {table}: Source={sourceCount}, Target={targetCount}";
                            Log.Warning("{Message}", msg);
                            discrepancies.Add(msg);
                            ReportWriter.AddValidationRow(table, sourceCount, targetCount, "Full", "MISMATCH");
                        }
                        else
                        {
                            matchCount++;
                            ReportWriter.AddValidationRow(table, sourceCount, targetCount, "Full", "MATCH");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log.Warning("[Skip] {Table}: {ErrorMessage}", table, ex.Message);
                }
            }

            if (discrepancies.Count == 0)
                Log.Information("[OK] All verified table row counts match ({TenantCount} by tenant, {IdMappingCount} via IdMapping).", matchCount, idMappingMatchCount);
            else
                Log.Warning("[Validator] {DiscrepancyCount} discrepancy(ies) found. {MatchCount} by tenant matched, {IdMappingCount} via IdMapping matched.",
                    discrepancies.Count, matchCount, idMappingMatchCount);

            return (matchCount, idMappingMatchCount, discrepancies.Count);
        }

        private async Task ValidateForeignKeysAsync()
        {
            Log.Information("\n[2/3] Validating Foreign Keys in Target...");
            using var target = new SqlConnection(_targetConnStr);

            try
            {
                // DBCC CHECKCONSTRAINTS can be very slow on large databases — use extended timeout
                var results = await target.QueryAsync("DBCC CHECKCONSTRAINTS WITH ALL_CONSTRAINTS",
                    commandTimeout: 3600);
                if (results.Any())
                {
                    Log.Error("[ERROR] Foreign Key Violations Found:");
                    foreach (var row in results)
                    {
                        Log.Error(" - Table: {Table}, Constraint: {Constraint}, Where: {Where}", row.Table, row.Constraint, row.Where);
                        ReportWriter.AddFkViolation((string)row.Table, (string)row.Constraint, (string)row.Where);
                    }
                }
                else
                {
                    Log.Information("[OK] No Foreign Key violations detected.");
                }
            }
            catch (Exception ex)
            {
                Log.Error("[Error] FK Validation failed: {ErrorMessage}", ex.Message);
            }
        }

        private async Task ValidateBusinessLogicAsync(int targetTenantId)
        {
            Log.Information("\n[3/3] Running Business Validation Queries...");
            using var target = new SqlConnection(_targetConnStr);

            // 1. Contact Count
            await RunCheck(target, "Total Contacts",
                "SELECT COUNT(*) FROM Contact WHERE TenantID = @TenantId",
                new { TenantId = targetTenantId });

            // 2. Timeband Range
            await RunCheck(target, "Timeband Schedule Range",
                "SELECT 'Start: ' + CONVERT(varchar(30), MIN(ScheduleStart), 120) + ' End: ' + CONVERT(varchar(30), MAX(ScheduleEnd), 120) FROM Timeband WHERE TenantId = @TenantId",
                new { TenantId = targetTenantId });

            // 3. Leave Request Stats
            await RunCheck(target, "Leave Requests by Status",
                "SELECT 'Status ' + CAST(StatusID as varchar(36)) + ': ' + CAST(COUNT(*) as varchar(20)) FROM LeaveRequest WHERE TenantID = @TenantId GROUP BY StatusID",
                new { TenantId = targetTenantId });
        }

        private async Task RunCheck(SqlConnection conn, string name, string sql, object? param = null)
        {
            try
            {
                var rows = await conn.QueryAsync(sql, param);

                Log.Information("--- {Name} ---", name);
                if (!rows.Any())
                {
                    Log.Information("(No results)");
                    return;
                }

                foreach (var row in rows)
                {
                   Log.Information("{Row}", row);
                }
            }
            catch(SqlException ex)
            {
                Log.Warning("[Skip] {Name}: {ErrorMessage}", name, ex.Message);
            }
        }
    }
}
