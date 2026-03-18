using Microsoft.Data.SqlClient;
using Dapper;
using Serilog;

namespace Logistics.DbMerger
{
    public static class PreFlightValidator
    {
        private record IdentityColumnInfo(string TableName, string ColumnName, string DataType);

        // Non-numeric identity types that cannot be compared with MAX for range overlap
        private static readonly HashSet<string> NonNumericIdentityTypes = new(StringComparer.OrdinalIgnoreCase)
        {
            "uniqueidentifier"
        };

        /// <summary>Returns (overlaps, failed, safe, skipped) counts for caller to assess blocking status.</summary>
        public static async Task<(int Overlaps, int Failed, int Safe, int Skipped)> RunIdentityRangeCheckAsync(string sourceConnStr, string targetConnStr)
        {
            Log.Information("\n[PreFlight] === Identity Range Overlap Check ===");

            using var source = new SqlConnection(sourceConnStr);
            using var target = new SqlConnection(targetConnStr);

            // Step 1: Get identity columns from both databases
            var identitySql = @"
                SELECT t.name AS TableName, c.name AS ColumnName, TYPE_NAME(c.system_type_id) AS DataType
                FROM sys.identity_columns ic
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON ic.object_id = t.object_id
                WHERE t.is_ms_shipped = 0
                ORDER BY t.name";

            var sourceIdentity = (await source.QueryAsync<IdentityColumnInfo>(identitySql)).ToList();
            var targetIdentity = (await target.QueryAsync<IdentityColumnInfo>(identitySql)).ToList();

            // Deduplicate by table name (tables with multiple identity columns only check once)
            var sourceByTable = sourceIdentity
                .GroupBy(x => x.TableName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
            var targetByTable = targetIdentity
                .GroupBy(x => x.TableName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            int checkedCount = 0, overlaps = 0, safe = 0, skipped = 0, failed = 0, sourceOnly = 0, targetOnly = 0;

            // Step 2: Check each source identity table (iterate deduplicated dictionary, not raw list)
            foreach (var si in sourceByTable.Values)
            {
                if (TableSkipRules.ShouldSkipTable(si.TableName))
                {
                    skipped++;
                    continue;
                }

                // P2 fix: Skip non-numeric identity columns (e.g., uniqueidentifier)
                if (NonNumericIdentityTypes.Contains(si.DataType))
                {
                    Log.Information("[PreFlight] INFO: {TableName} — {DataType} identity column, skipping numeric overlap check", si.TableName, si.DataType);
                    skipped++;
                    continue;
                }

                if (!targetByTable.ContainsKey(si.TableName))
                {
                    Log.Information("[PreFlight] INFO: {TableName} — exists only in source, skipping overlap check", si.TableName);
                    sourceOnly++;
                    continue;
                }

                checkedCount++;
                var ti = targetByTable[si.TableName];

                try
                {
                    // Query MAX(Id) from both databases (bracket-escape identifiers for defense-in-depth)
                    var srcColEsc = si.ColumnName.Replace("]", "]]");
                    var srcTblEsc = si.TableName.Replace("]", "]]");
                    var tgtColEsc = ti.ColumnName.Replace("]", "]]");
                    var tgtTblEsc = ti.TableName.Replace("]", "]]");
                    var sourceMax = await source.QueryFirstOrDefaultAsync<long?>($"SELECT MAX([{srcColEsc}]) FROM [{srcTblEsc}]");
                    var targetMax = await target.QueryFirstOrDefaultAsync<long?>($"SELECT MAX([{tgtColEsc}]) FROM [{tgtTblEsc}]");

                    if (!sourceMax.HasValue || sourceMax == 0)
                    {
                        Log.Information("[PreFlight] OK: {TableName} — empty in source", si.TableName);
                        safe++;
                        continue;
                    }

                    if (!targetMax.HasValue || targetMax == 0)
                    {
                        Log.Information("[PreFlight] OK: {TableName} — empty in target", si.TableName);
                        safe++;
                        continue;
                    }

                    // Both databases use IDENTITY(1,1), so any non-empty pair has overlapping ranges.
                    // IdMapping always generates new IDs, so this is informational — no actual collision risk.
                    // Flag sourceMax >= targetMax as noteworthy (source has more rows than target).
                    if (sourceMax >= targetMax)
                    {
                        Log.Warning("[PreFlight] INFO: {TableName} — Source MAX({SourceColumn})={SourceMax}, Target MAX({TargetColumn})={TargetMax}. Ranges overlap but IdMapping generates new IDs (no collision risk).", si.TableName, si.ColumnName, sourceMax, ti.ColumnName, targetMax);
                        overlaps++;
                    }
                    else
                    {
                        Log.Information("[PreFlight] OK: {TableName} — Source MAX={SourceMax} < Target MAX={TargetMax} (IdMapping generates new IDs)", si.TableName, sourceMax, targetMax);
                        safe++;
                    }
                }
                catch (Exception ex)
                {
                    // P1 fix: Track failed tables so summary counters add up
                    // P4 fix: Use [PreFlight] tag instead of [Error]
                    Log.Error("[PreFlight] ERROR: {TableName} — {ErrorMessage}", si.TableName, ex.Message);
                    failed++;
                }
            }

            // Step 3: Check for target-only identity tables
            foreach (var ti in targetIdentity)
            {
                // P5 fix: Count skipped tables in target-only loop
                if (TableSkipRules.ShouldSkipTable(ti.TableName))
                {
                    skipped++;
                    continue;
                }
                if (!sourceByTable.ContainsKey(ti.TableName))
                {
                    Log.Information("[PreFlight] INFO: {TableName} — exists only in target, skipping overlap check", ti.TableName);
                    targetOnly++;
                }
            }

            // Step 4: Summary
            Log.Information("\n[PreFlight] Summary: {CheckedCount} tables checked, {Overlaps} overlaps, {Safe} safe, {Failed} failed, {Skipped} skipped, {SourceOnly} source-only, {TargetOnly} target-only", checkedCount, overlaps, safe, failed, skipped, sourceOnly, targetOnly);

            if (overlaps == 0 && failed == 0)
                Log.Information("[PreFlight] All identity ranges safe — no overlaps detected");
            else if (overlaps > 0)
                Log.Information("[PreFlight] {Overlaps} potential overlap(s) detected — review before data migration", overlaps);

            if (failed > 0)
                Log.Warning("[PreFlight] {Failed} table(s) failed to check — review errors above", failed);

            Log.Information("[PreFlight] === Identity Range Check Complete ===\n");
            return (overlaps, failed, safe, skipped);
        }

        /// <summary>Returns count of partition schemes needing remap.</summary>
        public static async Task<int> RunPartitionFileGroupCheckAsync(string sourceConnStr, string targetConnStr)
        {
            Log.Information("\n[PreFlight] === Partition Filegroup Check ===");

            using var source = new SqlConnection(sourceConnStr);
            using var target = new SqlConnection(targetConnStr);

            // Query source partition schemes with their filegroups
            var sourceSchemes = await source.QueryAsync<dynamic>(@"
                SELECT
                    ps.name AS SchemeName,
                    pf.name AS FunctionName,
                    ds.name AS FileGroupName
                FROM sys.partition_schemes ps
                INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
                INNER JOIN sys.data_spaces ds ON dds.data_space_id = ds.data_space_id
                ORDER BY ps.name, dds.destination_id");

            if (!sourceSchemes.Any())
            {
                Log.Information("[PreFlight] No partition schemes in source — filegroup check skipped");
                Log.Information("[PreFlight] === Partition Filegroup Check Complete ===\n");
                return 0;
            }

            // Query target filegroups
            var targetFileGroups = (await target.QueryAsync<string>(
                "SELECT name FROM sys.data_spaces WHERE type = 'FG'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var schemeGroups = sourceSchemes.GroupBy(s => (string)s.SchemeName, StringComparer.OrdinalIgnoreCase);
            int schemesChecked = 0, schemesNeedingRemap = 0, schemesOK = 0;

            foreach (var group in schemeGroups)
            {
                string schemeName = group.Key;
                schemesChecked++;

                var missingFgs = group
                    .Select(g => (string)g.FileGroupName)
                    .Where(fg => !targetFileGroups.Contains(fg))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                if (missingFgs.Any())
                {
                    foreach (var fg in missingFgs)
                    {
                        Log.Warning("[PreFlight] WARNING: Partition scheme '{SchemeName}' references filegroup '{FileGroup}' — not found in target, will be remapped to PRIMARY", schemeName, fg);
                    }
                    schemesNeedingRemap++;
                }
                else
                {
                    Log.Information("[PreFlight] OK: Partition scheme '{SchemeName}' — all filegroups present in target", schemeName);
                    schemesOK++;
                }
            }

            // Summary
            Log.Information("\n[PreFlight] Partition summary: {SchemesChecked} schemes checked, {SchemesNeedingRemap} need remap, {SchemesOk} OK", schemesChecked, schemesNeedingRemap, schemesOK);

            if (schemesNeedingRemap == 0)
                Log.Information("[PreFlight] All partition filegroups present in target — no remapping needed");
            else
                Log.Information("[PreFlight] {SchemesNeedingRemap} scheme(s) will be remapped to PRIMARY during schema sync", schemesNeedingRemap);

            Log.Information("[PreFlight] === Partition Filegroup Check Complete ===\n");
            return schemesNeedingRemap;
        }

        /// <summary>Returns (mismatch, sourceCollation, targetCollation).</summary>
        public static async Task<(bool Mismatch, string SourceCollation, string TargetCollation)> RunCollationCheckAsync(string sourceConnStr, string targetConnStr)
        {
            Log.Information("\n[PreFlight] === Database Collation Check ===");

            using var source = new SqlConnection(sourceConnStr);
            using var target = new SqlConnection(targetConnStr);

            var sourceCollation = await source.QueryFirstOrDefaultAsync<string>(
                "SELECT collation_name FROM sys.databases WHERE database_id = DB_ID()") ?? "unknown";
            var targetCollation = await target.QueryFirstOrDefaultAsync<string>(
                "SELECT collation_name FROM sys.databases WHERE database_id = DB_ID()") ?? "unknown";

            bool mismatch = !string.Equals(sourceCollation, targetCollation, StringComparison.OrdinalIgnoreCase);
            if (!mismatch)
                Log.Information("[PreFlight] OK: Database collations match ({Collation})", sourceCollation);
            else
                Log.Warning("[PreFlight] WARNING: Database collation mismatch — Source: {SourceCollation}, Target: {TargetCollation}", sourceCollation, targetCollation);

            Log.Information("[PreFlight] === Database Collation Check Complete ===\n");
            return (mismatch, sourceCollation, targetCollation);
        }
    }
}
