using Dapper;
using Microsoft.Data.SqlClient;
using Serilog;

namespace Logistics.DbMerger
{
    public class ObjectSync
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;

        private record ObjectHashInfo(string Name, byte[] DefinitionHash);

        public ObjectSync(string sourceConnStr, string targetConnStr)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
        }

        public async Task SyncObjectsAsync(bool dryRun)
        {
            // Types: P (SQL_STORED_PROCEDURE), V (VIEW), FN (SQL_SCALAR_FUNCTION), TF (SQL_TABLE_VALUED_FUNCTION), IF (SQL_INLINE_TABLE_VALUED_FUNCTION), TR (SQL_TRIGGER)
            var types = new[] { "P", "V", "FN", "TF", "IF", "TR" };

            const string hashQuery = @"
                SELECT o.name AS Name, HASHBYTES('SHA2_256', m.definition) AS DefinitionHash
                FROM sys.objects o
                JOIN sys.sql_modules m ON o.object_id = m.object_id
                WHERE o.type = @Type AND o.is_ms_shipped = 0";

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            foreach (var type in types)
            {
                var typeName = GetTypeName(type);
                Log.Information("\n[Objects] Checking {TypeName}...", typeName);

                var sourceObjects = (await source.QueryAsync<ObjectHashInfo>(hashQuery, new { Type = type })).ToList();
                var targetObjects = (await target.QueryAsync<ObjectHashInfo>(hashQuery, new { Type = type })).ToList();

                var targetLookup = targetObjects.ToDictionary(
                    o => o.Name,
                    o => o.DefinitionHash,
                    StringComparer.OrdinalIgnoreCase);

                int createdCount = 0, identicalCount = 0, divergedCount = 0, skippedCount = 0, failedCount = 0;

                foreach (var obj in sourceObjects)
                {
                    // Task 1: Filter backup/test objects
                    if (TableSkipRules.ShouldSkipObject(obj.Name))
                    {
                        Log.Information("[Objects] Skipped backup/test: {ObjectName}", obj.Name);
                        skippedCount++;
                        ReportWriter.AddObjectResult(typeName, obj.Name, "Skipped");
                        continue;
                    }

                    // Task 2: Three-way comparison
                    if (targetLookup.TryGetValue(obj.Name, out var targetHash))
                    {
                        // P1 fix: guard against NULL definitions (encrypted/synonym objects)
                        if (obj.DefinitionHash is null || targetHash is null)
                        {
                            Log.Warning("[Objects] WARNING: Diverged — {ObjectName} (cannot compare — encrypted or missing definition)", obj.Name);
                            divergedCount++;
                            ReportWriter.AddObjectResult(typeName, obj.Name, "Diverged");
                        }
                        else if (obj.DefinitionHash.SequenceEqual(targetHash))
                        {
                            Log.Information("[Objects] Identical: {ObjectName}", obj.Name);
                            identicalCount++;
                            ReportWriter.AddObjectResult(typeName, obj.Name, "Identical");
                        }
                        else
                        {
                            Log.Warning("[Objects] WARNING: Diverged — {ObjectName}", obj.Name);
                            divergedCount++;
                            ReportWriter.AddObjectResult(typeName, obj.Name, "Diverged");
                        }
                        continue;
                    }

                    // Source-only object — create in target
                    if (dryRun)
                    {
                        Log.Information("[DryRun] Would create {TypeName}: {ObjectName}", typeName, obj.Name);
                        createdCount++;
                        continue;
                    }

                    try
                    {
                        var definition = await source.QueryFirstOrDefaultAsync<string>(
                            "SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID(@Name)", new { Name = obj.Name });

                        if (string.IsNullOrEmpty(definition))
                        {
                            Log.Warning("[Objects] Skipped {ObjectName} — definition is null or empty (encrypted?)", obj.Name);
                            skippedCount++;
                            continue;
                        }

                        // Split on GO batch separators — most definitions are single-batch,
                        // but defensive handling prevents silent failures
                        var batches = System.Text.RegularExpressions.Regex.Split(definition,
                            @"^\s*GO\s*$",
                            System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                        foreach (var batch in batches)
                        {
                            var trimmed = batch.Trim();
                            if (!string.IsNullOrEmpty(trimmed))
                                await target.ExecuteAsync(trimmed);
                        }
                        Log.Information("[Objects] Created: {ObjectName}", obj.Name);
                        RollbackLogger.LogObjectCreation(obj.Name, typeName);
                        createdCount++;
                        ReportWriter.AddObjectResult(typeName, obj.Name, "Created");
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[Error] Failed to create {ObjectName}: {ErrorMessage}", obj.Name, ex.Message);
                        failedCount++;
                        ReportWriter.AddObjectResult(typeName, obj.Name, "Failed");
                    }
                }

                // Task 3: Per-type summary
                var createdLabel = dryRun ? "would create" : "created";
                var summary = $"[Objects] {typeName}: {createdCount} {createdLabel}, {identicalCount} identical, {divergedCount} diverged, {skippedCount} skipped";
                if (failedCount > 0)
                    summary += $", {failedCount} failed";
                Log.Information("{Summary}", summary);
            }

            ReportWriter.WriteObjectReport();
        }

        private string GetTypeName(string code) => code switch
        {
            "P" => "Stored Procedures",
            "V" => "Views",
            "FN" => "Scalar Functions",
            "TF" => "Table Functions",
            "IF" => "Inline Functions",
            "TR" => "Triggers",
            _ => code
        };
    }
}
