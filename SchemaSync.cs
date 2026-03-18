using Microsoft.Data.SqlClient;
using Dapper;
using Serilog;
using System.Text;
using System.IO;
using System.Linq;
using System.Diagnostics;

namespace Logistics.DbMerger
{
    public class SchemaSync
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;

        public SchemaSync(string sourceConnStr, string targetConnStr)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
        }

        /// <summary>Escape single quotes for use in SQL string literals (WHERE name = '...').</summary>
        private static string SqlEsc(string name) => name.Replace("'", "''");
        /// <summary>Escape brackets for use in SQL bracket-delimited identifiers ([...]).</summary>
        private static string BracketEsc(string name) => name.Replace("]", "]]");

        public async Task<List<string>> GetMissingTablesAsync()
        {
            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceTables = await source.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'");
            var targetTables = await target.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'");

            var missing = sourceTables
                .Except(targetTables, StringComparer.OrdinalIgnoreCase)
                .Where(t => !TableSkipRules.ShouldSkipTable(t))
                .ToList();
            return missing;
        }

        public async Task SyncTableAsync(string tableName)
        {
            var sw = Stopwatch.StartNew();
            using var target = new SqlConnection(_targetConnStr);

            // IF NOT EXISTS guard: check table existence before executing any batches
            var alreadyExists = await target.QueryFirstOrDefaultAsync<int>(
                "SELECT COUNT(*) FROM sys.tables WHERE name = @Name AND schema_id = SCHEMA_ID('dbo')",
                new { Name = tableName });
            if (alreadyExists > 0)
            {
                Log.Information("[Schema] Table already exists, skipping: {TableName}", tableName);
                ReportWriter.AddSchemaTable(tableName, "Skipped", 0);
                return;
            }

            var createScript = await GenerateCreateScriptAsync(tableName);

            // Split by GO batch separator — matches GO on its own line only (no trailing text except optional semicolon)
            // This avoids matching GO inside comments like "-- GO forward" or string literals
            var batches = System.Text.RegularExpressions.Regex.Split(createScript, @"^\s*GO\s*;?\s*$", System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            bool anyBatchExecuted = false;
            foreach (var batch in batches)
            {
                if (!string.IsNullOrWhiteSpace(batch))
                {
                    try
                    {
                        await target.ExecuteAsync(batch);
                        // H3: Log rollback entry on first successful batch so partial creates can be rolled back
                        if (!anyBatchExecuted)
                        {
                            anyBatchExecuted = true;
                            RollbackLogger.LogTableCreation(tableName);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "[Error] Failed batch for {TableName} (rollback entry exists if table was partially created)", tableName);
                        throw;
                    }
                }
            }

            if (anyBatchExecuted)
            {
                Log.Information("[Schema] Created table {TableName} (with Constraints)", tableName);
            }

            sw.Stop();
            Log.Information("[Schema] Synced table {TableName} in {ElapsedMs}ms", tableName, sw.ElapsedMilliseconds);
            if (anyBatchExecuted)
                ReportWriter.AddSchemaTable(tableName, "Created", sw.ElapsedMilliseconds);
        }

        private async Task<string> GenerateCreateScriptAsync(string tableName)
        {
            using var source = new SqlConnection(_sourceConnStr);

            // 1. Fetch Columns with extended properties (Identity, Computed)
            // Join with sys.computed_columns to get definition if computed
            var columns = await source.QueryAsync<dynamic>(@"
                SELECT 
                    c.name AS ColumnName,
                    t.name AS DataType,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    c.is_computed,
                    c.column_id,
                    cc.definition as ComputedDefinition
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                WHERE c.object_id = OBJECT_ID(@TableName)
                ORDER BY c.column_id", new { TableName = tableName });

            // 2. Fetch Default Constraints
            var defaults = await source.QueryAsync<dynamic>(@"
                SELECT 
                    name AS ConstraintName,
                    definition AS DefaultValue,
                    parent_column_id
                FROM sys.default_constraints
                WHERE parent_object_id = OBJECT_ID(@TableName)", new { TableName = tableName });
            var defaultDict = defaults.ToDictionary(k => (int)k.parent_column_id, v => (string)v.DefaultValue);

            // 3. Fetch Check Constraints
            var checks = await source.QueryAsync<dynamic>(@"
                SELECT definition AS CheckDefinition, name AS CheckName
                FROM sys.check_constraints
                WHERE parent_object_id = OBJECT_ID(@TableName)", new { TableName = tableName });

            // 4. Fetch Indexes (PK and Non-Clustered)
            var indexes = await source.QueryAsync<dynamic>(@"
                SELECT 
                    i.index_id,
                    i.name AS IndexName,
                    i.type_desc AS IndexType,
                    i.is_primary_key,
                    i.is_unique,
                    i.is_padded,
                    i.ignore_dup_key,
                    i.allow_row_locks,
                    i.allow_page_locks,
                    i.fill_factor,
                    s.no_recompute,
                    i.filter_definition
                FROM sys.indexes i
                LEFT JOIN sys.stats s ON i.object_id = s.object_id AND i.index_id = s.stats_id
                WHERE i.object_id = OBJECT_ID(@TableName) 
                  AND i.type_desc <> 'HEAP'
                ORDER BY i.is_primary_key DESC, i.name", new { TableName = tableName });

            // Fetch Index Columns for ALL indexes
            var indexCols = await source.QueryAsync<dynamic>(@"
                SELECT 
                    ic.index_id,
                    c.name AS ColumnName,
                    ic.is_descending_key,
                    ic.is_included_column
                FROM sys.index_columns ic
                INNER JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                WHERE ic.object_id = OBJECT_ID(@TableName)
                ORDER BY ic.index_id, ic.key_ordinal", new { TableName = tableName });

            var indexColLookup = indexCols.GroupBy(x => (int)x.index_id).ToDictionary(g => g.Key, g => g.ToList());

            var pkInfo = indexes.FirstOrDefault(i => i.is_primary_key == true);

            var sb = new StringBuilder();
            sb.AppendLine("SET ANSI_NULLS ON");
            sb.AppendLine("GO");
            sb.AppendLine("SET QUOTED_IDENTIFIER ON");
            sb.AppendLine("GO");
            sb.AppendLine($"CREATE TABLE [dbo].[{BracketEsc(tableName)}](");

            var colList = columns.ToList();
            bool hasLob = false;

            for (int i = 0; i < colList.Count; i++)
            {
                var col = colList[i];

                // Computed Column
                if (col.is_computed)
                {
                    sb.Append($"	[{BracketEsc(col.ColumnName)}] AS ({col.ComputedDefinition})");
                }
                else
                {
                    string typeDef = $"[{col.DataType}]";
                    string typeLower = ((string)col.DataType).ToLower();

                    if (typeLower == "nvarchar" || typeLower == "varchar" || typeLower == "char" || typeLower == "nchar" || typeLower == "varbinary" || typeLower == "binary")
                    {
                        string len = col.max_length == -1 ? "max" : (typeLower.StartsWith("n") ? col.max_length / 2 : col.max_length).ToString();
                        typeDef += $"({len})";
                        if (len == "max") hasLob = true;
                    }
                    else if (typeLower == "decimal" || typeLower == "numeric")
                    {
                        typeDef += $"({col.precision}, {col.scale})";
                    }
                    else if (typeLower == "text" || typeLower == "ntext" || typeLower == "image" || typeLower == "xml")
                    {
                        hasLob = true;
                    }

                    string identity = col.is_identity == true ? " IDENTITY(1,1)" : "";
                    string nullable = col.is_nullable == true ? " NULL" : " NOT NULL";

                    sb.Append($"	[{BracketEsc(col.ColumnName)}] {typeDef}{identity}{nullable}");

                    // Default Constraint
                    if (defaultDict.ContainsKey((int)col.column_id))
                    {
                        var defNameRaw = $"DF_{tableName}_{col.ColumnName}";
                        string defName = defNameRaw.Length > 128 ? defNameRaw.Substring(0, 128) : defNameRaw;
                        sb.Append($" CONSTRAINT [{BracketEsc(defName)}] DEFAULT {defaultDict[(int)col.column_id]}");
                    }
                }

                // Comma Logic
                if (i < colList.Count - 1 || pkInfo != null || checks.Any())
                    sb.AppendLine(",");
                else
                    sb.AppendLine("");
            }

            // Append PK with Options
            if (pkInfo != null)
            {
                sb.AppendLine($" CONSTRAINT [{BracketEsc(pkInfo.IndexName)}] PRIMARY KEY {pkInfo.IndexType} ");
                sb.AppendLine("(");

                var pkCols = indexColLookup.ContainsKey((int)pkInfo.index_id) ? indexColLookup[(int)pkInfo.index_id] : new List<dynamic>();

                for (int k = 0; k < pkCols.Count; k++)
                {
                    var c = pkCols[k];
                    sb.Append($"	[{BracketEsc(c.ColumnName)}] {(c.is_descending_key ? "DESC" : "ASC")}");
                    if (k < pkCols.Count - 1) sb.Append(",");
                    sb.AppendLine();
                }

                // Construct Options
                var opts = new List<string>();
                opts.Add($"PAD_INDEX = {(pkInfo.is_padded == true ? "ON" : "OFF")}");
                opts.Add($"STATISTICS_NORECOMPUTE = {(pkInfo.no_recompute == true ? "ON" : "OFF")}");
                opts.Add($"IGNORE_DUP_KEY = {(pkInfo.ignore_dup_key == true ? "ON" : "OFF")}");
                opts.Add($"ALLOW_ROW_LOCKS = {(pkInfo.allow_row_locks == true ? "ON" : "OFF")}");
                opts.Add($"ALLOW_PAGE_LOCKS = {(pkInfo.allow_page_locks == true ? "ON" : "OFF")}");
                if (pkInfo.fill_factor != null && pkInfo.fill_factor > 0)
                {
                    opts.Add($"FILLFACTOR = {pkInfo.fill_factor}");
                }

                sb.Append($")WITH ({string.Join(", ", opts)}) ON [PRIMARY]");

                if (checks.Any()) sb.AppendLine(","); else sb.AppendLine("");
            }

            // Append Check Constraints
            var checkList = checks.ToList();
            for (int c = 0; c < checkList.Count; c++)
            {
                var chk = checkList[c];
                sb.Append($"	CONSTRAINT [{BracketEsc(chk.CheckName)}] CHECK {chk.CheckDefinition}");
                if (c < checkList.Count - 1) sb.AppendLine(","); else sb.AppendLine("");
            }

            sb.Append(") ON [PRIMARY]");
            if (hasLob) sb.Append(" TEXTIMAGE_ON [PRIMARY]");

            sb.AppendLine("");
            sb.AppendLine("GO");

            // Append Non-Clustered Indexes
            foreach (var idx in indexes)
            {
                if (idx.is_primary_key == true) continue;

                string unique = idx.is_unique == true ? "UNIQUE " : "";
                string type = idx.IndexType; // NONCLUSTERED
                sb.AppendLine($"CREATE {unique}{type} INDEX [{BracketEsc(idx.IndexName)}] ON [dbo].[{BracketEsc(tableName)}]");
                sb.Append("(");

                var cols = indexColLookup.ContainsKey((int)idx.index_id) ? indexColLookup[(int)idx.index_id] : new List<dynamic>();
                var keyCols = cols.Where(x => x.is_included_column == false).ToList();
                var incCols = cols.Where(x => x.is_included_column == true).ToList();

                for (int k = 0; k < keyCols.Count; k++)
                {
                    var c = keyCols[k];
                    sb.Append($"[{BracketEsc(c.ColumnName)}] {(c.is_descending_key ? "DESC" : "ASC")}");
                    if (k < keyCols.Count - 1) sb.Append(", ");
                }
                sb.Append(")");

                if (incCols.Any())
                {
                    sb.AppendLine();
                    sb.Append("INCLUDE (");
                    for (int k = 0; k < incCols.Count; k++)
                    {
                        sb.Append($"[{BracketEsc(incCols[k].ColumnName)}]");
                        if (k < incCols.Count - 1) sb.Append(", ");
                    }
                    sb.Append(")");
                }

                if (!string.IsNullOrEmpty(idx.filter_definition))
                {
                    sb.AppendLine();
                    sb.Append($"WHERE {idx.filter_definition}");
                }

                sb.AppendLine(" WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]");
                sb.AppendLine("GO");
            }

            return sb.ToString();
        }

        public async Task<List<string>> GetExistingTargetTablesAsync()
        {
            using var target = new SqlConnection(_targetConnStr);
            return (await target.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'")).ToList();
        }

        public async Task<List<string>> GetExistingSourceTablesAsync()
        {
            using var source = new SqlConnection(_sourceConnStr);
            return (await source.QueryAsync<string>("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'")).ToList();
        }

        public async Task<List<dynamic>> GetRequiredColumnsAsync(string tableName)
        {
            using var source = new SqlConnection(_sourceConnStr);
            var cols = await source.QueryAsync<dynamic>(@"
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @Name", new { Name = tableName });
            return cols.ToList();
        }

        public async Task AlterTableAsync(string tableName, List<dynamic> newColumns)
        {
            if (!newColumns.Any()) return;

            using var target = new SqlConnection(_targetConnStr);
            foreach (var col in newColumns)
            {
                string typeDef = $"{col.DATA_TYPE}";
                if (col.DATA_TYPE == "nvarchar" || col.DATA_TYPE == "varchar" || col.DATA_TYPE == "char" || col.DATA_TYPE == "nchar" || col.DATA_TYPE == "varbinary" || col.DATA_TYPE == "binary")
                {
                    string len = col.CHARACTER_MAXIMUM_LENGTH == -1 ? "MAX" : col.CHARACTER_MAXIMUM_LENGTH.ToString();
                    typeDef += $"({len})";
                }
                else if (col.DATA_TYPE == "decimal" || col.DATA_TYPE == "numeric")
                {
                    typeDef += $"({col.NUMERIC_PRECISION}, {col.NUMERIC_SCALE})";
                }

                // IF NOT EXISTS guard for idempotent re-runs
                var colExistsAlready = await target.QueryFirstOrDefaultAsync<int>(
                    "SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(@Table) AND name = @Col",
                    new { Table = $"[dbo].[{tableName}]", Col = (string)col.COLUMN_NAME });
                if (colExistsAlready > 0)
                {
                    continue;
                }

                // Respect source nullability: if source says NOT NULL, still add as NULL
                // because target table may have existing rows that need a default value.
                // Adding NOT NULL without a default would fail on tables with existing data.
                string nullable = "NULL";
                string definition = $"ALTER TABLE [{BracketEsc(tableName)}] ADD [{BracketEsc(col.COLUMN_NAME)}] {typeDef} {nullable}";

                Log.Information("[Schema] Altering {TableName}: Adding {ColumnName}", tableName, col.COLUMN_NAME);
                await target.ExecuteAsync(definition);
                ReportWriter.AddSchemaColumn(tableName, col.COLUMN_NAME);

                // Log Rollback (Drop Column)
                // Note: RollbackLogger needs update or we just use raw SQL string
                // Dropping a column is: ALTER TABLE x DROP COLUMN y
                // We'll append manually for now or add helper.
                string rollback = $"IF EXISTS(SELECT * FROM sys.columns WHERE Name = N'{SqlEsc(col.COLUMN_NAME)}' AND Object_ID = Object_ID(N'dbo.{SqlEsc(tableName)}')) ALTER TABLE [dbo].[{BracketEsc(tableName)}] DROP COLUMN [{BracketEsc(col.COLUMN_NAME)}];\n";
                RollbackLogger.LogCustomScript(rollback);
            }
        }

        public async Task SyncTableSchemaAsync(string sourceTable, string targetTable, bool dryRun = false)
        {
            var sw = Stopwatch.StartNew();
            var sourceCols = await GetRequiredColumnsAsync(sourceTable);

            using var target = new SqlConnection(_targetConnStr);
            var targetCols = (await target.QueryAsync<string>("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @Name", new { Name = targetTable })).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var missingCols = sourceCols.Where(c => !targetCols.Contains((string)c.COLUMN_NAME)).ToList();

            if (missingCols.Any())
            {
                if (dryRun)
                {
                    foreach (var col in missingCols)
                        Log.Information("[DryRun] Would Add Column: {TargetTable}.{ColumnName}", targetTable, col.COLUMN_NAME);
                }
                else
                {
                    await AlterTableAsync(targetTable, missingCols);
                }
            }
            /* Verbose logging
            else
            {
                // Console.WriteLine($"[Schema] No new columns to add for {targetTable}");
            }
            */

            sw.Stop();
            if (sw.ElapsedMilliseconds > 100)
                Log.Information("[Schema] Column sync for {TargetTable} in {ElapsedMs}ms", targetTable, sw.ElapsedMilliseconds);
        }

        public async Task SyncAllConstraintsAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            var sw = Stopwatch.StartNew();
            Log.Information("\n=== [Post-Table] Constraint & Partition Sync ===");
            await SyncDefaultConstraintsAsync(tableMappings, dryRun);
            await SyncCheckConstraintsAsync(tableMappings, dryRun);
            await SyncIndexesAsync(tableMappings, dryRun);
            await SyncForeignKeysAsync(tableMappings, dryRun);
            await SyncPartitionsAsync(dryRun);
            Log.Information("=== [Post-Table] Constraint Sync Complete ===\n");

            sw.Stop();
            Log.Information("[Schema] Constraint sync completed in {ElapsedMs}ms", sw.ElapsedMilliseconds);
        }

        public async Task SyncForeignKeysAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Log.Information("\n[Constraints] Syncing Foreign Keys...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceFKs = await source.QueryAsync<dynamic>(@"
                SELECT 
                    fk.object_id AS FKObjectId,
                    fk.name AS FKName,
                    OBJECT_NAME(fk.parent_object_id) AS ParentTable,
                    OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable,
                    fk.delete_referential_action_desc AS DeleteAction,
                    fk.update_referential_action_desc AS UpdateAction
                FROM sys.foreign_keys fk
                ORDER BY OBJECT_NAME(fk.parent_object_id), fk.name");

            var sourceFKCols = await source.QueryAsync<dynamic>(@"
                SELECT 
                    fkc.constraint_object_id AS FKObjectId,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ParentColumn,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ReferencedColumn
                FROM sys.foreign_key_columns fkc
                ORDER BY fkc.constraint_object_id, fkc.constraint_column_id");

            var fkColLookup = sourceFKCols.GroupBy(x => (int)x.FKObjectId)
                .ToDictionary(g => g.Key, g => g.ToList());

            var targetFKNames = (await target.QueryAsync<string>("SELECT name FROM sys.foreign_keys"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetColData = await target.QueryAsync<dynamic>(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS");
            var targetColsByTable = targetColData
                .GroupBy(x => (string)x.TABLE_NAME, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(c => (string)c.COLUMN_NAME).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var fk in sourceFKs)
            {
                string fkName = fk.FKName;
                string parentTable = fk.ParentTable;
                string referencedTable = fk.ReferencedTable;
                int fkObjectId = fk.FKObjectId;

                string targetParent = tableMappings.TryGetValue(parentTable, out var mp) ? mp : parentTable;
                string targetRef = tableMappings.TryGetValue(referencedTable, out var mr) ? mr : referencedTable;

                if (!targetTables.Contains(targetParent) || !targetTables.Contains(targetRef))
                { skipped++; continue; }

                if (targetFKNames.Contains(fkName))
                    continue;

                if (!fkColLookup.TryGetValue(fkObjectId, out var cols) || !cols.Any())
                { skipped++; continue; }

                bool valid = true;
                if (targetColsByTable.TryGetValue(targetParent, out var pCols) &&
                    targetColsByTable.TryGetValue(targetRef, out var rCols))
                {
                    foreach (var c in cols)
                    {
                        if (!pCols.Contains((string)c.ParentColumn) || !rCols.Contains((string)c.ReferencedColumn))
                        { valid = false; break; }
                    }
                }
                else { valid = false; }

                if (!valid)
                {
                    Log.Information("[FK] Skipping {FkName}: columns missing in target", fkName);
                    skipped++;
                    continue;
                }

                var parentColList = string.Join(", ", cols.Select(c => $"[{BracketEsc((string)c.ParentColumn)}]"));
                var refColList = string.Join(", ", cols.Select(c => $"[{BracketEsc((string)c.ReferencedColumn)}]"));
                string deleteAction = ((string)fk.DeleteAction).Replace("_", " ");
                string updateAction = ((string)fk.UpdateAction).Replace("_", " ");

                // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                var fkNameEsc = fkName.Replace("'", "''");
                var fkNameBracket = fkName.Replace("]", "]]");
                var targetParentBracket = targetParent.Replace("]", "]]");
                var targetRefBracket = targetRef.Replace("]", "]]");
                string sql = $@"
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = N'{fkNameEsc}')
    ALTER TABLE [dbo].[{targetParentBracket}] ADD CONSTRAINT [{fkNameBracket}] FOREIGN KEY ({parentColList}) REFERENCES [dbo].[{targetRefBracket}] ({refColList}) ON DELETE {deleteAction} ON UPDATE {updateAction}";

                if (dryRun)
                {
                    Log.Information("[DryRun] Would create FK: {FkName} ({TargetParent} -> {TargetRef})", fkName, targetParent, targetRef);
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Log.Information("[FK] Created: {FkName} ({TargetParent} -> {TargetRef})", fkName, targetParent, targetRef);
                        RollbackLogger.LogCustomScript(
                            $"IF OBJECT_ID(N'{fkNameEsc}', 'F') IS NOT NULL ALTER TABLE [dbo].[{targetParentBracket}] DROP CONSTRAINT [{fkNameBracket}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[FK] Failed {FkName}: {ErrorMessage}", fkName, ex.Message);
                        failed++;
                    }
                }
            }

            Log.Information("[FK] Summary: {Created} created, {Skipped} skipped, {Failed} failed", created, skipped, failed);
            ReportWriter.AddSchemaConstraint("Foreign Keys", created, skipped, failed);
        }

        public async Task SyncIndexesAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Log.Information("\n[Constraints] Syncing Indexes & Unique Constraints...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceIndexes = await source.QueryAsync<dynamic>(@"
                SELECT 
                    i.object_id,
                    i.index_id,
                    i.name AS IndexName,
                    OBJECT_NAME(i.object_id) AS TableName,
                    i.type_desc AS IndexType,
                    i.is_unique,
                    i.is_unique_constraint,
                    i.is_primary_key,
                    i.filter_definition,
                    i.is_padded,
                    i.ignore_dup_key,
                    i.allow_row_locks,
                    i.allow_page_locks,
                    i.fill_factor,
                    s.no_recompute
                FROM sys.indexes i
                LEFT JOIN sys.stats s ON i.object_id = s.object_id AND i.index_id = s.stats_id
                WHERE i.is_primary_key = 0
                  AND i.type_desc <> 'HEAP'
                  AND i.name IS NOT NULL
                ORDER BY OBJECT_NAME(i.object_id), i.name");

            var sourceIdxCols = await source.QueryAsync<dynamic>(@"
                SELECT 
                    ic.object_id,
                    ic.index_id,
                    c.name AS ColumnName,
                    ic.is_descending_key,
                    ic.is_included_column
                FROM sys.index_columns ic
                INNER JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
                INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                WHERE i.is_primary_key = 0 AND i.type_desc <> 'HEAP' AND i.name IS NOT NULL
                ORDER BY ic.object_id, ic.index_id, ic.key_ordinal");

            var idxColLookup = sourceIdxCols
                .GroupBy(x => $"{(int)x.object_id}_{(int)x.index_id}")
                .ToDictionary(g => g.Key, g => g.ToList());

            var targetIndexNames = (await target.QueryAsync<string>(
                "SELECT name FROM sys.indexes WHERE name IS NOT NULL"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetColData = await target.QueryAsync<dynamic>(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS");
            var targetColsByTable = targetColData
                .GroupBy(x => (string)x.TABLE_NAME, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(c => (string)c.COLUMN_NAME).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var idx in sourceIndexes)
            {
                string indexName = idx.IndexName;
                string sourceTable = idx.TableName;
                int objectId = idx.object_id;
                int indexId = idx.index_id;
                string lookupKey = $"{objectId}_{indexId}";

                string targetTable = tableMappings.TryGetValue(sourceTable, out var mt) ? mt : sourceTable;

                if (!targetTables.Contains(targetTable))
                { skipped++; continue; }

                if (targetIndexNames.Contains(indexName))
                    continue;

                if (!idxColLookup.TryGetValue(lookupKey, out var cols) || !cols.Any())
                { skipped++; continue; }

                if (!targetColsByTable.TryGetValue(targetTable, out var tCols))
                { skipped++; continue; }

                bool valid = cols.All(c => tCols.Contains((string)c.ColumnName));
                if (!valid)
                { skipped++; continue; }

                var keyCols = cols.Where(x => x.is_included_column == false).ToList();
                var incCols = cols.Where(x => x.is_included_column == true).ToList();

                string sql;
                bool isUniqueConstraint = idx.is_unique_constraint == true;

                if (isUniqueConstraint)
                {
                    var keyColStr = string.Join(", ", keyCols.Select(c => $"[{BracketEsc((string)c.ColumnName)}]"));
                    // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                    sql = $@"
IF NOT EXISTS (SELECT * FROM sys.key_constraints WHERE name = '{SqlEsc(indexName)}' AND type = 'UQ' AND parent_object_id = OBJECT_ID('[dbo].[{BracketEsc(targetTable)}]'))
    ALTER TABLE [dbo].[{BracketEsc(targetTable)}] ADD CONSTRAINT [{BracketEsc(indexName)}] UNIQUE ({keyColStr})";
                }
                else
                {
                    string unique = idx.is_unique == true ? "UNIQUE " : "";
                    string type = idx.IndexType;
                    var keyColStr = string.Join(", ", keyCols.Select(c =>
                        $"[{BracketEsc((string)c.ColumnName)}] {((bool)c.is_descending_key ? "DESC" : "ASC")}"));

                    // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                    sql = $@"
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{SqlEsc(indexName)}' AND object_id = OBJECT_ID('[dbo].[{BracketEsc(targetTable)}]'))
    CREATE {unique}{type} INDEX [{BracketEsc(indexName)}] ON [dbo].[{BracketEsc(targetTable)}] ({keyColStr})";

                    if (incCols.Any())
                    {
                        var incColStr = string.Join(", ", incCols.Select(c => $"[{BracketEsc((string)c.ColumnName)}]"));
                        sql += $" INCLUDE ({incColStr})";
                    }

                    if (!string.IsNullOrEmpty((string)idx.filter_definition))
                        sql += $" WHERE {idx.filter_definition}";

                    string padIndex = idx.is_padded == true ? "ON" : "OFF";
                    string noRecompute = idx.no_recompute == true ? "ON" : "OFF";
                    string ignoreDupKey = idx.ignore_dup_key == true ? "ON" : "OFF";
                    string rowLocks = idx.allow_row_locks == true ? "ON" : "OFF";
                    string pageLocks = idx.allow_page_locks == true ? "ON" : "OFF";

                    sql += $" WITH (PAD_INDEX = {padIndex}, STATISTICS_NORECOMPUTE = {noRecompute}, " +
                        $"IGNORE_DUP_KEY = {ignoreDupKey}, ALLOW_ROW_LOCKS = {rowLocks}, ALLOW_PAGE_LOCKS = {pageLocks})";
                }

                if (dryRun)
                {
                    Log.Information("[DryRun] Would create {IndexType}: {IndexName} on {TargetTable}", isUniqueConstraint ? "unique constraint" : "index", indexName, targetTable);
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Log.Information("[Index] Created: {IndexName} on {TargetTable}", indexName, targetTable);
                        if (isUniqueConstraint)
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = '{SqlEsc(indexName)}' AND type = 'UQ') ALTER TABLE [dbo].[{BracketEsc(targetTable)}] DROP CONSTRAINT [{BracketEsc(indexName)}];\n");
                        else
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{SqlEsc(indexName)}') DROP INDEX [{BracketEsc(indexName)}] ON [dbo].[{BracketEsc(targetTable)}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[Index] Failed {IndexName}: {ErrorMessage}", indexName, ex.Message);
                        failed++;
                    }
                }
            }

            Log.Information("[Index] Summary: {Created} created, {Skipped} skipped, {Failed} failed", created, skipped, failed);
            ReportWriter.AddSchemaConstraint("Indexes", created, skipped, failed);
        }

        public async Task SyncCheckConstraintsAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Log.Information("\n[Constraints] Syncing Check Constraints...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceChecks = await source.QueryAsync<dynamic>(@"
                SELECT 
                    cc.name AS ConstraintName,
                    OBJECT_NAME(cc.parent_object_id) AS TableName,
                    cc.definition AS CheckDefinition
                FROM sys.check_constraints cc
                ORDER BY OBJECT_NAME(cc.parent_object_id), cc.name");

            var targetCheckNames = (await target.QueryAsync<string>("SELECT name FROM sys.check_constraints"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var chk in sourceChecks)
            {
                string name = chk.ConstraintName;
                string sourceTable = chk.TableName;
                string definition = chk.CheckDefinition;

                string targetTable = tableMappings.TryGetValue(sourceTable, out var mt) ? mt : sourceTable;

                if (!targetTables.Contains(targetTable))
                { skipped++; continue; }

                if (targetCheckNames.Contains(name))
                    continue;

                // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                string sql = $@"
IF NOT EXISTS (SELECT * FROM sys.check_constraints WHERE name = '{SqlEsc(name)}')
    ALTER TABLE [dbo].[{BracketEsc(targetTable)}] ADD CONSTRAINT [{BracketEsc(name)}] CHECK {definition}";

                if (dryRun)
                {
                    Log.Information("[DryRun] Would create check: {Name} on {TargetTable}", name, targetTable);
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Log.Information("[Check] Created: {Name} on {TargetTable}", name, targetTable);
                        RollbackLogger.LogCustomScript(
                            $"IF OBJECT_ID('{SqlEsc(name)}', 'C') IS NOT NULL ALTER TABLE [dbo].[{BracketEsc(targetTable)}] DROP CONSTRAINT [{BracketEsc(name)}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[Check] Failed {Name}: {ErrorMessage}", name, ex.Message);
                        failed++;
                    }
                }
            }

            Log.Information("[Check] Summary: {Created} created, {Skipped} skipped, {Failed} failed", created, skipped, failed);
            ReportWriter.AddSchemaConstraint("Check Constraints", created, skipped, failed);
        }

        public async Task SyncDefaultConstraintsAsync(Dictionary<string, string> tableMappings, bool dryRun)
        {
            Log.Information("\n[Constraints] Syncing Default Constraints...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourceDefaults = await source.QueryAsync<dynamic>(@"
                SELECT 
                    dc.name AS ConstraintName,
                    OBJECT_NAME(dc.parent_object_id) AS TableName,
                    COL_NAME(dc.parent_object_id, dc.parent_column_id) AS ColumnName,
                    dc.definition AS DefaultValue
                FROM sys.default_constraints dc
                ORDER BY OBJECT_NAME(dc.parent_object_id), dc.name");

            var targetDefaults = await target.QueryAsync<dynamic>(@"
                SELECT 
                    dc.name AS ConstraintName,
                    OBJECT_NAME(dc.parent_object_id) AS TableName,
                    COL_NAME(dc.parent_object_id, dc.parent_column_id) AS ColumnName
                FROM sys.default_constraints dc");

            var targetDefaultNames = targetDefaults.Select(d => (string)d.ConstraintName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var targetDefaultsByCol = targetDefaults
                .Select(d => $"{((string)d.TableName).ToLowerInvariant()}.{((string)d.ColumnName).ToLowerInvariant()}")
                .ToHashSet();

            var targetTables = (await target.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var targetColData = await target.QueryAsync<dynamic>(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS");
            var targetColsByTable = targetColData
                .GroupBy(x => (string)x.TABLE_NAME, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(c => (string)c.COLUMN_NAME).ToHashSet(StringComparer.OrdinalIgnoreCase),
                    StringComparer.OrdinalIgnoreCase);

            int created = 0, skipped = 0, failed = 0;

            foreach (var df in sourceDefaults)
            {
                string name = df.ConstraintName;
                string sourceTable = df.TableName;
                string colName = df.ColumnName;
                string defaultVal = df.DefaultValue;

                string targetTable = tableMappings.TryGetValue(sourceTable, out var mt) ? mt : sourceTable;

                if (!targetTables.Contains(targetTable))
                { skipped++; continue; }

                if (targetDefaultNames.Contains(name))
                    continue;

                // Skip if column already has a default (may have been created with a different name)
                string tableColKey = $"{targetTable.ToLowerInvariant()}.{colName.ToLowerInvariant()}";
                if (targetDefaultsByCol.Contains(tableColKey))
                    continue;

                if (!targetColsByTable.TryGetValue(targetTable, out var cols) || !cols.Contains(colName))
                { skipped++; continue; }

                // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                string sql = $@"
IF NOT EXISTS (SELECT * FROM sys.default_constraints WHERE name = '{SqlEsc(name)}')
    ALTER TABLE [dbo].[{BracketEsc(targetTable)}] ADD CONSTRAINT [{BracketEsc(name)}] DEFAULT {defaultVal} FOR [{BracketEsc(colName)}]";

                if (dryRun)
                {
                    Log.Information("[DryRun] Would create default: {Name} on {TargetTable}.{ColumnName}", name, targetTable, colName);
                    created++;
                }
                else
                {
                    try
                    {
                        await target.ExecuteAsync(sql);
                        Log.Information("[Default] Created: {Name} on {TargetTable}.{ColumnName}", name, targetTable, colName);
                        RollbackLogger.LogCustomScript(
                            $"IF OBJECT_ID('{SqlEsc(name)}', 'D') IS NOT NULL ALTER TABLE [dbo].[{BracketEsc(targetTable)}] DROP CONSTRAINT [{BracketEsc(name)}];\n");
                        created++;
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[Default] Failed {Name}: {ErrorMessage}", name, ex.Message);
                        failed++;
                    }
                }
            }

            Log.Information("[Default] Summary: {Created} created, {Skipped} skipped, {Failed} failed", created, skipped, failed);
            ReportWriter.AddSchemaConstraint("Defaults", created, skipped, failed);
        }

        public async Task SyncPartitionsAsync(bool dryRun)
        {
            Log.Information("\n[Constraints] Checking Partitions...");

            using var source = new SqlConnection(_sourceConnStr);
            using var target = new SqlConnection(_targetConnStr);

            var sourcePFs = await source.QueryAsync<dynamic>(@"
                SELECT 
                    pf.function_id,
                    pf.name AS FunctionName,
                    pf.type_desc AS FunctionType,
                    pf.boundary_value_on_right,
                    t.name AS ParameterType,
                    pp.max_length,
                    pp.precision,
                    pp.scale
                FROM sys.partition_functions pf
                INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
                INNER JOIN sys.types t ON pp.system_type_id = t.system_type_id AND pp.user_type_id = t.user_type_id");

            if (!sourcePFs.Any())
            {
                Log.Information("[Partitions] No partition functions in source.");
            }
            else
            {
                var targetPFNames = (await target.QueryAsync<string>("SELECT name FROM sys.partition_functions"))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                int pfCreated = 0, pfFailed = 0;

                foreach (var pf in sourcePFs)
                {
                    string pfName = pf.FunctionName;

                    if (targetPFNames.Contains(pfName))
                    {
                        Log.Information("[Partitions] Function '{FunctionName}' already exists.", pfName);
                        continue;
                    }

                    var boundaries = await source.QueryAsync<dynamic>(@"
                        SELECT boundary_id, value
                        FROM sys.partition_range_values
                        WHERE function_id = @FunctionId
                        ORDER BY boundary_id", new { FunctionId = (int)pf.function_id });

                    string paramType = pf.ParameterType;
                    string typeDef = paramType;
                    string typeLower = paramType.ToLower();
                    if (typeLower == "nvarchar" || typeLower == "varchar" || typeLower == "char" || typeLower == "nchar" || typeLower == "varbinary")
                    {
                        string len = pf.max_length == -1 ? "MAX" : (typeLower.StartsWith("n") ? pf.max_length / 2 : pf.max_length).ToString();
                        typeDef += $"({len})";
                    }
                    else if (typeLower == "decimal" || typeLower == "numeric")
                    {
                        typeDef += $"({pf.precision}, {pf.scale})";
                    }

                    string range = pf.boundary_value_on_right == true ? "RIGHT" : "LEFT";
                    var boundaryValues = boundaries.Select(b =>
                    {
                        object val = b.value;
                        if (val is string s) return $"N'{s}'";
                        if (val is DateTime dt) return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
                        return val?.ToString() ?? "NULL";
                    });
                    string valuesStr = string.Join(", ", boundaryValues);

                    // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                    string createPF = $@"
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = '{SqlEsc(pfName)}')
    CREATE PARTITION FUNCTION [{BracketEsc(pfName)}] ({typeDef}) AS RANGE {range} FOR VALUES ({valuesStr})";

                    if (dryRun)
                    {
                        Log.Information("[DryRun] Would create partition function: {FunctionName}", pfName);
                        pfCreated++;
                    }
                    else
                    {
                        try
                        {
                            await target.ExecuteAsync(createPF);
                            Log.Information("[Partitions] Created function: {FunctionName}", pfName);
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{SqlEsc(pfName)}') DROP PARTITION FUNCTION [{BracketEsc(pfName)}];\n");
                            pfCreated++;
                        }
                        catch (Exception ex)
                        {
                            Log.Error("[Partitions] Failed function {FunctionName}: {ErrorMessage}", pfName, ex.Message);
                            pfFailed++;
                        }
                    }
                }

                Log.Information("[Partitions] Functions: {Created} created, {Failed} failed", pfCreated, pfFailed);
                ReportWriter.AddSchemaConstraint("Partition Functions", pfCreated, 0, pfFailed);
            }

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
                Log.Information("[Partitions] No partition schemes in source.");
            }
            else
            {
                var targetSchemeNames = (await target.QueryAsync<string>("SELECT name FROM sys.partition_schemes"))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var targetFileGroups = (await target.QueryAsync<string>(
                    "SELECT name FROM sys.data_spaces WHERE type = 'FG'"))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var schemeGroups = sourceSchemes.GroupBy(s => (string)s.SchemeName);
                int psCreated = 0, psFailed = 0;

                foreach (var group in schemeGroups)
                {
                    string schemeName = group.Key;

                    if (targetSchemeNames.Contains(schemeName))
                    {
                        Log.Information("[Partitions] Scheme '{SchemeName}' already exists.", schemeName);
                        continue;
                    }

                    string funcName = group.First().FunctionName;

                    var fileGroups = group.Select(g =>
                    {
                        string fg = (string)g.FileGroupName;
                        if (!targetFileGroups.Contains(fg))
                        {
                            Log.Warning("[PreFlight] Remapped partition filegroup '{FileGroup}' to PRIMARY (not found in target)", fg);
                            return "[PRIMARY]";
                        }
                        return $"[{BracketEsc(fg)}]";
                    });

                    string fgList = string.Join(", ", fileGroups);
                    // IF NOT EXISTS guard for idempotent re-runs (defense-in-depth alongside HashSet check)
                    string createPS = $@"
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = '{SqlEsc(schemeName)}')
    CREATE PARTITION SCHEME [{BracketEsc(schemeName)}] AS PARTITION [{BracketEsc(funcName)}] TO ({fgList})";

                    if (dryRun)
                    {
                        Log.Information("[DryRun] Would create partition scheme: {SchemeName}", schemeName);
                        psCreated++;
                    }
                    else
                    {
                        try
                        {
                            await target.ExecuteAsync(createPS);
                            Log.Information("[Partitions] Created scheme: {SchemeName}", schemeName);
                            RollbackLogger.LogCustomScript(
                                $"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{SqlEsc(schemeName)}') DROP PARTITION SCHEME [{BracketEsc(schemeName)}];\n");
                            psCreated++;
                        }
                        catch (Exception ex)
                        {
                            Log.Error("[Partitions] Failed scheme {SchemeName}: {ErrorMessage}", schemeName, ex.Message);
                            psFailed++;
                        }
                    }
                }

                Log.Information("[Partitions] Schemes: {Created} created, {Failed} failed", psCreated, psFailed);
                ReportWriter.AddSchemaConstraint("Partition Schemes", psCreated, 0, psFailed);
            }

            var partitionedTables = await source.QueryAsync<dynamic>(@"
                SELECT 
                    OBJECT_NAME(p.object_id) AS TableName,
                    ps.name AS SchemeName,
                    pf.name AS FunctionName,
                    COUNT(DISTINCT p.partition_number) AS PartitionCount
                FROM sys.partitions p
                INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
                INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                WHERE i.index_id <= 1
                GROUP BY p.object_id, ps.name, pf.name
                ORDER BY OBJECT_NAME(p.object_id)");

            if (partitionedTables.Any())
            {
                Log.Information("\n[Partitions] Partitioned Tables in Source:");
                foreach (var pt in partitionedTables)
                {
                    Log.Information("   {TableName} -> Scheme: {SchemeName}, Function: {FunctionName}, Partitions: {PartitionCount}", pt.TableName, pt.SchemeName, pt.FunctionName, pt.PartitionCount);
                }
                Log.Information("[Partitions] Note: Table-level partition assignment requires index rebuild and should be reviewed manually.");
            }
        }
    }
}
