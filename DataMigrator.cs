using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Dapper;
using Serilog;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Target column schema plus default expression and nullability (from INFORMATION_SCHEMA.COLUMNS).
    /// </summary>
    public class TargetColumnInfo
    {
        public ColumnSchema Schema { get; set; } = null!;
        public string? DefaultValue { get; set; }
        /// <summary>True when IS_NULLABLE = 'YES'. Used to choose type-based default for NOT NULL ADC-only columns.</summary>
        public bool IsNullable { get; set; } = true;
    }

    /// <summary>
    /// Primary key column info for a table. If PkColumnCount > 1, the table has a composite PK (IdMapping not supported for it).
    /// </summary>
    public class PkColumnInfo
    {
        public string ColumnName { get; set; } = "";
        public string DataType { get; set; } = "";
        public int PkColumnCount { get; set; }
    }

    public class DataMigrator
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;
        private readonly int _batchSize;

        /// <summary>
        /// Known ADC-only columns and their default value for migration (when source has no such column).
        /// Other ADC-only columns use COLUMN_DEFAULT or NULL.
        /// </summary>
        private static readonly Dictionary<string, object> KnownAdcOnlyDefaults = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
        {
            ["OldSAPID"] = DBNull.Value,
            ["PartTimeFlex"] = (object)false,
            ["FobNumber"] = DBNull.Value,
            ["ExcludeOvertime"] = (object)false,
            ["ExcludeOvertimeComment"] = DBNull.Value,
            ["VisaRestriction"] = (object)false,
            ["VisaEndDate"] = DBNull.Value,
            ["MaximumHoursByFortnight"] = DBNull.Value,
            ["RosterProfile"] = DBNull.Value,
            ["ExcludeFromNotifications"] = (object)false,
            ["Order"] = (object)0, // sort/display order (ADC-only, NOT NULL)
            ["Type"] = (object)0, // Qualification.Type etc. (ADC-only, NOT NULL)
            ["CreationTime"] = (object)"__USE_DATETIME_NOW__", // Sentinel: resolved to DateTime.UtcNow at call time (SQL path uses SYSDATETIME())
            ["MandatoryQualification"] = (object)false, // Contact. ADC-only, bit NOT NULL
            ["TeamMemberBreakGroup"] = (object)false, // Settings. ADC-only, bit NOT NULL
            ["UKGPunchIntegration"] = (object)false, // Settings. ADC-only, bit NOT NULL
            // Other NOT NULL ADC-only columns use type-based default when targetCol is provided
        };

        /// <summary>Cumulative row count across all Migrate* calls on this instance. Read from caller for summary.</summary>
        public long TotalRowsMigrated { get; private set; }

        public DataMigrator(string sourceConnStr, string targetConnStr, int batchSize = 5000)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
            _batchSize = batchSize;
        }

        /// <summary>
        /// Gets primary key column info (first column only). Returns null if no PK.
        /// If PkColumnCount > 1, caller should skip IdMapping (composite PK).
        /// DataType is normalized (int, bigint, uniqueidentifier) for choosing IdMapping table.
        /// </summary>
        public static async Task<PkColumnInfo?> GetPkColumnInfoAsync(SqlConnection conn, string tableName)
        {
            var fullName = "dbo." + tableName;
            var rows = await conn.QueryAsync<(string ColumnName, string DataType, int KeyOrdinal)>(@"
                SELECT c.name AS ColumnName, t.name AS DataType, ic.key_ordinal AS KeyOrdinal
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(@TableName)
                  AND ic.is_included_column = 0
                ORDER BY ic.key_ordinal", new { TableName = fullName });
            var list = rows.ToList();
            if (list.Count == 0) return null;
            var first = list[0];
            var dataType = (first.DataType ?? "").ToLowerInvariant();
            return new PkColumnInfo
            {
                ColumnName = first.ColumnName,
                DataType = dataType,
                PkColumnCount = list.Count
            };
        }

        /// <summary>
        /// Gets all PK column names for a table (ordered by key_ordinal). Returns empty list if no PK.
        /// </summary>
        public static async Task<List<string>> GetPkColumnNamesAsync(SqlConnection conn, string tableName)
        {
            var fullName = "dbo." + tableName;
            var rows = await conn.QueryAsync<string>(@"
                SELECT c.name AS ColumnName
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(@TableName)
                  AND ic.is_included_column = 0
                ORDER BY ic.key_ordinal", new { TableName = fullName });
            return rows.ToList();
        }

        /// <summary>
        /// FK column info for a table (child column and its referenced table/column for IdMapping join).
        /// </summary>
        public class FkColumnInfo
        {
            public string ChildColumn { get; set; } = "";
            public string ReferencedTable { get; set; } = "";
            public string ReferencedColumn { get; set; } = "";
            public string DataType { get; set; } = "";
        }

        /// <summary>
        /// Gets FK columns for a table (parent_object_id = table). Used for composite-key table migration to join IdMapping.
        /// </summary>
        public static async Task<List<FkColumnInfo>> GetFkColumnsForTableAsync(SqlConnection conn, string tableName)
        {
            var fullName = "dbo." + tableName;
            var rows = await conn.QueryAsync<(string ChildColumn, string ReferencedTable, string ReferencedColumn, string DataType)>(@"
                SELECT cChild.name AS ChildColumn,
                       OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable,
                       cRef.name AS ReferencedColumn,
                       t.name AS DataType
                FROM sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns cChild ON fkc.parent_object_id = cChild.object_id AND fkc.parent_column_id = cChild.column_id
                INNER JOIN sys.columns cRef ON fkc.referenced_object_id = cRef.object_id AND fkc.referenced_column_id = cRef.column_id
                INNER JOIN sys.types t ON cChild.user_type_id = t.user_type_id
                WHERE fk.parent_object_id = OBJECT_ID(@TableName)
                ORDER BY fkc.constraint_column_id", new { TableName = fullName });
            return rows.Select(r => new FkColumnInfo
            {
                ChildColumn = r.ChildColumn,
                ReferencedTable = r.ReferencedTable,
                ReferencedColumn = r.ReferencedColumn,
                DataType = (r.DataType ?? "").ToLowerInvariant()
            }).ToList();
        }

        private async Task<bool> HasIdentityColumnAsync(SqlConnection conn, string tableName, SqlTransaction? transaction = null)
        {
            var fullName = "dbo." + tableName;
            var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*)
                FROM sys.identity_columns
                WHERE object_id = OBJECT_ID(@TableName)", new { TableName = fullName }, transaction: transaction);
            return count > 0;
        }



        private async Task<bool> HasTenantIdColumnAsync(SqlConnection conn, string tableName)
        {
            var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName AND COLUMN_NAME = 'TenantId'", new { TableName = tableName });
            return count > 0;
        }

        /// <summary>
        /// Returns true if the table has a TenantId column (for building whereClause).
        /// </summary>
        public static async Task<bool> TableHasTenantIdColumnAsync(SqlConnection conn, string tableName)
        {
            var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName AND COLUMN_NAME = 'TenantId'", new { TableName = tableName });
            return count > 0;
        }

        public async Task MigrateTableAsync(string sourceTableName, bool isNewTable, string? targetTableName = null, int? sourceTenantId = null, int? targetTenantId = null, Dictionary<long, long>? userMapping = null, SqlConnection? externalSourceConn = null, SqlConnection? externalTargetConn = null)
        {
            string destTable = targetTableName ?? sourceTableName;
            Log.Information("[Data] Migrating {SourceTable} -> {DestTable}...", sourceTableName, destTable);
            var sw = Stopwatch.StartNew();

            var ownSource = externalSourceConn == null;
            var ownTarget = externalTargetConn == null;
            var sourceConn = externalSourceConn ?? new SqlConnection(_sourceConnStr);
            var targetConn = externalTargetConn ?? new SqlConnection(_targetConnStr);
            try
            {
                if (ownSource) await sourceConn.OpenAsync();
                if (ownTarget) await targetConn.OpenAsync();

            bool hasIdentity = await HasIdentityColumnAsync(targetConn, destTable);

            // Check Tenant Filter eligibility
            bool hasTenantId = await HasTenantIdColumnAsync(sourceConn, sourceTableName);
            string whereClause = "";

            if (sourceTenantId.HasValue && hasTenantId)
            {
                whereClause = " WHERE TenantId = @TenantId";
                Log.Information("   -> Filtering by TenantId = {SourceTenantId}", sourceTenantId.Value);
            }
            else if (sourceTenantId.HasValue && !hasTenantId)
            {
                Log.Information("   -> Table has no TenantId. Migrating ALL rows (Global/System Table).");
            }

            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var targetColsWithDefault = await GetTargetColumnsWithDefaultsAsync(targetConn, destTable);
            var targetColsByName = targetColsWithDefault.ToDictionary(t => t.Schema.ColumnName, t => t.Schema, StringComparer.OrdinalIgnoreCase);
            var targetSchemaCols = new HashSet<string>(targetColsByName.Keys, StringComparer.OrdinalIgnoreCase);
            var sourceColNames = new HashSet<string>(sourceCols.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);

            var adcOnlyCols = targetColsWithDefault.Where(t => !sourceColNames.Contains(t.Schema.ColumnName)).ToList();
            var typeMismatchColNames = sourceCols
                .Where(sc => targetColsByName.TryGetValue(sc.ColumnName, out var tc) && !ColumnTypesEqual(sc, tc))
                .Select(sc => sc.ColumnName)
                .ToList();
            var stringLengthTruncateCols = sourceCols
                .Where(sc => targetColsByName.TryGetValue(sc.ColumnName, out var tc) && IsStringType(sc.DataType) && IsStringType(tc.DataType)
                    && GetEffectiveMaxLength(tc) > 0 && GetEffectiveMaxLength(tc) != -1
                    && (GetEffectiveMaxLength(sc) == -1 || GetEffectiveMaxLength(sc) > GetEffectiveMaxLength(tc)))
                .Select(sc => sc.ColumnName)
                .ToList();
            bool hasCommonStringCols = sourceCols.Any(sc => targetColsByName.TryGetValue(sc.ColumnName, out var tc) && IsStringType(sc.DataType) && IsStringType(tc.DataType));
            bool hasCommonBinaryCols = sourceCols.Any(sc => targetColsByName.TryGetValue(sc.ColumnName, out var tcb) && IsBinaryType(sc.DataType) && IsBinaryType(tcb.DataType));
            bool hasTargetBinaryCols = targetColsWithDefault.Any(t => IsBinaryType(t.Schema.DataType));

            if (adcOnlyCols.Count > 0)
                Log.Information("   -> ADC-only columns: {AdcOnlyCount} (will set defaults)", adcOnlyCols.Count);
            if (typeMismatchColNames.Count > 0)
                Log.Information("   -> Type-mismatch columns: {TypeMismatchCount} (will convert)", typeMismatchColNames.Count);
            if (stringLengthTruncateCols.Count > 0)
                Log.Information("   -> String length truncation: {TruncateCount} column(s) (target shorter than source)", stringLengthTruncateCols.Count);

            // Guard Contact hardcoded defaults: only append if columns are truly ADC-only (not already detected)
            bool needContactDefaults = sourceTableName.Equals("Contact", StringComparison.OrdinalIgnoreCase)
                && adcOnlyCols.Count == 0 && typeMismatchColNames.Count == 0
                && !sourceColNames.Contains("OldSAPID");

            string selectSql;
            if (adcOnlyCols.Count > 0 || typeMismatchColNames.Count > 0)
            {
                selectSql = BuildSelectSqlWithConversions(sourceTableName, sourceCols, targetColsByName, typeMismatchColNames, whereClause);
            }
            else
            {
                selectSql = "SELECT *";
                if (needContactDefaults)
                    selectSql += ", NULL as OldSAPID, 0 as PartTimeFlex, NULL as FobNumber, 0 as ExcludeOvertime, NULL as ExcludeOvertimeComment, 0 as VisaRestriction, NULL as VisaEndDate, NULL as MaximumHoursByFortnight, NULL as RosterProfile, 0 as ExcludeFromNotifications, 0 as MandatoryQualification";
                selectSql += $" FROM [{sourceTableName.Replace("]", "]]")}]{whereClause}";
            }

            using var cmd = new SqlCommand(selectSql, sourceConn);
            cmd.CommandTimeout = 600;
            if (sourceTenantId.HasValue && hasTenantId)
                cmd.Parameters.AddWithValue("@TenantId", sourceTenantId.Value);
            
            using var reader = await cmd.ExecuteReaderAsync();

            // Begin external transaction for per-table rollback safety (FR24, NFR6)
            using var transaction = targetConn.BeginTransaction();
            try
            {
            using var bulkCopy = new SqlBulkCopy(targetConn,
                hasIdentity ? SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.CheckConstraints : SqlBulkCopyOptions.CheckConstraints,
                transaction);

            bulkCopy.DestinationTableName = destTable;
            bulkCopy.BatchSize = _batchSize;
            bulkCopy.BulkCopyTimeout = 600;
            bulkCopy.NotifyAfter = 10000;
            long streamedRows = 0;
            bulkCopy.SqlRowsCopied += (sender, e) => { streamedRows = e.RowsCopied; if (e.RowsCopied % 50000 == 0) Log.Information("[DataSync] {DestTable}: {RowsCopied} rows copied...", destTable, e.RowsCopied); };

            bool transformTenantId = (sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId && hasTenantId);
            bool transformUsers = (userMapping != null && userMapping.Count > 0);
            bool useBufferPath = transformTenantId || transformUsers || adcOnlyCols.Count > 0 || typeMismatchColNames.Count > 0 || stringLengthTruncateCols.Count > 0 || hasCommonStringCols || hasCommonBinaryCols || hasTargetBinaryCols;

            long totalRows = 0;
            int totalTruncations = 0;

            if (useBufferPath)
            {
                if (transformTenantId) Log.Information("   -> Transforming TenantId: {SourceTenantId} -> {TargetTenantId}", sourceTenantId, targetTenantId);
                if (transformUsers) Log.Information("   -> Transforming User IDs (Audit Fields)");

                var dt = new DataTable();
                using (var schemaTable = reader.GetSchemaTable())
                {
                    if (schemaTable != null)
                    {
                        foreach (DataRow schemaRow in schemaTable.Rows)
                        {
                            var colName = (string)schemaRow["ColumnName"];
                            var dataType = (Type)schemaRow["DataType"];
                            var col = new DataColumn(colName, dataType);
                            if (dataType == typeof(string) && schemaRow["ColumnSize"] != DBNull.Value && schemaRow["ColumnSize"] is int size
                                && size > 0 && size != -1 && size != 2147483647)
                                col.MaxLength = size;
                            dt.Columns.Add(col);
                        }
                    }
                }

                foreach (var adcOnly in adcOnlyCols)
                {
                    var clrType = GetClrType(adcOnly.Schema);
                    var adcCol = new DataColumn(adcOnly.Schema.ColumnName, clrType);
                    if (clrType == typeof(string) && adcOnly.Schema.CharacterMaximumLength.HasValue)
                    {
                        var len = adcOnly.Schema.CharacterMaximumLength.Value;
                        if (len > 0 && len != -1 && len != 2147483647)
                            adcCol.MaxLength = len;
                    }
                    dt.Columns.Add(adcCol);
                }

                var computedColsBuffer = await GetComputedColumnNamesAsync(targetConn, destTable, transaction);
                foreach (var targetColInfo in targetColsWithDefault)
                {
                    var colName = targetColInfo.Schema.ColumnName;
                    if (!dt.Columns.Contains(colName) || computedColsBuffer.Contains(colName)) continue;
                    bulkCopy.ColumnMappings.Add(colName, colName);
                }
                foreach (DataColumn col in dt.Columns)
                {
                    if (col.DataType != typeof(string)) continue;
                    if (!targetColsByName.TryGetValue(col.ColumnName, out var targetCol) || !targetCol.CharacterMaximumLength.HasValue)
                        continue;
                    var maxLen = targetCol.CharacterMaximumLength.Value;
                    if (maxLen > 0 && maxLen != -1 && maxLen != 2147483647)
                        col.MaxLength = maxLen;
                }

                while (true)
                {
                    for (int i = 0; i < _batchSize && reader.Read(); i++)
                    {
                        var row = dt.NewRow();
                        for (int c = 0; c < reader.FieldCount; c++)
                            row[c] = reader.IsDBNull(c) ? DBNull.Value : reader.GetValue(c);
                        dt.Rows.Add(row);
                    }
                    if (dt.Rows.Count == 0)
                        break;

                    foreach (var adcOnly in adcOnlyCols)
                    {
                        var colName = adcOnly.Schema.ColumnName;
                        var defaultValue = GetDefaultForAdcOnlyColumn(colName, adcOnly);
                        foreach (DataRow row in dt.Rows)
                            row[colName] = defaultValue;
                    }

                    if (transformTenantId && dt.Columns.Contains("TenantId"))
                    {
                        foreach (DataRow row in dt.Rows)
                            row["TenantId"] = targetTenantId!.Value;
                    }

                    if (transformUsers)
                    {
                        string[] userCols = new[] { "CreatorUserId", "LastModifierUserId", "DeleterUserId", "CreatedBy", "ModifiedBy", "UserId" };
                        foreach (var colName in userCols)
                        {
                            if (!dt.Columns.Contains(colName)) continue;
                            foreach (DataRow row in dt.Rows)
                            {
                                if (row[colName] == DBNull.Value) continue;
                                try
                                {
                                    long oldId = Convert.ToInt64(row[colName]);
                                    if (userMapping!.TryGetValue(oldId, out long newId))
                                        row[colName] = newId;
                                }
                                catch (Exception ex) { Log.Debug("[Data] User ID mapping conversion failed for column {Column}: {Error}", colName, ex.Message); }
                            }
                        }
                    }

                    totalTruncations += TruncateStringRowsToColumnMaxLength(dt, destTable);
                    await bulkCopy.WriteToServerAsync(dt);
                    totalRows += dt.Rows.Count;
                    dt.Rows.Clear();
                }

                Log.Information("[Data] Completed {SourceTable} (Transformed {TotalRows} rows)", sourceTableName, totalRows);
                if (totalTruncations > 0)
                    Log.Information("[Truncation] {DestTable}: {TotalTruncations} values truncated", destTable, totalTruncations);
            }
            else
            {
                // Streaming Mode (No ID Transform)
                // Exclude computed columns from mapping — BulkCopy cannot write to them
                var computedCols = await GetComputedColumnNamesAsync(targetConn, destTable, transaction);
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string colName = reader.GetName(i);
                    if (targetSchemaCols.Contains(colName) && !computedCols.Contains(colName))
                        bulkCopy.ColumnMappings.Add(colName, colName);
                }

                await bulkCopy.WriteToServerAsync(reader);
                // streamedRows only updates every NotifyAfter (10000) rows — query source for accurate count
                reader.Close();
                var countSql = $"SELECT COUNT_BIG(*) FROM [{sourceTableName.Replace("]", "]]")}]{whereClause}";
                using var countCmd = new SqlCommand(countSql, sourceConn);
                countCmd.CommandTimeout = 120;
                if (sourceTenantId.HasValue && whereClause.Contains("@TenantId"))
                    countCmd.Parameters.AddWithValue("@TenantId", sourceTenantId.Value);
                totalRows = (long)(await countCmd.ExecuteScalarAsync() ?? 0L);
                Log.Information("[Data] Completed {SourceTable} | Rows: {TotalRows} (streaming BulkCopy)", sourceTableName, totalRows);
                // Post-copy truncation detection for streaming path: check target table for values at column max length
                if (totalRows > 0)
                    await DetectStagingTruncationsAsync(targetConn, destTable, destTable, transaction);
            }

            transaction.Commit();
            sw.Stop();
            TotalRowsMigrated += totalRows;
            var rowsPerSec = sw.Elapsed.TotalSeconds > 0 ? (long)(totalRows / sw.Elapsed.TotalSeconds) : totalRows;
            Log.Information("[DataSync] Committed {DestTable} — {TotalRows} rows in {ElapsedMs}ms ({RowsPerSec} rows/sec)", destTable, totalRows, sw.ElapsedMilliseconds, rowsPerSec);
            ReportWriter.AddDataSyncTable(sourceTableName, destTable, totalRows, sw.ElapsedMilliseconds, "Tenant-filtered", null);
            }
            catch (Exception ex)
            {
                try { transaction.Rollback(); } catch { /* rollback can fail if connection dropped */ }
                Log.Warning("[DataSync] Rolled back {DestTable}: {ErrorMessage}", destTable, ex.Message);
                throw; // Re-throw so caller skips checkpoint and continues to next table
            }
            }
            finally
            {
                if (ownSource) (sourceConn as IDisposable)?.Dispose();
                if (ownTarget) (targetConn as IDisposable)?.Dispose();
            }
        }

        /// <summary>
        /// For tables with natural PK (e.g. nvarchar): copy source to staging, then INSERT into target only rows where PK does not already exist (insert missing only). Avoids duplicate key.
        /// </summary>
        public async Task MigrateTableNaturalPkAsync(
            SqlConnection sourceConn,
            SqlConnection targetConn,
            string sourceTableName,
            string targetTableName,
            PkColumnInfo pkInfo,
            int? sourceTenantId,
            int? targetTenantId,
            Dictionary<long, long>? userMapping)
        {
            Log.Information("[Data] Migrating {SourceTable} -> {TargetTable}... (Natural PK, insert missing only)", sourceTableName, targetTableName);
            var sw = Stopwatch.StartNew();
            var stagingName = targetTableName + "_staging";

            // Begin external transaction for per-table rollback safety (FR24, NFR6)
            using var transaction = targetConn.BeginTransaction();
            try
            {
            await CreateStagingTableForCompositeAsync(sourceConn, targetConn, sourceTableName, targetTableName, transaction);

            bool hasTenantId = await HasTenantIdColumnAsync(sourceConn, sourceTableName);
            string whereClause = (sourceTenantId.HasValue && hasTenantId) ? " WHERE TenantId = @TenantId" : "";
            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var sourceColNames = new HashSet<string>(sourceCols.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);
            var selectParts = sourceCols.Select(c => $"[{c.ColumnName.Replace("]", "]]")}]").ToList();
            var selectSql = "SELECT " + string.Join(", ", selectParts) + $" FROM [{sourceTableName.Replace("]", "]]")}]{whereClause}";

            using (var selectCmd = new SqlCommand(selectSql, sourceConn))
            {
                selectCmd.CommandTimeout = 600;
                if (sourceTenantId.HasValue && hasTenantId)
                    selectCmd.Parameters.AddWithValue("@TenantId", sourceTenantId.Value);
                using var reader = await selectCmd.ExecuteReaderAsync();
                using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.CheckConstraints, transaction);
                bulkCopy.DestinationTableName = stagingName;
                bulkCopy.BatchSize = _batchSize;
                bulkCopy.BulkCopyTimeout = 600;
                bool transformTenantId = sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId && hasTenantId;
                bool transformUsers = userMapping != null && userMapping.Count > 0;

                if (transformTenantId || transformUsers)
                {
                    var dt = new DataTable();
                    using (var schemaTable = reader.GetSchemaTable())
                    {
                        if (schemaTable != null)
                            foreach (DataRow schemaRow in schemaTable.Rows)
                            {
                                var colName2 = (string)schemaRow["ColumnName"];
                                var dataType = (Type)schemaRow["DataType"];
                                var col = new DataColumn(colName2, dataType);
                                if (dataType == typeof(string) && schemaRow["ColumnSize"] != DBNull.Value && schemaRow["ColumnSize"] is int sz
                                    && sz > 0 && sz != -1 && sz != 2147483647)
                                    col.MaxLength = sz;
                                dt.Columns.Add(col);
                            }
                    }
                    // Override MaxLength from target column schema for truncation detection
                    var targetColsForTrunc = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
                    var targetColByName = targetColsForTrunc.ToDictionary(t => t.Schema.ColumnName, t => t.Schema, StringComparer.OrdinalIgnoreCase);
                    foreach (DataColumn col in dt.Columns)
                    {
                        if (col.DataType != typeof(string)) continue;
                        if (!targetColByName.TryGetValue(col.ColumnName, out var tc) || !tc.CharacterMaximumLength.HasValue) continue;
                        var maxLen = tc.CharacterMaximumLength.Value;
                        if (maxLen > 0 && maxLen != -1 && maxLen != 2147483647)
                            col.MaxLength = maxLen;
                    }
                    foreach (DataColumn col in dt.Columns)
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    int totalTruncations = 0;
                    while (true)
                    {
                        for (int i = 0; i < _batchSize && reader.Read(); i++)
                        {
                            var row = dt.NewRow();
                            for (int c = 0; c < reader.FieldCount; c++)
                                row[c] = reader.IsDBNull(c) ? DBNull.Value : reader.GetValue(c);
                            dt.Rows.Add(row);
                        }
                        if (dt.Rows.Count == 0) break;
                        if (transformTenantId && dt.Columns.Contains("TenantId"))
                            foreach (DataRow row in dt.Rows) row["TenantId"] = targetTenantId!.Value;
                        if (transformUsers)
                            foreach (var colName2 in new[] { "CreatorUserId", "LastModifierUserId", "DeleterUserId", "CreatedBy", "ModifiedBy", "UserId" })
                                if (dt.Columns.Contains(colName2))
                                    foreach (DataRow row in dt.Rows)
                                    {
                                        if (row[colName2] == DBNull.Value) continue;
                                        try
                                        {
                                            if (userMapping!.TryGetValue(Convert.ToInt64(row[colName2]), out long newId))
                                                row[colName2] = newId;
                                        }
                                        catch (Exception ex) { Log.Debug("[Data] User ID mapping conversion failed for column {Column}: {Error}", colName2, ex.Message); }
                                    }
                        totalTruncations += TruncateStringRowsToColumnMaxLength(dt, targetTableName);
                        await bulkCopy.WriteToServerAsync(dt);
                        dt.Rows.Clear();
                    }
                    if (totalTruncations > 0)
                        Log.Information("[Truncation] {TargetTable}: {TotalTruncations} values truncated (NaturalPk buffered path)", targetTableName, totalTruncations);
                }
                else
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                        bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                    await bulkCopy.WriteToServerAsync(reader);
                }
            }

            // Detect potential truncations in staging table
            await DetectStagingTruncationsAsync(targetConn, stagingName, targetTableName, transaction);

            var targetColsWithDefault = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
            var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName, transaction);
            var pkColEsc = pkInfo.ColumnName.Replace("]", "]]");
            var targetEsc = targetTableName.Replace("]", "]]");
            var stagingEsc = stagingName.Replace("]", "]]");

            var insertCols = new List<string>();
            var selectExprs = new List<string>();
            foreach (var tc in targetColsWithDefault)
            {
                if (computedCols.Contains(tc.Schema.ColumnName)) continue;
                var colEsc = tc.Schema.ColumnName.Replace("]", "]]");
                insertCols.Add($"[{colEsc}]");
                selectExprs.Add(sourceColNames.Contains(tc.Schema.ColumnName) ? $"s.[{colEsc}]" : GetDefaultSqlForAdcOnlyColumn(tc.Schema.ColumnName, tc));
            }

            var insertSql = $@"
INSERT INTO [dbo].[{targetEsc}] ({string.Join(", ", insertCols)})
SELECT {string.Join(", ", selectExprs)}
FROM [dbo].[{stagingEsc}] s
WHERE NOT EXISTS (SELECT 1 FROM [dbo].[{targetEsc}] t WHERE t.[{pkColEsc}] = s.[{pkColEsc}])";
            var inserted = await targetConn.ExecuteAsync(insertSql, commandTimeout: 600, transaction: transaction);
            await DropStagingTableIfExistsAsync(targetConn, stagingName, transaction);

            transaction.Commit();
            Log.Information("   -> Natural PK: inserted {Inserted} missing row(s) into [dbo].[{TargetTable}] (skipped existing).", inserted, targetTableName);
            sw.Stop();
            TotalRowsMigrated += inserted;
            var rowsPerSec = sw.Elapsed.TotalSeconds > 0 ? (long)(inserted / sw.Elapsed.TotalSeconds) : inserted;
            Log.Information("[DataSync] Committed {TargetTable} — {Inserted} rows in {ElapsedMs}ms ({RowsPerSec} rows/sec)", targetTableName, inserted, sw.ElapsedMilliseconds, rowsPerSec);
            ReportWriter.AddDataSyncTable(sourceTableName, targetTableName, inserted, sw.ElapsedMilliseconds, "Natural-PK", null);
            }
            catch (Exception ex)
            {
                try { transaction.Rollback(); } catch { }
                Log.Warning("[DataSync] Rolled back {TargetTable}: {ErrorMessage}", targetTableName, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Creates staging table on target: OldId (PK type) + all source columns except PK. Drops existing if present.
        /// </summary>
        public async Task CreateStagingTableAsync(SqlConnection sourceConn, SqlConnection targetConn, string sourceTableName, string targetTableName, PkColumnInfo pkInfo, SqlTransaction? transaction = null)
        {
            var stagingName = targetTableName + "_staging";
            await DropStagingTableIfExistsAsync(targetConn, stagingName, transaction);

            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var pkColName = pkInfo.ColumnName;
            var nonPkCols = sourceCols.Where(c => !string.Equals(c.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();

            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"CREATE TABLE [dbo].[{stagingName.Replace("]", "]]")}] (");
            sb.AppendLine($"  [OldId] {GetPkSqlType(pkInfo.DataType)} NOT NULL,");
            foreach (var c in nonPkCols)
            {
                var nullable = " NULL";
                sb.AppendLine($"  [{c.ColumnName.Replace("]", "]]")}] {GetSqlTypeString(c)}{nullable},");
            }
            sb.Length -= Environment.NewLine.Length + 1; // remove last comma + newline
            sb.AppendLine();
            sb.AppendLine(");");

            using var cmd = new SqlCommand(sb.ToString(), targetConn);
            if (transaction != null) cmd.Transaction = transaction;
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();
            Log.Information("   -> Created staging table [dbo].[{StagingName}]", stagingName);
        }

        private static string GetPkSqlType(string dataType)
        {
            return dataType?.ToLowerInvariant() switch
            {
                "int" => "INT",
                "bigint" => "BIGINT",
                "uniqueidentifier" => "UNIQUEIDENTIFIER",
                _ => "BIGINT"
            };
        }

        private static async Task DropStagingTableIfExistsAsync(SqlConnection conn, string stagingName, SqlTransaction? transaction = null)
        {
            var objName = "dbo." + stagingName;
            await conn.ExecuteAsync("IF OBJECT_ID(@Name, 'U') IS NOT NULL DROP TABLE [dbo].[" + stagingName.Replace("]", "]]") + "]", new { Name = objName }, transaction: transaction);
        }

        /// <summary>
        /// Creates staging table with same structure as source (all columns). For composite-key table migration.
        /// </summary>
        public async Task CreateStagingTableForCompositeAsync(SqlConnection sourceConn, SqlConnection targetConn, string sourceTableName, string targetTableName, SqlTransaction? transaction = null)
        {
            var stagingName = targetTableName + "_staging";
            await DropStagingTableIfExistsAsync(targetConn, stagingName, transaction);
            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"CREATE TABLE [dbo].[{stagingName.Replace("]", "]]")}] (");
            foreach (var c in sourceCols)
            {
                var nullable = " NULL";
                sb.AppendLine($"  [{c.ColumnName.Replace("]", "]]")}] {GetSqlTypeString(c)}{nullable},");
            }
            sb.Length -= Environment.NewLine.Length + 1; // remove last comma + newline
            sb.AppendLine();
            sb.AppendLine(");");
            using var cmd = new SqlCommand(sb.ToString(), targetConn);
            if (transaction != null) cmd.Transaction = transaction;
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();
            Log.Information("   -> Created staging table [dbo].[{StagingName}] (composite)", stagingName);
        }

        /// <summary>
        /// Migrates a table with composite PK: BulkCopy to staging, then INSERT into target with IdMapping JOIN and NOT EXISTS (scope by Tenant).
        /// </summary>
        public async Task MigrateCompositeKeyTableAsync(
            SqlConnection sourceConn,
            SqlConnection targetConn,
            string sourceTableName,
            string targetTableName,
            List<string> pkColumnNames,
            List<FkColumnInfo> fkColumns,
            int? sourceTenantId,
            int? targetTenantId,
            Dictionary<long, long>? userMapping = null)
        {
            // Begin external transaction for per-table rollback safety (FR24, NFR6)
            using var transaction = targetConn.BeginTransaction();
            try
            {
            var sw = Stopwatch.StartNew();
            bool hasTenantId = await TableHasTenantIdColumnAsync(sourceConn, sourceTableName);
            string whereClause = "";
            if (sourceTenantId.HasValue && hasTenantId)
                whereClause = " WHERE TenantId = @TenantId";

            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var sourceColNames = new HashSet<string>(sourceCols.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);

            var stagingName = targetTableName + "_staging";
            await CreateStagingTableForCompositeAsync(sourceConn, targetConn, sourceTableName, targetTableName, transaction);

            var selectSql = "SELECT * FROM [" + sourceTableName.Replace("]", "]]") + "]" + whereClause;
            using (var selectCmd = new SqlCommand(selectSql, sourceConn))
            {
                selectCmd.CommandTimeout = 600;
                if (sourceTenantId.HasValue && hasTenantId)
                    selectCmd.Parameters.AddWithValue("@TenantId", sourceTenantId.Value);
                using var reader = await selectCmd.ExecuteReaderAsync();
                using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.CheckConstraints, transaction);
                bulkCopy.DestinationTableName = stagingName;
                bulkCopy.BatchSize = _batchSize;
                bulkCopy.BulkCopyTimeout = 600;
                for (int i = 0; i < reader.FieldCount; i++)
                    bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                await bulkCopy.WriteToServerAsync(reader);
            }

            // Detect potential truncations in staging table (streaming path has no in-memory check)
            await DetectStagingTruncationsAsync(targetConn, stagingName, targetTableName, transaction);

            bool transformTenantId = sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId && hasTenantId;
            if (transformTenantId)
            {
                await targetConn.ExecuteAsync(
                    $"UPDATE [dbo].[{stagingName.Replace("]", "]]")}] SET TenantId = @TenantId",
                    new { TenantId = targetTenantId!.Value }, transaction: transaction);
            }

            // Remap user audit columns in staging table via userMapping
            if (userMapping != null && userMapping.Count > 0)
            {
                var stagingEsc = stagingName.Replace("]", "]]");
                string[] userCols = new[] { "CreatorUserId", "LastModifierUserId", "DeleterUserId", "CreatedBy", "ModifiedBy", "UserId" };
                foreach (var colName in userCols)
                {
                    if (!sourceColNames.Contains(colName))
                        continue;
                    var colEsc = colName.Replace("]", "]]");
                    // Use a temp table to hold the mapping for efficient batch UPDATE
                    // For smaller mappings, individual UPDATEs per key are acceptable;
                    // use a JOIN-based approach against a VALUES list for larger mappings
                    foreach (var kvp in userMapping)
                    {
                        await targetConn.ExecuteAsync(
                            $"UPDATE [dbo].[{stagingEsc}] SET [{colEsc}] = @NewId WHERE [{colEsc}] = @OldId",
                            new { OldId = kvp.Key, NewId = kvp.Value },
                            transaction: transaction, commandTimeout: 120);
                    }
                }
            }

            var targetCols = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
            var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName, transaction);
            var fkByChildCol = fkColumns.ToDictionary(f => f.ChildColumn, f => f, StringComparer.OrdinalIgnoreCase);
            var fkColumnToAlias = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var usedFkForParams = new List<(string RefTable, string RefCol)>();
            var selectParts = new List<string>();
            var joinClauses = new List<string>();
            string mapAlias = "s";
            int fkIndex = 0;
            foreach (var tc in targetCols)
            {
                if (computedCols.Contains(tc.Schema.ColumnName))
                    continue; // computed column: DB tự tính, không đưa vào INSERT
                var colEsc = tc.Schema.ColumnName.Replace("]", "]]");
                if (!sourceColNames.Contains(tc.Schema.ColumnName))
                {
                    selectParts.Add(GetDefaultSqlForAdcOnlyColumn(tc.Schema.ColumnName, tc) + " AS [" + colEsc + "]");
                    continue;
                }
                if (string.Equals(tc.Schema.ColumnName, "TenantId", StringComparison.OrdinalIgnoreCase) && transformTenantId)
                    selectParts.Add("@TenantId AS [" + colEsc + "]");
                else if (fkByChildCol.TryGetValue(tc.Schema.ColumnName, out var fk))
                {
                    var mappingTable = fk.DataType switch
                    {
                        "int" => "IdMappingInt",
                        "bigint" => "IdMappingBigInt",
                        "uniqueidentifier" => "IdMappingGuid",
                        _ => null
                    };
                    if (mappingTable == null) { selectParts.Add($"{mapAlias}.[{colEsc}]"); continue; }
                    fkIndex++;
                    var alias = "m" + fkIndex;
                    fkColumnToAlias[tc.Schema.ColumnName] = alias;
                    usedFkForParams.Add((fk.ReferencedTable, fk.ReferencedColumn));
                    var tenantJoinFilter = targetTenantId.HasValue
                        ? $" AND {alias}.TenantId = @TenantId"
                        : $" AND {alias}.TenantId IS NULL";
                    joinClauses.Add($" LEFT JOIN [dbo].[{mappingTable}] {alias} ON {alias}.TableName = @RefTable_{fkIndex} AND {alias}.ColumnName = @RefCol_{fkIndex} AND {alias}.OldId = {mapAlias}.[{colEsc}]{tenantJoinFilter}");
                    selectParts.Add($"COALESCE({alias}.NewId, {mapAlias}.[{colEsc}]) AS [{colEsc}]");
                }
                else
                    selectParts.Add($"{mapAlias}.[{colEsc}]");
            }

            var notExistsParts = new List<string>();
            foreach (var pk in pkColumnNames)
            {
                var colEsc = pk.Replace("]", "]]");
                if (fkColumnToAlias.TryGetValue(pk, out var alias))
                    notExistsParts.Add($"t.[{colEsc}] = COALESCE({alias}.NewId, {mapAlias}.[{colEsc}])");
                else
                    notExistsParts.Add($"t.[{colEsc}] = {mapAlias}.[{colEsc}]");
            }

            var insertCols = string.Join(", ", targetCols.Where(t => !computedCols.Contains(t.Schema.ColumnName)).Select(t => "[" + t.Schema.ColumnName.Replace("]", "]]") + "]"));
            var joinSql = string.Join("", joinClauses);
            var notExistsSql = notExistsParts.Count > 0 ? $" WHERE NOT EXISTS (SELECT 1 FROM [dbo].[{targetTableName.Replace("]", "]]")}] t WHERE " + string.Join(" AND ", notExistsParts) + ")" : "";
            var insertSql = $@"INSERT INTO [dbo].[{targetTableName.Replace("]", "]]")}] ({insertCols})
SELECT {string.Join(", ", selectParts)}
FROM [dbo].[{stagingName.Replace("]", "]]")}] {mapAlias}
{joinSql}
{notExistsSql}";

            var prm = new DynamicParameters();
            prm.Add("TenantId", targetTenantId);
            for (int i = 0; i < usedFkForParams.Count; i++)
            {
                prm.Add("RefTable_" + (i + 1), usedFkForParams[i].RefTable);
                prm.Add("RefCol_" + (i + 1), usedFkForParams[i].RefCol);
            }
            var inserted = await targetConn.ExecuteAsync(insertSql, prm, transaction: transaction);
            await DropStagingTableIfExistsAsync(targetConn, stagingName, transaction);

            transaction.Commit();
            Log.Information("   -> Composite key: inserted {Inserted} row(s) into [dbo].[{TargetTable}] (skipped existing).", inserted, targetTableName);
            sw.Stop();
            TotalRowsMigrated += inserted;
            var rowsPerSec = sw.Elapsed.TotalSeconds > 0 ? (long)(inserted / sw.Elapsed.TotalSeconds) : inserted;
            Log.Information("[DataSync] Committed {TargetTable} — {Inserted} rows in {ElapsedMs}ms ({RowsPerSec} rows/sec)", targetTableName, inserted, sw.ElapsedMilliseconds, rowsPerSec);
            ReportWriter.AddDataSyncTable(sourceTableName, targetTableName, inserted, sw.ElapsedMilliseconds, "Composite-PK", null);
            }
            catch (Exception ex)
            {
                try { transaction.Rollback(); } catch { }
                Log.Warning("[DataSync] Rolled back {TargetTable}: {ErrorMessage}", targetTableName, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Copies source data to staging (with TenantId/User transform), then MERGE from staging to target with OUTPUT, writes IdMapping, drops staging.
        /// Target must have single-column PK (int/bigint/uniqueidentifier) and identity. ADC-only columns get default SQL in the MERGE.
        /// </summary>
        public async Task InsertTableWithIdMappingAsync(
            SqlConnection sourceConn,
            SqlConnection targetConn,
            string sourceTableName,
            string targetTableName,
            PkColumnInfo pkInfo,
            string migrationBatch,
            int? tenantId,
            string whereClause,
            int? sourceTenantId,
            int? targetTenantId,
            Dictionary<long, long>? userMapping,
            int mergeChunkSize = 0,
            int? commandTimeoutOverride = null)
        {
            int cmdTimeout = commandTimeoutOverride ?? 600;
            var stagingName = targetTableName + "_staging";

            // Begin external transaction for per-table rollback safety (FR24, NFR6)
            // Staging table creation is inside the transaction so it's rolled back on failure (Story 14.3)
            using var transaction = targetConn.BeginTransaction();
            try
            {
            await CreateStagingTableAsync(sourceConn, targetConn, sourceTableName, targetTableName, pkInfo, transaction);
            var sw = Stopwatch.StartNew();
            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var pkColName = pkInfo.ColumnName;
            var nonPkCols = sourceCols.Where(c => !string.Equals(c.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();

            // Build SELECT: Id AS OldId, col1, col2, ... FROM source WHERE ...
            var selectParts = new List<string> { $"[{pkColName}] AS OldId" };
            foreach (var c in nonPkCols)
                selectParts.Add($"[{c.ColumnName}]");
            var selectSql = "SELECT " + string.Join(", ", selectParts) + $" FROM [{sourceTableName.Replace("]", "]]")}]{whereClause}";

            using var selectCmd = new SqlCommand(selectSql, sourceConn);
            selectCmd.CommandTimeout = cmdTimeout;
            if (sourceTenantId.HasValue && whereClause.Contains("@TenantId"))
                selectCmd.Parameters.AddWithValue("@TenantId", sourceTenantId.Value);
            using var reader = await selectCmd.ExecuteReaderAsync();

            using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.CheckConstraints, transaction);
            bulkCopy.DestinationTableName = stagingName;
            bulkCopy.BatchSize = _batchSize;
            // For large-table runs (commandTimeoutOverride set), use 0 = no timeout so bulk copy is not killed
            bulkCopy.BulkCopyTimeout = commandTimeoutOverride.HasValue ? 0 : cmdTimeout;
            bulkCopy.NotifyAfter = 10000;
            bulkCopy.SqlRowsCopied += (_, e) => { if (e.RowsCopied % 50000 == 0) Log.Information("[DataSync] {TargetTableName}: {RowsCopied} rows staged...", targetTableName, e.RowsCopied); };

            bool hasTenantId = nonPkCols.Any(c => string.Equals(c.ColumnName, "TenantId", StringComparison.OrdinalIgnoreCase));
            bool transformTenantId = sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId && hasTenantId;
            bool transformUsers = userMapping != null && userMapping.Count > 0;

            if (transformTenantId || transformUsers)
            {
                var dt = new DataTable();
                using (var schemaTable = reader.GetSchemaTable())
                {
                    if (schemaTable != null)
                    {
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            var colName = (string)row["ColumnName"];
                            var dataType = (Type)row["DataType"];
                            var col = new DataColumn(colName, dataType);
                            if (dataType == typeof(string) && row["ColumnSize"] != DBNull.Value && row["ColumnSize"] is int size
                                && size > 0 && size != -1 && size != 2147483647)
                                col.MaxLength = size;
                            dt.Columns.Add(col);
                        }
                    }
                }
                var stagingColsOrder = await GetTargetColumnsWithDefaultsAsync(targetConn, stagingName, transaction);
                foreach (var t in stagingColsOrder)
                {
                    if (dt.Columns.Contains(t.Schema.ColumnName))
                        bulkCopy.ColumnMappings.Add(t.Schema.ColumnName, t.Schema.ColumnName);
                }
                // Override MaxLength from target table schema for truncation detection (staging has source-sized columns)
                var targetColsForIdMap = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
                var targetColByNameIdMap = targetColsForIdMap.ToDictionary(t => t.Schema.ColumnName, t => t.Schema, StringComparer.OrdinalIgnoreCase);
                foreach (DataColumn col in dt.Columns)
                {
                    if (col.DataType != typeof(string)) continue;
                    if (!targetColByNameIdMap.TryGetValue(col.ColumnName, out var tc) || !tc.CharacterMaximumLength.HasValue) continue;
                    var maxLen = tc.CharacterMaximumLength.Value;
                    if (maxLen > 0 && maxLen != -1 && maxLen != 2147483647)
                        col.MaxLength = maxLen;
                }

                int totalTruncations = 0;
                while (true)
                {
                    for (int i = 0; i < _batchSize && reader.Read(); i++)
                    {
                        var dataRow = dt.NewRow();
                        for (int c = 0; c < reader.FieldCount; c++)
                            dataRow[c] = reader.IsDBNull(c) ? DBNull.Value : reader.GetValue(c);
                        dt.Rows.Add(dataRow);
                    }
                    if (dt.Rows.Count == 0) break;
                    if (transformTenantId && dt.Columns.Contains("TenantId"))
                        foreach (DataRow row in dt.Rows) row["TenantId"] = targetTenantId!.Value;
                    if (transformUsers)
                    {
                        foreach (var colName in new[] { "CreatorUserId", "LastModifierUserId", "DeleterUserId", "CreatedBy", "ModifiedBy", "UserId" })
                        {
                            if (!dt.Columns.Contains(colName)) continue;
                            foreach (DataRow row in dt.Rows)
                            {
                                if (row[colName] == DBNull.Value) continue;
                                try
                                {
                                    if (userMapping!.TryGetValue(Convert.ToInt64(row[colName]), out long newId))
                                        row[colName] = newId;
                                }
                                catch (Exception ex) { Log.Debug("[Data] User ID mapping conversion failed for column {Column}: {Error}", colName, ex.Message); }
                            }
                        }
                    }
                    totalTruncations += TruncateStringRowsToColumnMaxLength(dt, targetTableName);
                    await bulkCopy.WriteToServerAsync(dt);
                    dt.Rows.Clear();
                }
                if (totalTruncations > 0)
                    Log.Information("[Truncation] {TargetTable}: {TotalTruncations} values truncated (IdMapping buffered path)", targetTableName, totalTruncations);
            }
            else
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                await bulkCopy.WriteToServerAsync(reader);
                // Detect potential truncations in staging table (streaming path has no in-memory check)
                await DetectStagingTruncationsAsync(targetConn, stagingName, targetTableName, transaction);
            }

            Log.Information("");

            // Target columns and ADC-only defaults for MERGE
            var targetColsWithDefault = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
            var stagingColNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "OldId" };
            foreach (var c in nonPkCols) stagingColNames.Add(c.ColumnName);
            var adcOnlyCols = targetColsWithDefault.Where(t => !stagingColNames.Contains(t.Schema.ColumnName) && !string.Equals(t.Schema.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();
            var targetColsExceptPk = targetColsWithDefault.Where(t => !string.Equals(t.Schema.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();
            var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName, transaction);

            bool hasIdentity = await HasIdentityColumnAsync(targetConn, targetTableName, transaction);
            bool pkIsGuid = string.Equals(pkInfo.DataType, "uniqueidentifier", StringComparison.OrdinalIgnoreCase);

            var insertCols = new List<string>();
            var valueExprs = new List<string>();
            // Bảng không có identity và PK kiểu uniqueidentifier: đưa PK vào INSERT và gán NEWID()
            if (!hasIdentity && pkIsGuid)
            {
                insertCols.Add($"[{pkColName}]");
                valueExprs.Add("NEWID()");
            }
            foreach (var t in targetColsExceptPk)
            {
                if (computedCols.Contains(t.Schema.ColumnName))
                    continue; // computed column: DB tự tính, không đưa vào INSERT
                insertCols.Add($"[{t.Schema.ColumnName}]");
                if (stagingColNames.Contains(t.Schema.ColumnName))
                    valueExprs.Add($"s.[{t.Schema.ColumnName}]");
                else
                    valueExprs.Add(GetDefaultSqlForAdcOnlyColumn(t.Schema.ColumnName, t)); // use t (target column info with IsNullable) so NOT NULL gets type-based default
            }

            // OUTPUT INTO table variable: bảng đích có trigger thì không được dùng OUTPUT trả về client
            var mappingTable = pkInfo.DataType?.ToLowerInvariant() switch
            {
                "int" => "IdMappingInt",
                "bigint" => "IdMappingBigInt",
                "uniqueidentifier" => "IdMappingGuid",
                _ => null
            };
            var pkSqlType = string.Equals(pkInfo.DataType, "uniqueidentifier", StringComparison.OrdinalIgnoreCase) ? "uniqueidentifier" : (string.Equals(pkInfo.DataType, "bigint", StringComparison.OrdinalIgnoreCase) ? "bigint" : "int");
            var stagingEsc = stagingName.Replace("]", "]]");
            var targetEsc = targetTableName.Replace("]", "]]");

            int totalInserted = 0;
            if (mergeChunkSize > 0 && mappingTable != null)
            {
                // MERGE theo chunk: mỗi chunk SELECT INTO #Chunk, MERGE, INSERT IdMapping, DELETE staging; giảm memory và timeout cho bảng lớn
                var mappingTableEsc = mappingTable.Replace("]", "]]");
                int totalMapped = 0;
                int iteration = 0;
                var chunkSql = $@"
IF OBJECT_ID('tempdb..#Chunk') IS NOT NULL DROP TABLE #Chunk;
SELECT TOP (@ChunkSize) * INTO #Chunk FROM [dbo].[{stagingEsc}] ORDER BY OldId;
DECLARE @Mapping TABLE (OldId {pkSqlType}, NewId {pkSqlType});
MERGE [dbo].[{targetEsc}] AS t
USING #Chunk AS s ON 1=0
WHEN NOT MATCHED THEN
  INSERT ({string.Join(", ", insertCols)})
  VALUES ({string.Join(", ", valueExprs)})
OUTPUT s.OldId, inserted.[{pkColName}] INTO @Mapping(OldId, NewId);
INSERT INTO [dbo].[{mappingTableEsc}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)
SELECT @TableName, @ColumnName, OldId, NewId, @MigrationBatch, @TenantId FROM @Mapping;
DELETE s FROM [dbo].[{stagingEsc}] s INNER JOIN #Chunk c ON s.OldId = c.OldId;
SELECT COUNT(*) FROM @Mapping;";
                var chunkPrm = new { ChunkSize = mergeChunkSize, TableName = targetTableName, ColumnName = pkColName, MigrationBatch = migrationBatch, TenantId = (int?)tenantId };
                while (true)
                {
                    var rowsInChunk = await targetConn.ExecuteScalarAsync<int>(chunkSql, chunkPrm, commandTimeout: cmdTimeout, transaction: transaction);
                    if (rowsInChunk == 0) break;
                    totalMapped += rowsInChunk;
                    iteration++;
                    if (iteration % 10 == 0)
                        Log.Information("[DataSync] {TargetTable}: {TotalMapped} rows merged ({Iteration} chunks)...", targetTableName, totalMapped, iteration);
                }
                if (totalMapped > 0)
                {
                    Log.Information("   -> Inserted {TotalMapped} row(s) into [dbo].[{TargetTable}] (chunked MERGE)", totalMapped, targetTableName);
                    Log.Information("   -> IdMapping (chunked): {TotalMapped} row(s) -> [dbo].[{MappingTable}]", totalMapped, mappingTable);
                }
                totalInserted = totalMapped;
            }
            else
            {
                var mergeSql = $@"
DECLARE @Mapping TABLE (OldId {pkSqlType}, NewId {pkSqlType});
MERGE [dbo].[{targetEsc}] AS t
USING [dbo].[{stagingEsc}] AS s ON 1=0
WHEN NOT MATCHED THEN
  INSERT ({string.Join(", ", insertCols)})
  VALUES ({string.Join(", ", valueExprs)})
OUTPUT s.OldId, inserted.[{pkColName}] INTO @Mapping(OldId, NewId);";
                mergeSql += "\nSELECT COUNT(*) FROM @Mapping;";
                if (mappingTable != null)
                {
                    var mappingTableEsc = mappingTable.Replace("]", "]]");
                    // Insert into IdMapping before the SELECT COUNT so we capture the count after all mutations
                    mergeSql = mergeSql.Replace("SELECT COUNT(*) FROM @Mapping;",
                        $@"INSERT INTO [dbo].[{mappingTableEsc}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)
SELECT @TableName, @ColumnName, OldId, NewId, @MigrationBatch, @TenantId FROM @Mapping;
SELECT COUNT(*) FROM @Mapping;");
                }
                var prm = new { TableName = targetTableName, ColumnName = pkColName, MigrationBatch = migrationBatch, TenantId = (int?)tenantId };
                var insertedCount = await targetConn.ExecuteScalarAsync<int>(mergeSql, prm, commandTimeout: cmdTimeout, transaction: transaction);
                if (insertedCount > 0)
                {
                    Log.Information("   -> Inserted {InsertedCount} row(s) into [dbo].[{TargetTable}]", insertedCount, targetTableName);
                    if (mappingTable != null)
                        Log.Information("   -> IdMapping (bulk): {InsertedCount} row(s) -> [dbo].[{MappingTable}]", insertedCount, mappingTable);
                }
                totalInserted = insertedCount;
            }

            await DropStagingTableIfExistsAsync(targetConn, stagingName, transaction);
            Log.Information("   -> Dropped [dbo].[{StagingName}]", stagingName);

            transaction.Commit();
            sw.Stop();
            TotalRowsMigrated += totalInserted;
            var rowsPerSec = sw.Elapsed.TotalSeconds > 0 ? (long)(totalInserted / sw.Elapsed.TotalSeconds) : totalInserted;
            Log.Information("[DataSync] Committed {TargetTable} — {TotalInserted} rows in {ElapsedMs}ms ({RowsPerSec} rows/sec)", targetTableName, totalInserted, sw.ElapsedMilliseconds, rowsPerSec);
            ReportWriter.AddDataSyncTable(sourceTableName, targetTableName, totalInserted, sw.ElapsedMilliseconds, "IdMapping", null);
            }
            catch (Exception ex)
            {
                try { transaction.Rollback(); } catch { }
                Log.Warning("[DataSync] Rolled back {TargetTable}: {ErrorMessage}", targetTableName, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// MERGE upsert for global tables (no TenantId). Inserts new rows and updates existing MDC-originated rows.
        /// ADC-native rows are never modified. Tracks inserted rows in IdMapping with TenantId = NULL.
        /// </summary>
        public async Task MergeGlobalTableAsync(
            SqlConnection sourceConn,
            SqlConnection targetConn,
            string sourceTableName,
            string targetTableName,
            string matchKeyColumn,
            string migrationBatch,
            Dictionary<long, long>? userMapping = null,
            int? commandTimeoutOverride = null)
        {
            int cmdTimeout = commandTimeoutOverride ?? 600;
            var sw = Stopwatch.StartNew();
            var stagingName = targetTableName + "_staging";

            // Pre-transaction: get PK info (Dapper queries require explicit transaction enrollment)
            var pkInfo = await GetPkColumnInfoAsync(targetConn, targetTableName);
            if (pkInfo == null)
            {
                Log.Error("[DataSync] No PK found for global table {TargetTable}. Cannot MERGE.", targetTableName);
                return;
            }

            using var transaction = targetConn.BeginTransaction();
            try
            {
                // 1. Create staging table with all source columns
                await CreateStagingTableForCompositeAsync(sourceConn, targetConn, sourceTableName, targetTableName, transaction);

                // 2. BulkCopy all source rows into staging (no TenantId filter)
                var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
                var sourceColNames = new HashSet<string>(sourceCols.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);
                var selectParts = sourceCols.Select(c => $"[{c.ColumnName.Replace("]", "]]")}]").ToList();
                var selectSql = "SELECT " + string.Join(", ", selectParts) + $" FROM [{sourceTableName.Replace("]", "]]")}]";

                using (var selectCmd = new SqlCommand(selectSql, sourceConn))
                {
                    selectCmd.CommandTimeout = cmdTimeout;
                    using var reader = await selectCmd.ExecuteReaderAsync();
                    using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.CheckConstraints, transaction);
                    bulkCopy.DestinationTableName = stagingName;
                    bulkCopy.BatchSize = _batchSize;
                    bulkCopy.BulkCopyTimeout = cmdTimeout;

                    bool transformUsers = userMapping != null && userMapping.Count > 0;
                    if (transformUsers)
                    {
                        var dt = new DataTable();
                        using (var schemaTable = reader.GetSchemaTable())
                        {
                            if (schemaTable != null)
                                foreach (DataRow schemaRow in schemaTable.Rows)
                                {
                                    var colName = (string)schemaRow["ColumnName"];
                                    var dataType = (Type)schemaRow["DataType"];
                                    var col = new DataColumn(colName, dataType);
                                    if (dataType == typeof(string) && schemaRow["ColumnSize"] != DBNull.Value && schemaRow["ColumnSize"] is int sz
                                        && sz > 0 && sz != -1 && sz != 2147483647)
                                        col.MaxLength = sz;
                                    dt.Columns.Add(col);
                                }
                        }
                        // Override MaxLength from target column schema for truncation detection
                        var targetColsForGlobal = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
                        var targetColByNameGlobal = targetColsForGlobal.ToDictionary(t => t.Schema.ColumnName, t => t.Schema, StringComparer.OrdinalIgnoreCase);
                        foreach (DataColumn col in dt.Columns)
                        {
                            if (col.DataType != typeof(string)) continue;
                            if (!targetColByNameGlobal.TryGetValue(col.ColumnName, out var tc) || !tc.CharacterMaximumLength.HasValue) continue;
                            var maxLen = tc.CharacterMaximumLength.Value;
                            if (maxLen > 0 && maxLen != -1 && maxLen != 2147483647)
                                col.MaxLength = maxLen;
                        }
                        foreach (DataColumn col in dt.Columns)
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                        int totalTruncations = 0;
                        while (true)
                        {
                            for (int i = 0; i < _batchSize && reader.Read(); i++)
                            {
                                var row = dt.NewRow();
                                for (int c = 0; c < reader.FieldCount; c++)
                                    row[c] = reader.IsDBNull(c) ? DBNull.Value : reader.GetValue(c);
                                dt.Rows.Add(row);
                            }
                            if (dt.Rows.Count == 0) break;
                            foreach (var colName in new[] { "CreatorUserId", "LastModifierUserId", "DeleterUserId", "CreatedBy", "ModifiedBy", "UserId" })
                                if (dt.Columns.Contains(colName))
                                    foreach (DataRow row in dt.Rows)
                                    {
                                        if (row[colName] == DBNull.Value) continue;
                                        try
                                        {
                                            if (userMapping!.TryGetValue(Convert.ToInt64(row[colName]), out long newId))
                                                row[colName] = newId;
                                        }
                                        catch (Exception ex) { Log.Debug("[Data] User ID mapping conversion failed for column {Column}: {Error}", colName, ex.Message); }
                                    }
                            totalTruncations += TruncateStringRowsToColumnMaxLength(dt, targetTableName);
                            await bulkCopy.WriteToServerAsync(dt);
                            dt.Rows.Clear();
                        }
                        if (totalTruncations > 0)
                            Log.Information("[Truncation] {TargetTable}: {TotalTruncations} values truncated (global table buffered path)", targetTableName, totalTruncations);
                    }
                    else
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                            bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                        await bulkCopy.WriteToServerAsync(reader);
                    }
                }

                // 2b. Detect potential truncations in staging table
                await DetectStagingTruncationsAsync(targetConn, stagingName, targetTableName, transaction);

                // 3. Target columns and PK setup
                var pkColumn = pkInfo.ColumnName;
                var pkColEsc = pkColumn.Replace("]", "]]");
                bool hasIdentity = await HasIdentityColumnAsync(targetConn, targetTableName, transaction);
                bool pkIsGuid = string.Equals(pkInfo.DataType, "uniqueidentifier", StringComparison.OrdinalIgnoreCase);
                var matchKeyEsc = matchKeyColumn.Replace("]", "]]");

                var targetColsWithDefault = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName, transaction);
                var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName, transaction);
                var targetEsc = targetTableName.Replace("]", "]]");
                var stagingEsc = stagingName.Replace("]", "]]");

                // 4. Build MERGE column lists
                // UPDATE SET: all non-PK, non-computed columns that exist in both source and target
                var updateSetParts = new List<string>();
                var insertCols = new List<string>();
                var insertVals = new List<string>();

                foreach (var tc in targetColsWithDefault)
                {
                    if (computedCols.Contains(tc.Schema.ColumnName)) continue;
                    var colEsc = tc.Schema.ColumnName.Replace("]", "]]");

                    // For INSERT: exclude identity PK (auto-generated), include GUID PK (preserved)
                    bool isPkCol = string.Equals(tc.Schema.ColumnName, pkColumn, StringComparison.OrdinalIgnoreCase);
                    if (isPkCol && hasIdentity)
                    {
                        // identity PK: excluded from INSERT (auto-generated)
                    }
                    else if (isPkCol && pkIsGuid)
                    {
                        // GUID PK: preserve source GUID
                        insertCols.Add($"[{colEsc}]");
                        insertVals.Add($"s.[{colEsc}]");
                    }
                    else
                    {
                        insertCols.Add($"[{colEsc}]");
                        if (sourceColNames.Contains(tc.Schema.ColumnName))
                            insertVals.Add($"s.[{colEsc}]");
                        else
                            insertVals.Add(GetDefaultSqlForAdcOnlyColumn(tc.Schema.ColumnName, tc));
                    }

                    // For UPDATE SET: non-PK, non-matchKey columns that exist in source
                    if (!isPkCol && !string.Equals(tc.Schema.ColumnName, matchKeyColumn, StringComparison.OrdinalIgnoreCase)
                        && sourceColNames.Contains(tc.Schema.ColumnName))
                    {
                        updateSetParts.Add($"t.[{colEsc}] = s.[{colEsc}]");
                    }
                }

                // 5. Build and execute MERGE SQL
                var pkSqlType = pkInfo.DataType?.ToLowerInvariant() switch
                {
                    "uniqueidentifier" => "uniqueidentifier",
                    "bigint" => "bigint",
                    _ => "int"
                };

                var updateClause = updateSetParts.Count > 0
                    ? $"WHEN MATCHED THEN UPDATE SET {string.Join(", ", updateSetParts)}\n"
                    : "";

                var mergeSql = $@"
DECLARE @MergeOutput TABLE (MergeAction NVARCHAR(10), OldId {pkSqlType}, NewId {pkSqlType});
MERGE [dbo].[{targetEsc}] AS t
USING [dbo].[{stagingEsc}] AS s
ON t.[{matchKeyEsc}] = s.[{matchKeyEsc}]
{updateClause}WHEN NOT MATCHED BY TARGET THEN
  INSERT ({string.Join(", ", insertCols)})
  VALUES ({string.Join(", ", insertVals)})
OUTPUT $action, s.[{pkColEsc}], inserted.[{pkColEsc}] INTO @MergeOutput(MergeAction, OldId, NewId);
SELECT MergeAction, OldId, NewId FROM @MergeOutput;";

                var mergeResults = (await targetConn.QueryAsync<(string MergeAction, object OldId, object NewId)>(
                    mergeSql, commandTimeout: cmdTimeout, transaction: transaction)).ToList();

                int insertedCount = 0;
                int updatedCount = 0;

                // 6. Process OUTPUT: only track INSERTed rows in IdMapping
                var mappingTable = pkInfo.DataType?.ToLowerInvariant() switch
                {
                    "int" => "IdMappingInt",
                    "bigint" => "IdMappingBigInt",
                    "uniqueidentifier" => "IdMappingGuid",
                    _ => (string?)null
                };

                if (mappingTable != null)
                {
                    var mappingTableEsc = mappingTable.Replace("]", "]]");
                    var insertRows = new List<object>();
                    foreach (var row in mergeResults)
                    {
                        if (string.Equals(row.MergeAction, "INSERT", StringComparison.OrdinalIgnoreCase))
                        {
                            insertedCount++;
                            insertRows.Add(new { TableName = targetTableName, ColumnName = pkColumn, OldId = row.OldId, NewId = row.NewId, MigrationBatch = migrationBatch });
                        }
                        else if (string.Equals(row.MergeAction, "UPDATE", StringComparison.OrdinalIgnoreCase))
                        {
                            updatedCount++;
                        }
                    }
                    if (insertRows.Count > 0)
                    {
                        await targetConn.ExecuteAsync(
                            $"INSERT INTO [dbo].[{mappingTableEsc}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId) VALUES (@TableName, @ColumnName, @OldId, @NewId, @MigrationBatch, NULL)",
                            insertRows, transaction: transaction, commandTimeout: cmdTimeout);
                    }
                }
                else
                {
                    insertedCount = mergeResults.Count(r => string.Equals(r.MergeAction, "INSERT", StringComparison.OrdinalIgnoreCase));
                    updatedCount = mergeResults.Count(r => string.Equals(r.MergeAction, "UPDATE", StringComparison.OrdinalIgnoreCase));
                }

                // 7. Drop staging table and commit
                await DropStagingTableIfExistsAsync(targetConn, stagingName, transaction);
                transaction.Commit();

                sw.Stop();
                TotalRowsMigrated += insertedCount;
                Log.Information("[DataSync] Global MERGE {TargetTable} — {Inserted} inserted, {Updated} updated in {ElapsedMs}ms",
                    targetTableName, insertedCount, updatedCount, sw.ElapsedMilliseconds);
                ReportWriter.AddDataSyncTable(sourceTableName, targetTableName, insertedCount + updatedCount, sw.ElapsedMilliseconds, "MERGE (global)", null);
                if (mappingTable != null && insertedCount > 0)
                    Log.Information("   -> IdMapping: {Inserted} row(s) -> [dbo].[{MappingTable}] (TenantId = NULL)", insertedCount, mappingTable);
            }
            catch (Exception ex)
            {
                try { transaction.Rollback(); } catch { }
                Log.Warning("[DataSync] Rolled back global MERGE {TargetTable}: {ErrorMessage}", targetTableName, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Gets column schemas for a single table (dbo) from the given connection.
        /// </summary>
        private static async Task<List<ColumnSchema>> GetSourceColumnSchemasAsync(SqlConnection conn, string tableName)
        {
            var rows = await conn.QueryAsync<ColumnSchema>(@"
                SELECT TABLE_NAME AS TableName, COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType,
                    CHARACTER_MAXIMUM_LENGTH AS CharacterMaximumLength,
                    NUMERIC_PRECISION AS NumericPrecision, NUMERIC_SCALE AS NumericScale
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @TableName
                ORDER BY ORDINAL_POSITION", new { TableName = tableName });
            return rows.ToList();
        }

        /// <summary>
        /// Gets target column schemas plus COLUMN_DEFAULT and IS_NULLABLE for a single table.
        /// </summary>
        private static async Task<List<TargetColumnInfo>> GetTargetColumnsWithDefaultsAsync(SqlConnection conn, string tableName, SqlTransaction? transaction = null)
        {
            var rows = await conn.QueryAsync<(string TableName, string ColumnName, string DataType, int? CharacterMaximumLength, byte? NumericPrecision, int? NumericScale, string? DefaultValue, string IsNullable)>(@"
                SELECT TABLE_NAME AS TableName, COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType,
                    CHARACTER_MAXIMUM_LENGTH AS CharacterMaximumLength,
                    NUMERIC_PRECISION AS NumericPrecision, NUMERIC_SCALE AS NumericScale,
                    COLUMN_DEFAULT AS DefaultValue, IS_NULLABLE AS IsNullable
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @TableName
                ORDER BY ORDINAL_POSITION", new { TableName = tableName }, transaction: transaction);
            return rows.Select(r => new TargetColumnInfo
            {
                Schema = new ColumnSchema
                {
                    TableName = r.TableName,
                    ColumnName = r.ColumnName,
                    DataType = r.DataType,
                    CharacterMaximumLength = r.CharacterMaximumLength,
                    NumericPrecision = r.NumericPrecision,
                    NumericScale = r.NumericScale,
                },
                DefaultValue = r.DefaultValue,
                IsNullable = string.Equals(r.IsNullable, "YES", StringComparison.OrdinalIgnoreCase)
            }).ToList();
        }

        /// <summary>
        /// Gets column names that are computed (is_computed = 1). These must be excluded from INSERT/MERGE.
        /// </summary>
        private static async Task<HashSet<string>> GetComputedColumnNamesAsync(SqlConnection conn, string tableName, SqlTransaction? transaction = null)
        {
            var names = await conn.QueryAsync<string>(@"
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID(@TableName) AND c.is_computed = 1",
                new { TableName = "dbo." + tableName }, transaction: transaction);
            return new HashSet<string>(names ?? Enumerable.Empty<string>(), StringComparer.OrdinalIgnoreCase);
        }

        private static bool ColumnTypesEqual(ColumnSchema a, ColumnSchema b)
        {
            if (!string.Equals(a.DataType, b.DataType, StringComparison.OrdinalIgnoreCase)) return false;
            if ((a.CharacterMaximumLength ?? -1) != (b.CharacterMaximumLength ?? -1)) return false;
            if ((a.NumericPrecision ?? 0) != (b.NumericPrecision ?? 0)) return false;
            if ((a.NumericScale ?? 0) != (b.NumericScale ?? 0)) return false;
            return true;
        }

        private static bool IsStringType(string? dataType)
        {
            var dt = (dataType ?? "").ToLowerInvariant();
            return dt is "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext";
        }

        private static bool IsBinaryType(string? dataType)
        {
            var dt = (dataType ?? "").ToLowerInvariant();
            return dt is "varbinary" or "binary" or "image";
        }

        private static int GetEffectiveMaxLength(ColumnSchema c)
        {
            if (c.CharacterMaximumLength.HasValue) return c.CharacterMaximumLength.Value;
            return -1;
        }

        private static string GetSqlTypeString(ColumnSchema c)
        {
            var dt = (c.DataType ?? "").ToLowerInvariant();
            if (dt == "nvarchar" || dt == "varchar" || dt == "nchar" || dt == "char")
            {
                var len = c.CharacterMaximumLength == -1 || c.CharacterMaximumLength == 2147483647 ? "max" : (c.CharacterMaximumLength?.ToString() ?? "255");
                return $"{c.DataType}({len})";
            }
            if (dt == "varbinary" || dt == "binary" || dt == "image")
            {
                var maxLen = c.CharacterMaximumLength ?? -1;
                bool useMax = maxLen == -1 || maxLen == 2147483647 || maxLen <= 0 || maxLen == 1; // 1 thường là metadata sai cho varbinary(max)
                var len = useMax ? "max" : maxLen.ToString();
                return dt == "image" ? "varbinary(max)" : $"{c.DataType}({len})";
            }
            if (dt == "decimal" || dt == "numeric")
                return $"{c.DataType}({c.NumericPrecision ?? 18},{c.NumericScale ?? 0})";
            return c.DataType ?? "sql_variant";
        }

        private static Type GetClrType(ColumnSchema c)
        {
            var dt = (c.DataType ?? "").ToLowerInvariant();
            return dt switch
            {
                "int" => typeof(int),
                "bigint" => typeof(long),
                "smallint" => typeof(short),
                "tinyint" => typeof(byte),
                "bit" => typeof(bool),
                "datetime" or "datetime2" or "date" or "smalldatetime" => typeof(DateTime),
                "decimal" or "numeric" => typeof(decimal),
                "float" => typeof(double),
                "real" => typeof(float),
                "uniqueidentifier" => typeof(Guid),
                "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext" => typeof(string),
                "varbinary" or "binary" or "image" => typeof(byte[]),
                _ => typeof(object)
            };
        }

        /// <summary>
        /// Builds SELECT with CONVERT for type-mismatch columns. ADC-only columns are not in SELECT.
        /// </summary>
        private static string BuildSelectSqlWithConversions(
            string sourceTableName,
            List<ColumnSchema> sourceCols,
            Dictionary<string, ColumnSchema> targetColsByName,
            List<string> typeMismatchColNames,
            string whereClause)
        {
            var parts = new List<string>();
            foreach (var sc in sourceCols)
            {
                var name = sc.ColumnName;
                var nameEsc = name.Replace("]", "]]");
                if (typeMismatchColNames.Contains(name) && targetColsByName.TryGetValue(name, out var tc))
                    parts.Add($"CONVERT({GetSqlTypeString(tc)}, [{nameEsc}]) AS [{nameEsc}]");
                else
                    parts.Add($"[{nameEsc}]");
            }
            return "SELECT " + string.Join(", ", parts) + $" FROM [{sourceTableName.Replace("]", "]]")}]{whereClause}";
        }

        /// <summary>
        /// Parses COLUMN_DEFAULT (e.g. (0), ((0)), NULL) to a value for DataRow. Returns DBNull.Value for NULL or unparseable.
        /// </summary>
        private static object ParseColumnDefaultToObject(string? defaultExpr, ColumnSchema schema)
        {
            if (string.IsNullOrWhiteSpace(defaultExpr)) return DBNull.Value;
            var s = defaultExpr.Trim();
            // Strip matched outer parentheses only: ((0)) -> (0) -> 0, but (getdate()) -> getdate()
            while (s.Length >= 2 && s[0] == '(' && s[s.Length - 1] == ')')
            {
                s = s.Substring(1, s.Length - 2).Trim();
            }
            if (string.IsNullOrEmpty(s) || s.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return DBNull.Value;

            // Handle SQL functions that produce runtime values
            var sLower = s.ToLowerInvariant();
            if (sLower == "getdate()" || sLower == "sysdatetime()" || sLower == "getutcdate()" || sLower == "sysutcdatetime()")
                return DateTime.UtcNow;
            if (sLower == "newid()" || sLower == "newsequentialid()")
                return Guid.NewGuid();

            var dt = (schema.DataType ?? "").ToLowerInvariant();
            try
            {
                if (dt == "bit") return s == "1" || s.Equals("true", StringComparison.OrdinalIgnoreCase);
                if (dt == "int" || dt == "smallint" || dt == "tinyint") return int.Parse(s);
                if (dt == "bigint") return long.Parse(s);
                if (dt == "decimal" || dt == "numeric" || dt == "float" || dt == "real") return decimal.Parse(s);
                if (dt == "datetime" || dt == "datetime2" || dt == "date") return DateTime.Parse(s);
                if (dt == "uniqueidentifier") return Guid.Parse(s.Replace("'", ""));
            }
            catch { /* fallback */ }
            return DBNull.Value;
        }

        /// <summary>
        /// Defensive: trim string values and truncate to column MaxLength when set (so BCP doesn't fail with "invalid column length").
        /// Only applies to string columns with MaxLength > 0; nvarchar(max) (no MaxLength) is left unchanged.
        /// </summary>
        private static int TruncateStringRowsToColumnMaxLength(DataTable dt, string tableName)
        {
            int truncationCount = 0;
            foreach (DataColumn col in dt.Columns)
            {
                if (col.DataType != typeof(string) || col.MaxLength <= 0 || col.MaxLength == 2147483647) continue;
                int maxLen = col.MaxLength;
                foreach (DataRow row in dt.Rows)
                {
                    if (row.IsNull(col)) continue;
                    var s = row[col] as string;
                    if (s == null) continue;
                    if (s.Length > maxLen)
                    {
                        // Only trim when truncation is actually needed — preserve whitespace otherwise
                        s = s.Trim();
                        if (s.Length > maxLen)
                        {
                            Log.Information("[Truncation] {TableName}.{ColumnName}: truncated from {SourceLength} to {MaxLength} chars", tableName, col.ColumnName, s.Length, maxLen);
                            s = s.Substring(0, maxLen);
                            truncationCount++;
                        }
                        row[col] = s;
                    }
                }
            }
            return truncationCount;
        }

        /// <summary>
        /// Detects string values in a staging table that were truncated during SqlBulkCopy streaming.
        /// Queries target column max lengths and checks for values at the boundary.
        /// </summary>
        private static async Task DetectStagingTruncationsAsync(SqlConnection targetConn, string stagingTableName, string displayTableName, SqlTransaction? transaction = null)
        {
            try
            {
                var stagingEsc = stagingTableName.Replace("]", "]]");
                // Get variable-length string columns with max lengths from the staging table
                // Exclude char/nchar (fixed-width) — LEN() always equals column width, causing false positives
                var cols = await targetConn.QueryAsync<(string ColumnName, string TypeName, int MaxLength)>(
                    @"SELECT c.name AS ColumnName, t.name AS TypeName, c.max_length AS MaxLength
                      FROM sys.columns c
                      INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                      WHERE c.object_id = OBJECT_ID(@Table)
                        AND t.name IN ('varchar','nvarchar')
                        AND c.max_length > 0 AND c.max_length <> -1",
                    new { Table = $"[dbo].[{stagingEsc}]" }, transaction: transaction);

                int totalTruncations = 0;
                foreach (var col in cols)
                {
                    var colEsc = col.ColumnName.Replace("]", "]]");
                    // For nvarchar, max_length is in bytes (2 per char)
                    var charLimit = col.TypeName == "nvarchar" ? col.MaxLength / 2 : col.MaxLength;

                    var count = await targetConn.ExecuteScalarAsync<int>(
                        $"SELECT COUNT(*) FROM [dbo].[{stagingEsc}] WHERE LEN([{colEsc}]) = @MaxLen",
                        new { MaxLen = charLimit }, transaction: transaction);
                    if (count > 0)
                    {
                        Log.Information("[Truncation] {TableName}.{ColumnName}: {Count} value(s) at max length ({MaxLength} chars) — possible truncation",
                            displayTableName, col.ColumnName, count, charLimit);
                        totalTruncations += count;
                    }
                }
                if (totalTruncations > 0)
                    Log.Information("[Truncation] {TableName}: {Count} potential truncation(s) detected in staging", displayTableName, totalTruncations);
            }
            catch (Exception ex)
            {
                Log.Debug("[Truncation] Detection query failed for {TableName}: {Error}", displayTableName, ex.Message);
            }
        }

        /// <summary>
        /// SQL expression for NOT NULL ADC-only column when no COLUMN_DEFAULT. Used to avoid INSERT NULL.
        /// </summary>
        private static string GetTypeBasedDefaultSql(string? dataType)
        {
            var dt = (dataType ?? "").ToLowerInvariant();
            return dt switch
            {
                "bit" => "0",
                "int" or "bigint" or "smallint" or "tinyint" or "decimal" or "numeric" or "float" or "real" => "0",
                "datetime" or "datetime2" or "date" or "smalldatetime" => "SYSDATETIME()",
                "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext" => "N''",
                "uniqueidentifier" => "CAST('00000000-0000-0000-0000-000000000000' AS uniqueidentifier)",
                _ => "NULL"
            };
        }

        /// <summary>
        /// CLR default for NOT NULL ADC-only column when no COLUMN_DEFAULT (buffer path).
        /// </summary>
        private static object GetTypeBasedDefaultObject(string? dataType)
        {
            var dt = (dataType ?? "").ToLowerInvariant();
            return dt switch
            {
                "bit" => (object)false,
                "int" => 0,
                "bigint" => (long)0,
                "smallint" => (short)0,
                "tinyint" => (byte)0,
                "decimal" or "numeric" => 0m,
                "float" => 0.0,
                "real" => 0f,
                "datetime" or "datetime2" or "date" or "smalldatetime" => DateTime.MinValue,
                "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext" => "",
                "uniqueidentifier" => Guid.Empty,
                _ => DBNull.Value
            };
        }

        /// <summary>
        /// Gets default value for an ADC-only column: Known list first, then COLUMN_DEFAULT, then type-based if NOT NULL, else NULL.
        /// Fallback: if we would return NULL but column has a value-type, use type default (in case IsNullable metadata was wrong).
        /// </summary>
        private static object GetDefaultForAdcOnlyColumn(string columnName, TargetColumnInfo? targetCol)
        {
            if (KnownAdcOnlyDefaults.TryGetValue(columnName, out var known))
            {
                // Resolve sentinel: CreationTime needs current timestamp, not class-load time
                if (known is string s && s == "__USE_DATETIME_NOW__")
                    return DateTime.UtcNow;
                return known;
            }
            if (targetCol != null && !string.IsNullOrWhiteSpace(targetCol.DefaultValue))
                return ParseColumnDefaultToObject(targetCol.DefaultValue, targetCol.Schema);
            if (targetCol != null && !targetCol.IsNullable)
                return GetTypeBasedDefaultObject(targetCol.Schema.DataType);
            if (targetCol != null)
            {
                var typeDefault = GetTypeBasedDefaultObject(targetCol.Schema.DataType);
                if (typeDefault != DBNull.Value) return typeDefault;
            }
            return DBNull.Value;
        }

        /// <summary>
        /// Returns SQL expression for default value of an ADC-only column (for use in INSERT ... SELECT).
        /// Order: KnownAdcOnlyDefaults → COLUMN_DEFAULT → type-based if NOT NULL → fallback type-based → NULL.
        /// </summary>
        internal static string GetDefaultSqlForAdcOnlyColumn(string columnName, TargetColumnInfo? targetCol)
        {
            if (KnownAdcOnlyDefaults.TryGetValue(columnName, out var known))
            {
                if (known == DBNull.Value) return "NULL";
                if (known is string s && s == "__USE_DATETIME_NOW__") return "SYSDATETIME()";
                if (known is bool b) return b ? "1" : "0";
                if (known is int or long or decimal) return known.ToString()!;
                if (known is DateTime) return "SYSDATETIME()";
                return "NULL";
            }
            if (targetCol != null && !string.IsNullOrWhiteSpace(targetCol.DefaultValue))
                return ParseColumnDefaultToSqlExpression(targetCol.DefaultValue);
            if (targetCol != null && !targetCol.IsNullable)
                return GetTypeBasedDefaultSql(targetCol.Schema.DataType);
            if (targetCol != null)
            {
                var typeDefault = GetTypeBasedDefaultSql(targetCol.Schema.DataType);
                if (typeDefault != "NULL") return typeDefault;
            }
            return "NULL";
        }

        /// <summary>
        /// Parses COLUMN_DEFAULT (e.g. (SYSDATETIME()), (0), ((0)), NULL) to a safe SQL expression for INSERT.
        /// </summary>
        private static string ParseColumnDefaultToSqlExpression(string? defaultExpr)
        {
            if (string.IsNullOrWhiteSpace(defaultExpr)) return "NULL";
            var s = defaultExpr.Trim();
            // Strip matched outer parentheses only: ((0)) -> (0) -> 0, but (getdate()) -> getdate()
            while (s.Length >= 2 && s[0] == '(' && s[s.Length - 1] == ')')
            {
                s = s.Substring(1, s.Length - 2).Trim();
            }
            if (string.IsNullOrEmpty(s) || s.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return "NULL";
            if (s.Equals("SYSDATETIME()", StringComparison.OrdinalIgnoreCase) || s.StartsWith("SYSDATETIME()", StringComparison.OrdinalIgnoreCase)) return "SYSDATETIME()";
            if (s.Equals("GETDATE()", StringComparison.OrdinalIgnoreCase)) return "GETDATE()";
            if (s == "0" || s == "1") return s;
            if (int.TryParse(s, out _) || long.TryParse(s, out _)) return s;
            if (s.StartsWith("'") && s.EndsWith("'")) return s;
            return "NULL";
        }
    }
}
