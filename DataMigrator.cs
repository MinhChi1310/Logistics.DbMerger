using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;

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
            ["CreationTime"] = (object)default(DateTime), // ADC-only, NOT NULL -> SYSDATETIME()
            ["MandatoryQualification"] = (object)false, // Contact. ADC-only, bit NOT NULL
            ["TeamMemberBreakGroup"] = (object)false, // Settings. ADC-only, bit NOT NULL
            ["UKGPunchIntegration"] = (object)false, // Settings. ADC-only, bit NOT NULL
            // Other NOT NULL ADC-only columns use type-based default when targetCol is provided
        };

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

        private async Task<bool> HasIdentityColumnAsync(SqlConnection conn, string tableName)
        {
            var fullName = "dbo." + tableName;
            var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) 
                FROM sys.identity_columns 
                WHERE object_id = OBJECT_ID(@TableName)", new { TableName = fullName });
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
            Console.WriteLine($"[Data] Migrating {sourceTableName} -> {destTable}...");

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
                whereClause = $" WHERE TenantId = {sourceTenantId.Value}";
                Console.WriteLine($"   -> Filtering by TenantId = {sourceTenantId.Value}");
            }
            else if (sourceTenantId.HasValue && !hasTenantId)
            {
                Console.WriteLine("   -> Table has no TenantId. Migrating ALL rows (Global/System Table).");
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
                Console.WriteLine($"   -> ADC-only columns: {adcOnlyCols.Count} (will set defaults)");
            if (typeMismatchColNames.Count > 0)
                Console.WriteLine($"   -> Type-mismatch columns: {typeMismatchColNames.Count} (will convert)");
            if (stringLengthTruncateCols.Count > 0)
                Console.WriteLine($"   -> String length truncation: {stringLengthTruncateCols.Count} column(s) (target shorter than source)");

            string selectSql;
            if (adcOnlyCols.Count > 0 || typeMismatchColNames.Count > 0)
            {
                selectSql = BuildSelectSqlWithConversions(sourceTableName, sourceCols, targetColsByName, typeMismatchColNames, whereClause);
            }
            else
            {
                selectSql = "SELECT *";
                if (sourceTableName.Equals("Contact", StringComparison.OrdinalIgnoreCase))
                    selectSql += ", NULL as OldSAPID, 0 as PartTimeFlex, NULL as FobNumber, 0 as ExcludeOvertime, NULL as ExcludeOvertimeComment, 0 as VisaRestriction, NULL as VisaEndDate, NULL as MaximumHoursByFortnight, NULL as RosterProfile, 0 as ExcludeFromNotifications, 0 as MandatoryQualification";
                selectSql += $" FROM [{sourceTableName}]{whereClause}";
            }

            using var cmd = new SqlCommand(selectSql, sourceConn);
            cmd.CommandTimeout = 600;
            
            using var reader = await cmd.ExecuteReaderAsync();

            using var bulkCopy = new SqlBulkCopy(targetConn,
                hasIdentity? SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock : SqlBulkCopyOptions.TableLock,
                null);

            bulkCopy.DestinationTableName = destTable;
            bulkCopy.BatchSize = _batchSize;
            bulkCopy.BulkCopyTimeout = 600;
            bulkCopy.NotifyAfter = 1000;
            bulkCopy.SqlRowsCopied += (sender, e) => Console.Write(".");

            bool transformTenantId = (sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId && hasTenantId);
            bool transformUsers = (userMapping != null && userMapping.Count > 0);
            // Dùng buffer path khi có cột string/binary (chung hoặc chỉ ở target) để set MaxLength đúng, tránh BCP "invalid column length" (vd. ProfilePhoto varbinary(max))
            bool useBufferPath = transformTenantId || transformUsers || adcOnlyCols.Count > 0 || typeMismatchColNames.Count > 0 || stringLengthTruncateCols.Count > 0 || hasCommonStringCols || hasCommonBinaryCols || hasTargetBinaryCols;

            if (useBufferPath)
            {
                if (transformTenantId) Console.WriteLine($"   -> Transforming TenantId: {sourceTenantId} -> {targetTenantId}");
                if (transformUsers) Console.WriteLine($"   -> Transforming User IDs (Audit Fields)");

                // 1. Build DataTable schema from reader: only set MaxLength for string columns; 2. Do not set for nvarchar(max)
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
                            // DataColumn.MaxLength chỉ áp dụng cho string; không set cho byte[]. Quy tắc vàng: nvarchar(max)/varchar(max) → KHÔNG set MaxLength
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

                // 3. ColumnMappings theo thứ tự cột ĐÍCH (ORDINAL_POSITION) để BCP colid khớp; đồng bộ MaxLength với target chỉ cho string
                foreach (var targetColInfo in targetColsWithDefault)
                {
                    var colName = targetColInfo.Schema.ColumnName;
                    if (!dt.Columns.Contains(colName)) continue;
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

                var totalRows = 0;
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
                                catch { /* ignore conversion */ }
                            }
                        }
                    }

                    TruncateStringRowsToColumnMaxLength(dt);
                    await bulkCopy.WriteToServerAsync(dt);
                    totalRows += dt.Rows.Count;
                    dt.Rows.Clear();
                }

                Console.WriteLine($"\n[Data] Completed {sourceTableName} (Transformed {totalRows} rows)");
            }
            else
            {
                // Streaming Mode (No ID Transform)
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string colName = reader.GetName(i);
                    if (targetSchemaCols.Contains(colName))
                        bulkCopy.ColumnMappings.Add(colName, colName);
                }
                try
                {

                    await bulkCopy.WriteToServerAsync(reader);
                    Console.WriteLine($"\n[Data] Completed {sourceTableName}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\n[Error] Failed to migrate {sourceTableName}: {ex.Message}");
                }
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
            Console.WriteLine($"[Data] Migrating {sourceTableName} -> {targetTableName}... (Natural PK, insert missing only)");
            var stagingName = targetTableName + "_staging";
            await CreateStagingTableForCompositeAsync(sourceConn, targetConn, sourceTableName, targetTableName); // same schema as source, name = targetTableName_staging

            bool hasTenantId = await HasTenantIdColumnAsync(sourceConn, sourceTableName);
            string whereClause = (sourceTenantId.HasValue && hasTenantId) ? $" WHERE TenantId = {sourceTenantId.Value}" : "";
            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var sourceColNames = new HashSet<string>(sourceCols.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);
            var selectParts = sourceCols.Select(c => $"[{c.ColumnName.Replace("]", "]]")}]").ToList();
            var selectSql = "SELECT " + string.Join(", ", selectParts) + $" FROM [{sourceTableName.Replace("]", "]]")}]{whereClause}";

            using (var selectCmd = new SqlCommand(selectSql, sourceConn))
            {
                selectCmd.CommandTimeout = 600;
                using var reader = await selectCmd.ExecuteReaderAsync();
                var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.TableLock, null);
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
                            foreach (DataRow row in schemaTable.Rows)
                                dt.Columns.Add(new DataColumn((string)row["ColumnName"], (Type)row["DataType"]));
                    }
                    foreach (DataColumn col in dt.Columns)
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
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
                                        catch { }
                                    }
                        await bulkCopy.WriteToServerAsync(dt);
                        dt.Rows.Clear();
                    }
                }
                else
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                        bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                    await bulkCopy.WriteToServerAsync(reader);
                }
            }

            var targetColsWithDefault = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName);
            var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName);
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
            var inserted = await targetConn.ExecuteAsync(insertSql, commandTimeout: 600);
            await DropStagingTableIfExistsAsync(targetConn, stagingName);
            Console.WriteLine($"   -> Natural PK: inserted {inserted} missing row(s) into [dbo].[{targetTableName}] (skipped existing).");
        }

        /// <summary>
        /// Creates staging table on target: OldId (PK type) + all source columns except PK. Drops existing if present.
        /// </summary>
        public async Task CreateStagingTableAsync(SqlConnection sourceConn, SqlConnection targetConn, string sourceTableName, string targetTableName, PkColumnInfo pkInfo)
        {
            var stagingName = targetTableName + "_staging";
            await DropStagingTableIfExistsAsync(targetConn, stagingName);

            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var pkColName = pkInfo.ColumnName;
            var nonPkCols = sourceCols.Where(c => !string.Equals(c.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();

            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"CREATE TABLE [dbo].[{stagingName}] (");
            sb.AppendLine($"  [OldId] {GetPkSqlType(pkInfo.DataType)} NOT NULL,");
            foreach (var c in nonPkCols)
            {
                var nullable = " NULL";
                sb.AppendLine($"  [{c.ColumnName}] {GetSqlTypeString(c)}{nullable},");
            }
            sb.Length -= 2; // remove last comma + newline
            sb.AppendLine();
            sb.AppendLine(");");

            using var cmd = new SqlCommand(sb.ToString(), targetConn);
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"   -> Created staging table [dbo].[{stagingName}]");
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

        private static async Task DropStagingTableIfExistsAsync(SqlConnection conn, string stagingName)
        {
            var objName = "dbo." + stagingName;
            await conn.ExecuteAsync("IF OBJECT_ID(@Name, 'U') IS NOT NULL DROP TABLE [dbo].[" + stagingName.Replace("]", "]]") + "]", new { Name = objName });
        }

        /// <summary>
        /// Creates staging table with same structure as source (all columns). For composite-key table migration.
        /// </summary>
        public async Task CreateStagingTableForCompositeAsync(SqlConnection sourceConn, SqlConnection targetConn, string sourceTableName, string targetTableName)
        {
            var stagingName = targetTableName + "_staging";
            await DropStagingTableIfExistsAsync(targetConn, stagingName);
            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"CREATE TABLE [dbo].[{stagingName}] (");
            foreach (var c in sourceCols)
            {
                var nullable = " NULL";
                sb.AppendLine($"  [{c.ColumnName}] {GetSqlTypeString(c)}{nullable},");
            }
            sb.Length -= 2;
            sb.AppendLine();
            sb.AppendLine(");");
            using var cmd = new SqlCommand(sb.ToString(), targetConn);
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"   -> Created staging table [dbo].[{stagingName}] (composite)");
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
            int? targetTenantId)
        {
            string whereClause = "";
            if (sourceTenantId.HasValue && await TableHasTenantIdColumnAsync(sourceConn, sourceTableName))
                whereClause = $" WHERE TenantId = {sourceTenantId.Value}";

            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var sourceColNames = new HashSet<string>(sourceCols.Select(c => c.ColumnName), StringComparer.OrdinalIgnoreCase);

            var stagingName = targetTableName + "_staging";
            await CreateStagingTableForCompositeAsync(sourceConn, targetConn, sourceTableName, targetTableName);

            var selectSql = "SELECT * FROM [" + sourceTableName.Replace("]", "]]") + "]" + whereClause;
            using (var selectCmd = new SqlCommand(selectSql, sourceConn))
            {
                selectCmd.CommandTimeout = 600;
                using var reader = await selectCmd.ExecuteReaderAsync();
                var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.TableLock, null);
                bulkCopy.DestinationTableName = stagingName;
                bulkCopy.BatchSize = _batchSize;
                bulkCopy.BulkCopyTimeout = 600;
                for (int i = 0; i < reader.FieldCount; i++)
                    bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                await bulkCopy.WriteToServerAsync(reader);
            }
            Console.Write(".");

            bool transformTenantId = sourceTenantId.HasValue && targetTenantId.HasValue && sourceTenantId != targetTenantId;
            if (transformTenantId)
            {
                await targetConn.ExecuteAsync(
                    $"UPDATE [dbo].[{stagingName.Replace("]", "]]")}] SET TenantId = @TenantId",
                    new { TenantId = targetTenantId!.Value });
            }

            var targetCols = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName);
            var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName);
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
            var inserted = await targetConn.ExecuteAsync(insertSql, prm);
            await DropStagingTableIfExistsAsync(targetConn, stagingName);
            Console.WriteLine($"\n   -> Composite key: inserted {inserted} row(s) into [dbo].[{targetTableName}] (skipped existing).");
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
            Dictionary<long, long>? userMapping)
        {
            var stagingName = targetTableName + "_staging";
            var sourceCols = await GetSourceColumnSchemasAsync(sourceConn, sourceTableName);
            var pkColName = pkInfo.ColumnName;
            var nonPkCols = sourceCols.Where(c => !string.Equals(c.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();

            // Build SELECT: Id AS OldId, col1, col2, ... FROM source WHERE ...
            var selectParts = new List<string> { $"[{pkColName}] AS OldId" };
            foreach (var c in nonPkCols)
                selectParts.Add($"[{c.ColumnName}]");
            var selectSql = "SELECT " + string.Join(", ", selectParts) + $" FROM [{sourceTableName}]{whereClause}";

            using var selectCmd = new SqlCommand(selectSql, sourceConn);
            selectCmd.CommandTimeout = 600;
            using var reader = await selectCmd.ExecuteReaderAsync();

            var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.TableLock, null);
            bulkCopy.DestinationTableName = stagingName;
            bulkCopy.BatchSize = _batchSize;
            bulkCopy.BulkCopyTimeout = 600;
            bulkCopy.NotifyAfter = 1000;
            bulkCopy.SqlRowsCopied += (_, _) => Console.Write(".");

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
                var stagingColsOrder = await GetTargetColumnsWithDefaultsAsync(targetConn, stagingName);
                foreach (var t in stagingColsOrder)
                {
                    if (dt.Columns.Contains(t.Schema.ColumnName))
                        bulkCopy.ColumnMappings.Add(t.Schema.ColumnName, t.Schema.ColumnName);
                }

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
                                catch { }
                            }
                        }
                    }
                    await bulkCopy.WriteToServerAsync(dt);
                    dt.Rows.Clear();
                }
            }
            else
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
                await bulkCopy.WriteToServerAsync(reader);
            }

            Console.WriteLine();

            // Target columns and ADC-only defaults for MERGE
            var targetColsWithDefault = await GetTargetColumnsWithDefaultsAsync(targetConn, targetTableName);
            var stagingColNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "OldId" };
            foreach (var c in nonPkCols) stagingColNames.Add(c.ColumnName);
            var adcOnlyCols = targetColsWithDefault.Where(t => !stagingColNames.Contains(t.Schema.ColumnName) && !string.Equals(t.Schema.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();
            var targetColsExceptPk = targetColsWithDefault.Where(t => !string.Equals(t.Schema.ColumnName, pkColName, StringComparison.OrdinalIgnoreCase)).ToList();
            var computedCols = await GetComputedColumnNamesAsync(targetConn, targetTableName);

            bool hasIdentity = await HasIdentityColumnAsync(targetConn, targetTableName);
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
            var mergeSql = $@"
DECLARE @Mapping TABLE (OldId {pkSqlType}, NewId {pkSqlType});
MERGE [dbo].[{targetTableName}] AS t
USING [dbo].[{stagingName}] AS s ON 1=0
WHEN NOT MATCHED THEN
  INSERT ({string.Join(", ", insertCols)})
  VALUES ({string.Join(", ", valueExprs)})
OUTPUT s.OldId, inserted.[{pkColName}] INTO @Mapping(OldId, NewId);";
            if (mappingTable != null)
            {
                mergeSql += $@"
INSERT INTO [dbo].[{mappingTable}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)
SELECT @TableName, @ColumnName, OldId, NewId, @MigrationBatch, @TenantId FROM @Mapping;";
            }

            var prm = new { TableName = targetTableName, ColumnName = pkColName, MigrationBatch = migrationBatch, TenantId = (int?)tenantId };
            var rowsAffected = await targetConn.ExecuteAsync(mergeSql, prm, commandTimeout: 600);
            if (mappingTable != null && rowsAffected > 0)
            {
                var mappingCount = rowsAffected / 2; // MERGE rows + INSERT rows
                Console.WriteLine($"   -> IdMapping (bulk): {mappingCount} row(s) -> [dbo].[{mappingTable}]");
            }

            await DropStagingTableIfExistsAsync(targetConn, stagingName);
            Console.WriteLine($"   -> Dropped [dbo].[{stagingName}]");
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
        private static async Task<List<TargetColumnInfo>> GetTargetColumnsWithDefaultsAsync(SqlConnection conn, string tableName)
        {
            var rows = await conn.QueryAsync<(string TableName, string ColumnName, string DataType, int? CharacterMaximumLength, byte? NumericPrecision, int? NumericScale, string? DefaultValue, string IsNullable)>(@"
                SELECT TABLE_NAME AS TableName, COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType,
                    CHARACTER_MAXIMUM_LENGTH AS CharacterMaximumLength,
                    NUMERIC_PRECISION AS NumericPrecision, NUMERIC_SCALE AS NumericScale,
                    COLUMN_DEFAULT AS DefaultValue, IS_NULLABLE AS IsNullable
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @TableName
                ORDER BY ORDINAL_POSITION", new { TableName = tableName });
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
        private static async Task<HashSet<string>> GetComputedColumnNamesAsync(SqlConnection conn, string tableName)
        {
            var names = await conn.QueryAsync<string>(@"
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID(@TableName) AND c.is_computed = 1",
                new { TableName = "dbo." + tableName });
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
                if (typeMismatchColNames.Contains(name) && targetColsByName.TryGetValue(name, out var tc))
                    parts.Add($"CONVERT({GetSqlTypeString(tc)}, [{name}]) AS [{name}]");
                else
                    parts.Add($"[{name}]");
            }
            return "SELECT " + string.Join(", ", parts) + $" FROM [{sourceTableName}]{whereClause}";
        }

        /// <summary>
        /// Parses COLUMN_DEFAULT (e.g. (0), ((0)), NULL) to a value for DataRow. Returns DBNull.Value for NULL or unparseable.
        /// </summary>
        private static object ParseColumnDefaultToObject(string? defaultExpr, ColumnSchema schema)
        {
            if (string.IsNullOrWhiteSpace(defaultExpr)) return DBNull.Value;
            var s = defaultExpr.Trim();
            while (s.Length > 0 && (s[0] == '(' || s[0] == ')' || s[s.Length - 1] == '(' || s[s.Length - 1] == ')'))
            {
                var prev = s;
                s = s.TrimStart('(', ')').TrimEnd('(', ')').Trim();
                if (s == prev) break;
            }
            if (string.IsNullOrEmpty(s) || s.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return DBNull.Value;
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
        private static void TruncateStringRowsToColumnMaxLength(DataTable dt)
        {
            foreach (DataColumn col in dt.Columns)
            {
                if (col.DataType != typeof(string) || col.MaxLength <= 0 || col.MaxLength == 2147483647) continue;
                int maxLen = col.MaxLength;
                foreach (DataRow row in dt.Rows)
                {
                    if (row.IsNull(col)) continue;
                    var s = row[col] as string;
                    if (s == null) continue;
                    s = s.Trim();
                    if (s.Length > maxLen)
                        s = s.Substring(0, maxLen);
                    row[col] = s;
                }
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
                return known;
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
            while (s.Length > 0 && (s[0] == '(' || s[0] == ')' || s[s.Length - 1] == '(' || s[s.Length - 1] == ')'))
            {
                var prev = s;
                s = s.TrimStart('(', ')').TrimEnd('(', ')').Trim();
                if (s == prev) break;
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
