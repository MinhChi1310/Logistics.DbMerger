using Microsoft.Data.SqlClient;
using Dapper;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Disable/Enable foreign keys and update FK columns from IdMapping tables.
    /// </summary>
    public static class FkConstraintHelper
    {
        /// <summary>
        /// Captures original FK state (disabled / not trusted) so we can restore design intent (NOCHECK/trusted) after migration.
        /// </summary>
        private class FkState
        {
            public string SchemaName { get; set; } = "";
            public string TableName { get; set; } = "";
            public string FkName { get; set; } = "";
            public bool WasDisabled { get; set; }
            public bool WasNotTrusted { get; set; }
        }

        // Cached FK states from DisableAllFkAsync. Used by EnableAllFkAsync to restore original enabled/NOCHECK configuration.
        private static List<FkState>? _originalFkStates;

        /// <summary>
        /// Disables all foreign key constraints in the database (schema dbo). Batched into one script for performance.
        /// </summary>
        public static async Task DisableAllFkAsync(SqlConnection conn)
        {
            if (conn.State != System.Data.ConnectionState.Open)
                await conn.OpenAsync();

            var fks = (await conn.QueryAsync<(string SchemaName, string TableName, string FkName, bool IsDisabled, bool IsNotTrusted)>(@"
                SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id) AS SchemaName,
                       OBJECT_NAME(fk.parent_object_id) AS TableName,
                       fk.name AS FkName,
                       fk.is_disabled AS IsDisabled,
                       fk.is_not_trusted AS IsNotTrusted
                FROM sys.foreign_keys fk
                WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'dbo'
                ORDER BY OBJECT_NAME(fk.parent_object_id), fk.name")).ToList();

            // Cache original FK states so we can restore disabled / NOCHECK flags later.
            _originalFkStates = fks.Select(f => new FkState
            {
                SchemaName = f.SchemaName,
                TableName = f.TableName,
                FkName = f.FkName,
                WasDisabled = f.IsDisabled,
                WasNotTrusted = f.IsNotTrusted
            }).ToList();

            var statements = fks.Select(f => $"ALTER TABLE [{f.SchemaName}].[{f.TableName}] NOCHECK CONSTRAINT [{f.FkName}]");
            var script = string.Join("; ", statements);
            if (!string.IsNullOrEmpty(script))
                await conn.ExecuteAsync(script, commandTimeout: 300);
            Console.WriteLine($"[FK] Disabled {fks.Count()} foreign key constraint(s).");
        }

        /// <summary>
        /// Enables all foreign key constraints in the database (schema dbo).
        /// Restores original design:
        /// - FK originally disabled stays disabled.
        /// - FK originally trusted: WITH CHECK CHECK (validate data).
        /// - FK originally untrusted (NOCHECK): CHECK CONSTRAINT (no full validation).
        /// </summary>
        public static async Task EnableAllFkAsync(SqlConnection conn)
        {
            if (conn.State != System.Data.ConnectionState.Open)
                await conn.OpenAsync();

            var states = _originalFkStates;

            // Fallback: if DisableAllFkAsync was not called, preserve old behavior (enable & validate all).
            if (states == null || states.Count == 0)
            {
                var fks = await conn.QueryAsync<(string SchemaName, string TableName, string FkName)>(@"
                    SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id) AS SchemaName,
                           OBJECT_NAME(fk.parent_object_id) AS TableName,
                           fk.name AS FkName
                    FROM sys.foreign_keys fk
                    WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'dbo'
                    ORDER BY OBJECT_NAME(fk.parent_object_id), fk.name");

                foreach (var (schemaName, tableName, fkName) in fks)
                {
                    var sql = $"ALTER TABLE [{schemaName}].[{tableName}] WITH CHECK CHECK CONSTRAINT [{fkName}]";
                    try
                    {
                        await conn.ExecuteAsync(sql);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[FK] Warning: Could not enable [{tableName}].[{fkName}]: {ex.Message}");
                    }
                }
                Console.WriteLine($"[FK] Enabled {fks.Count()} foreign key constraint(s).");
                return;
            }

            foreach (var fk in states)
            {
                // If FK was originally disabled, keep it disabled (design intent).
                if (fk.WasDisabled)
                    continue;

                string sql;
                if (!fk.WasNotTrusted)
                {
                    // Trusted FK: validate existing data.
                    sql = $"ALTER TABLE [{fk.SchemaName}].[{fk.TableName}] WITH CHECK CHECK CONSTRAINT [{fk.FkName}]";
                }
                else
                {
                    // Untrusted / NOCHECK FK: enable without validating existing data.
                    sql = $"ALTER TABLE [{fk.SchemaName}].[{fk.TableName}] CHECK CONSTRAINT [{fk.FkName}]";
                }

                try
                {
                    await conn.ExecuteAsync(sql);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[FK] Warning: Could not enable [{fk.TableName}].[{fk.FkName}]: {ex.Message}");
                }
            }
            Console.WriteLine($"[FK] Restored {states.Count} foreign key constraint(s) to original enabled/NOCHECK state.");
        }

        /// <summary>
        /// Updates child table FK columns from IdMapping (Int/BigInt/Guid) for the given migration batch.
        /// </summary>
        public static async Task UpdateFkFromIdMappingAsync(SqlConnection targetConn, string migrationBatch, int? tenantId)
        {
            if (targetConn.State != System.Data.ConnectionState.Open)
                await targetConn.OpenAsync();

            var fkList = (await targetConn.QueryAsync<FkRow>(@"
                SELECT
                    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS ChildSchema,
                    OBJECT_NAME(fk.parent_object_id) AS ChildTable,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ReferencedSchema,
                    OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable,
                    cChild.name AS ChildColumn,
                    cRef.name AS ReferencedColumn
                FROM sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns cChild ON fkc.parent_object_id = cChild.object_id AND fkc.parent_column_id = cChild.column_id
                INNER JOIN sys.columns cRef ON fkc.referenced_object_id = cRef.object_id AND fkc.referenced_column_id = cRef.column_id
                WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = 'dbo'
                  AND OBJECT_SCHEMA_NAME(fk.referenced_object_id) = 'dbo'")).ToList();

            var distinctChildTables = fkList.Select(f => f.ChildTable).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var pkCache = new Dictionary<string, PkColumnInfo?>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in distinctChildTables)
            {
                pkCache[table] = await DataMigrator.GetPkColumnInfoAsync(targetConn, table);
            }

            var columnTypes = (await targetConn.QueryAsync<(string TableName, string ColumnName, string DataType)>(@"
                SELECT OBJECT_NAME(c.object_id) AS TableName, c.name AS ColumnName, t.name AS DataType
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE OBJECT_SCHEMA_NAME(c.object_id) = 'dbo'")).ToList();
            // Key: "TableName|ColumnName" (case-insensitive)
            var columnTypeMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var row in columnTypes)
                columnTypeMap[$"{row.TableName}|{row.ColumnName}"] = row.DataType;

            foreach (var fk in fkList)
            {
                if (!pkCache.TryGetValue(fk.ChildTable, out var pkInfo) || pkInfo == null) continue;
                if (pkInfo.PkColumnCount > 1)
                    continue; // Child has composite PK: already inserted with NewId, no need to update FK

                var typeKey = $"{fk.ChildTable}|{fk.ChildColumn}";
                if (!columnTypeMap.TryGetValue(typeKey, out var dataType) || string.IsNullOrEmpty(dataType)) continue;

                var typeLower = dataType.ToLowerInvariant();
                string? mappingTable = typeLower switch
                {
                    "int" => "IdMappingInt",
                    "bigint" => "IdMappingBigInt",
                    "uniqueidentifier" => "IdMappingGuid",
                    _ => null
                };

                if (mappingTable == null)
                {
                    Console.WriteLine($"[FK] Skip update {fk.ChildTable}.{fk.ChildColumn} (type {dataType} not mapped).");
                    continue;
                }

                string tenantFilter = tenantId.HasValue
                    ? " AND m.TenantId = @TenantId"
                    : " AND m.TenantId IS NULL";

                var childColEsc = fk.ChildColumn.Replace("]", "]]");
                var childTableEsc = fk.ChildTable.Replace("]", "]]");
                var updateSql = $@"
                    UPDATE c SET c.[{childColEsc}] = m.NewId
                    FROM [dbo].[{childTableEsc}] c
                    INNER JOIN [dbo].[{mappingTable}] m
                        ON m.TableName = @ReferencedTable
                        AND m.ColumnName = @ReferencedColumn
                        AND c.[{childColEsc}] = m.OldId
                    WHERE m.MigrationBatch = @MigrationBatch
                    {tenantFilter}";

                var affected = await targetConn.ExecuteAsync(updateSql, new
                {
                    ReferencedTable = fk.ReferencedTable,
                    ReferencedColumn = fk.ReferencedColumn,
                    MigrationBatch = migrationBatch,
                    TenantId = tenantId
                }, commandTimeout: 600);
                if (affected > 0)
                    Console.WriteLine($"[FK] Updated {fk.ChildTable}.{fk.ChildColumn} -> {fk.ReferencedTable}.{fk.ReferencedColumn}: {affected} row(s).");
            }
        }

        private static async Task<string?> GetColumnDataTypeAsync(SqlConnection conn, string tableName, string columnName)
        {
            var dataType = await conn.QueryFirstOrDefaultAsync<string>(@"
                SELECT t.name AS DataType
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID(@TableName) AND c.name = @ColumnName",
                new { TableName = "dbo." + tableName, ColumnName = columnName });
            return dataType;
        }

        private class FkRow
        {
            public string ChildSchema { get; set; } = "";
            public string ChildTable { get; set; } = "";
            public string ReferencedSchema { get; set; } = "";
            public string ReferencedTable { get; set; } = "";
            public string ChildColumn { get; set; } = "";
            public string ReferencedColumn { get; set; } = "";
        }
    }
}
