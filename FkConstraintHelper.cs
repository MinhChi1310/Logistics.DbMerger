using Microsoft.Data.SqlClient;
using Dapper;
using Serilog;

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
            LastDisabledCount = 0;
            LastEnabledCount = 0;
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
            if (_originalFkStates != null && _originalFkStates.Count > 0)
                Log.Warning("[FK] Warning: DisableAllFkAsync called while FK states already cached ({Count} entries). Previous state will be overwritten.", _originalFkStates.Count);

            _originalFkStates = fks.Select(f => new FkState
            {
                SchemaName = f.SchemaName,
                TableName = f.TableName,
                FkName = f.FkName,
                WasDisabled = f.IsDisabled,
                WasNotTrusted = f.IsNotTrusted
            }).ToList();

            int disabled = 0;
            foreach (var f in fks)
            {
                var schemaEsc = f.SchemaName.Replace("]", "]]");
                var tableEsc = f.TableName.Replace("]", "]]");
                var fkEsc = f.FkName.Replace("]", "]]");
                try
                {
                    await conn.ExecuteAsync($"ALTER TABLE [{schemaEsc}].[{tableEsc}] NOCHECK CONSTRAINT [{fkEsc}]", commandTimeout: 60);
                    disabled++;
                }
                catch (Exception ex)
                {
                    Log.Error("[FK] Could not disable [{TableName}].[{FkName}]: {ErrorMessage}", f.TableName, f.FkName, ex.Message);
                }
            }
            Log.Information("[FK] Disabled {Count} foreign key constraint(s).", disabled);
            LastDisabledCount = disabled;
        }

        /// <summary>Count of FKs disabled in the most recent DisableAllFkAsync call.</summary>
        public static int LastDisabledCount { get; private set; }
        /// <summary>Count of FKs enabled/restored in the most recent EnableAllFkAsync call.</summary>
        public static int LastEnabledCount { get; private set; }

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
                    var sql = $"ALTER TABLE [{schemaName.Replace("]", "]]")}].[{tableName.Replace("]", "]]")}] WITH CHECK CHECK CONSTRAINT [{fkName.Replace("]", "]]")}]";
                    try
                    {
                        await conn.ExecuteAsync(sql);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[FK] Warning: Could not enable [{TableName}].[{FkName}]: {ErrorMessage}", tableName, fkName, ex.Message);
                    }
                }
                Log.Information("[FK] Enabled {Count} foreign key constraint(s).", fks.Count());
                LastEnabledCount = fks.Count();
                return;
            }

            foreach (var fk in states)
            {
                // If FK was originally disabled, keep it disabled (design intent).
                if (fk.WasDisabled)
                    continue;

                string sql;
                var sEsc = fk.SchemaName.Replace("]", "]]");
                var tEsc = fk.TableName.Replace("]", "]]");
                var fEsc = fk.FkName.Replace("]", "]]");
                if (!fk.WasNotTrusted)
                {
                    // Trusted FK: validate existing data.
                    sql = $"ALTER TABLE [{sEsc}].[{tEsc}] WITH CHECK CHECK CONSTRAINT [{fEsc}]";
                }
                else
                {
                    // Untrusted / NOCHECK FK: enable without validating existing data.
                    sql = $"ALTER TABLE [{sEsc}].[{tEsc}] CHECK CONSTRAINT [{fEsc}]";
                }

                try
                {
                    await conn.ExecuteAsync(sql);
                }
                catch (Exception ex)
                {
                    Log.Error("[FK] Warning: Could not enable [{TableName}].[{FkName}]: {ErrorMessage}", fk.TableName, fk.FkName, ex.Message);
                }
            }
            Log.Information("[FK] Restored {Count} foreign key constraint(s) to original enabled/NOCHECK state.", states.Count);
            LastEnabledCount = states.Count;
            _originalFkStates = null; // Clear stale state to prevent re-application on subsequent runs
        }

        /// <summary>
        /// Updates child table FK columns from IdMapping (Int/BigInt/Guid) for the given migration batch.
        /// Scopes updates to the target tenant's rows to prevent cross-tenant data corruption.
        /// </summary>
        public static async Task UpdateFkFromIdMappingAsync(SqlConnection targetConn, string migrationBatch, int? tenantId, int? targetTenantId)
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

            // Task 1: Build a cache of which child tables have a TenantId column
            var tablesWithTenantId = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var tenantIdCheck = (await targetConn.QueryAsync<string>(@"
                SELECT DISTINCT OBJECT_NAME(c.object_id)
                FROM sys.columns c
                WHERE c.name = 'TenantId'
                  AND OBJECT_SCHEMA_NAME(c.object_id) = 'dbo'
                  AND OBJECTPROPERTY(c.object_id, 'IsUserTable') = 1")).ToList();
            foreach (var t in tenantIdCheck)
                tablesWithTenantId.Add(t);

            foreach (var fk in fkList)
            {
                if (!pkCache.TryGetValue(fk.ChildTable, out var pkInfo) || pkInfo == null)
                {
                    Log.Warning("[FK] Skipping FK remapping for {ChildTable}.{ChildColumn} -> {ReferencedTable}.{ReferencedColumn}: child table has no PK info", fk.ChildTable, fk.ChildColumn, fk.ReferencedTable, fk.ReferencedColumn);
                    continue;
                }
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
                    Log.Information("[FK] Skip update {ChildTable}.{ChildColumn} (type {DataType} not mapped).", fk.ChildTable, fk.ChildColumn, dataType);
                    continue;
                }

                // IdMapping tenant filter (scopes which mapping rows to use)
                // Include TenantId IS NULL to also match global table mappings (Editions, AllowableAbsence, SubThreadType)
                string tenantFilter = tenantId.HasValue
                    ? " AND (m.TenantId = @TenantId OR m.TenantId IS NULL)"
                    : " AND m.TenantId IS NULL";

                // Task 2: Child table tenant filter (prevents cross-tenant data corruption)
                bool childHasTenantId = tablesWithTenantId.Contains(fk.ChildTable);
                string childTenantFilter = "";
                if (childHasTenantId)
                {
                    if (targetTenantId.HasValue)
                    {
                        childTenantFilter = " AND c.TenantId = @TargetTenantId";
                    }
                    else
                    {
                        // Global migration (no specific tenant): only update host-level rows (TenantId IS NULL)
                        // to prevent accidentally modifying all tenants' data
                        childTenantFilter = " AND c.TenantId IS NULL";
                        Log.Warning("[FK] Child table {ChildTable} has TenantId but targetTenantId is NULL — restricting to host rows only", fk.ChildTable);
                    }
                }

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
                    {tenantFilter}
                    {childTenantFilter}";

                var affected = await targetConn.ExecuteAsync(updateSql, new
                {
                    ReferencedTable = fk.ReferencedTable,
                    ReferencedColumn = fk.ReferencedColumn,
                    MigrationBatch = migrationBatch,
                    TenantId = tenantId,
                    TargetTenantId = targetTenantId
                }, commandTimeout: 600);
                if (affected > 0)
                {
                    Log.Information("[FK] Updated {ChildTable}.{ChildColumn} -> {ReferencedTable}.{ReferencedColumn}: {Affected} row(s).", fk.ChildTable, fk.ChildColumn, fk.ReferencedTable, fk.ReferencedColumn, affected);
                    ReportWriter.AddFkUpdate(fk.ChildTable, fk.ChildColumn, fk.ReferencedTable, fk.ReferencedColumn, affected);
                }
            }
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
