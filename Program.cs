using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Dapper;

namespace Logistics.DbMerger
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== Logistics DB Merger Tool ===");

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfiguration config = builder.Build();

            string sourceConn = config.GetConnectionString("SourceMdc");
            string targetConn = config.GetConnectionString("TargetAdc");
            bool dryRun = config.GetValue<bool>("Settings:DryRun");
            int batchSize = config.GetValue<int>("Settings:BatchSize");
            int mergeChunkSize = config.GetValue<int>("Settings:MergeChunkSize");
            var veryHighTimeoutTables = config.GetSection("Settings:VeryHighTimeoutTables").Get<string[]>() ?? Array.Empty<string>();
            int veryHighTimeoutSeconds = config.GetValue<int>("Settings:VeryHighTimeoutSeconds");
            var highTimeoutTables = config.GetSection("Settings:HighTimeoutTables").Get<string[]>() ?? Array.Empty<string>();
            int highTimeoutSeconds = config.GetValue<int>("Settings:HighTimeoutSeconds");

            if (string.IsNullOrEmpty(sourceConn) || string.IsNullOrEmpty(targetConn) || sourceConn.Contains("YOUR_"))
            {
                Console.WriteLine("[Error] Please configure valid connection strings in appsettings.json");
                return;
            }

            // Command Line Args for automation (bypass menu if detected)
            if (args.Length > 0 && args.Any(a => a.StartsWith("--tenant") || a.StartsWith("--mode")))
            {
                // Fallback to old automated behavior or implement --mode=schema etc.
                // For now, let's keep it simple: if args exist, run Step 3 (Data) assuming Schema is done?
                // Or just standard run. Let's redirect to standard full run if --tenant present.
                Console.WriteLine("[Auto-Run] Arguments detected. Running full migration...");
                await RunFullMigration(sourceConn, targetConn, batchSize, dryRun, args, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                return;
            }

            while (true)
            {
                Console.WriteLine("\n[Main Menu]");
                Console.WriteLine("1. Sync Schema (Tables & Columns)");
                Console.WriteLine("2. Sync Objects (Procedures, Views, Functions)");
                Console.WriteLine("3. Sync Data (Smart Merge & Tenant Filter)");
                Console.WriteLine("4. Validate / Verify");
                Console.WriteLine("5. Rollback Last Action");
                Console.WriteLine("6. Exit");
                Console.WriteLine("7. List tables only in MDC (console + file) and create structure in ADC");
                Console.WriteLine("8. Clear migration data (delete rows in ADC based on IdMapping)");
                Console.WriteLine("9. Sync Data by Tier (Tier -> Tenant)");
                Console.WriteLine("10. Enable FK (re-enable all foreign keys on target)");
                Console.Write("Select an option: ");
                
                var key = Console.ReadLine();
                try
                {
                    switch (key)
                    {
                        case "1":
                            await RunSchemaSync(sourceConn, targetConn, dryRun);
                            break;
                        case "2":
                            await RunObjectSync(sourceConn, targetConn, dryRun);
                            break;
                        case "3":
                            await RunDataSync(sourceConn, targetConn, batchSize, dryRun, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                            break;
                        case "4":
                            await RunValidation(sourceConn, targetConn);
                            break;
                        case "5":
                            await RunRollback(targetConn);
                            break;
                        case "6":
                            return;
                        case "7":
                            await RunListTablesOnlyInMdcAndCreateStructureAsync(sourceConn, targetConn, dryRun);
                            break;
                        case "8":
                            await RunClearMigrationDataAsync(targetConn);
                            break;
                        case "9":
                            await RunDataSyncByTier(sourceConn, targetConn, batchSize, dryRun, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                            break;
                        case "10":
                            await RunEnableFkAsync(targetConn);
                            break;
                        default:
                            Console.WriteLine("Invalid selection.");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Fatal Error] {ex.Message}");
                    Console.WriteLine(ex.StackTrace);
                }
            }
        }

        static async Task RunSchemaSync(string sourceConn, string targetConn, bool dryRun)
        {
            Console.WriteLine("\n--> [Step 1] Schema Sync");
            RollbackLogger.Initialize("schema");
            var schemaSync = new SchemaSync(sourceConn, targetConn);

            // Get "tables only in MDC" before any schema change to ADC; Option 3/9 will read from this file only
            Console.WriteLine("Writing tables only in MDC (before schema change)...");
            await Helper.GetTablesOnlyInMdcAsync(sourceConn, targetConn, Helper.MdcOnlyTablesFilePath, writeToFile: true);
            Console.WriteLine($"Saved to {Helper.MdcOnlyTablesFilePath}");

            Console.WriteLine("Checking missing tables...");
            var missingTables = await schemaSync.GetMissingTablesAsync();
            Console.WriteLine($"Found {missingTables.Count} missing tables.");

            if (!dryRun)
            {
                foreach (var table in missingTables)
                {
                    // Skip if explicitly mapped (we map to existing table instead of creating new one with source name)
                    if (ExplicitTableMappings.ContainsKey(table))
                    {
                        Console.WriteLine($"[Schema] Skipping creation of '{table}' (Explicitly mapped to '{ExplicitTableMappings[table]}')");
                        continue;
                    }
                    await schemaSync.SyncTableAsync(table);
                }
            }
            else
            {
                Console.WriteLine("[DryRun] Skipping table creation.");
            }

            // Sync Missing Columns for EXISTING Common Tables
            Console.WriteLine("\nChecking for missing columns in common tables...");
            var sourceTables = await schemaSync.GetExistingSourceTablesAsync();
            var targetTables = await schemaSync.GetExistingTargetTablesAsync();
            
            // Identify common tables (by exact name)
            var commonTables = sourceTables.Intersect(targetTables, StringComparer.OrdinalIgnoreCase).ToList();
            Console.WriteLine($"Found {commonTables.Count} common tables. Scanning columns...");

            foreach (var table in commonTables)
            {
                if (table == "sysdiagrams" ||  table.StartsWith("__")) continue;// delete table == "Tenants" ||

                // This adds missing columns if any
                await schemaSync.SyncTableSchemaAsync(table, table, dryRun);
            }

            // Sync Missing Columns for EXPLICIT MAPPED Tables (that are technically "missing" via commonTables logic)
            Console.WriteLine("Checking explicit mappings...");
            foreach (var kvp in ExplicitTableMappings)
            {
                var sourceTable = kvp.Key;
                var targetTable = kvp.Value;

                if (sourceTables.Contains(sourceTable, StringComparer.OrdinalIgnoreCase) && 
                    targetTables.Contains(targetTable, StringComparer.OrdinalIgnoreCase))
                {
                     Console.WriteLine($"[Mapping] Checking {sourceTable} -> {targetTable}...");
                     await schemaSync.SyncTableSchemaAsync(sourceTable, targetTable, dryRun);
                }
            }

            // After all tables and columns are synced, check constraints across all tables
            Console.WriteLine("\nSyncing constraints across all tables...");
            await schemaSync.SyncAllConstraintsAsync(ExplicitTableMappings, dryRun);
        }

        static async Task RunObjectSync(string sourceConn, string targetConn, bool dryRun)
        {
            Console.WriteLine("\n--> [Step 2] Object Sync");
            RollbackLogger.Initialize("objects");
            var objectSync = new ObjectSync(sourceConn, targetConn);
            await objectSync.SyncObjectsAsync(dryRun);
        }

        static async Task RunDataSync(string sourceConn, string targetConn, int batchSize, bool dryRun, int mergeChunkSize = 0,
            string[]? veryHighTimeoutTables = null, int veryHighTimeoutSeconds = 0,
            string[]? highTimeoutTables = null, int highTimeoutSeconds = 0)
        {
            Console.WriteLine("\n--> [Step 3] Data Sync");
            // Data Step doesn't necessarily create objects to rollback (except fuzzy match columns?)
            // Fuzzy match columns use RollbackLogger internally.
            // So we should init context "data_schema" perhaps?
            RollbackLogger.Initialize("data_schema");

            // Interactive Tenant Prompt
            Console.Write("\n[Input] Enter Tenant Name to filter by (or press Enter for ALL): ");
            string tenantName = Console.ReadLine()?.Trim();
            int? sourceTenantId = null;
            int? targetTenantId = null;
            // When ALL: list of (SourceTenantId, TargetTenantId, DisplayName)
            List<(int SourceId, int TargetId, string DisplayName)>? allTenantPairs = null;

            using (var source = new SqlConnection(sourceConn))
            using (var target = new SqlConnection(targetConn))
            {
                await source.OpenAsync();
                await target.OpenAsync();

                if (!string.IsNullOrEmpty(tenantName))
                {
                    // 1. Resolve Source Tenant
                    sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (sourceTenantId == null)
                        sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });
                    
                    if (sourceTenantId == null)
                    {
                        Console.WriteLine($"[Error] Source Tenant '{tenantName}' not found.");
                        return;
                    }
                    Console.WriteLine($"[TenantFilter] Resolved Source ID: {sourceTenantId}");

                    // 2. Resolve/Create Target Tenant
                    var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (existingTargetId == null)
                        existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                    if (existingTargetId != null)
                    {
                        targetTenantId = existingTargetId;
                        Console.WriteLine($"[TenantMap] Found existing Target Tenant '{tenantName}' (ID: {targetTenantId}). Merging into it.");
                    }
                    else
                    {
                        if (!dryRun)
                        {
                            Console.WriteLine($"[TenantMap] Creating new Tenant '{tenantName}' in Target...");
                            var sourceTenantRow = await source.QuerySingleAsync<dynamic>("SELECT * FROM Tenants WHERE Id = @Id", new { Id = sourceTenantId });
                            var props = (IDictionary<string, object>)sourceTenantRow;
                            var cols = props.Keys.Where(k => k != "Id").ToList();
                            var vals = cols.Select(k => "@" + k).ToList();
                            string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                            targetTenantId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                            Console.WriteLine($"[TenantMap] Created Target Tenant. New ID: {targetTenantId}");
                        }
                        else
                        {
                            Console.WriteLine($"[DryRun] Would Create Tenant '{tenantName}' in Target.");
                            targetTenantId = sourceTenantId;
                        }
                    }
                    if (targetTenantId == null && dryRun) targetTenantId = sourceTenantId;
                }
                else
                {
                    // ALL tenants: resolve/create each tenant in target
                    var sourceTenants = (await source.QueryAsync<(int Id, string? Name, string? TenancyName)>(
                        "SELECT Id, Name, TenancyName FROM Tenants ORDER BY Id")).ToList();
                    if (sourceTenants.Count == 0)
                    {
                        Console.WriteLine("[DataSync] No tenants found in Source.");
                        return;
                    }
                    Console.WriteLine($"[TenantFilter] ALL tenants: {sourceTenants.Count} tenant(s) in Source.");
                    allTenantPairs = new List<(int, int, string)>();
                    foreach (var row in sourceTenants)
                    {
                        string nameForLookup = (row.TenancyName ?? row.Name ?? row.Id.ToString()) ?? "";
                        var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = nameForLookup });
                        if (existingTargetId == null)
                            existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = nameForLookup });
                        int targetId;
                        if (existingTargetId != null)
                        {
                            targetId = existingTargetId.Value;
                            Console.WriteLine($"[TenantMap] '{nameForLookup}' Source ID {row.Id} -> Target ID {targetId} (existing).");
                        }
                        else
                        {
                            if (!dryRun)
                            {
                                var sourceTenantRow = await source.QuerySingleAsync<dynamic>("SELECT * FROM Tenants WHERE Id = @Id", new { Id = row.Id });
                                var props = (IDictionary<string, object>)sourceTenantRow;
                                var cols = props.Keys.Where(k => k != "Id").ToList();
                                var vals = cols.Select(k => "@" + k).ToList();
                                string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                                targetId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                                Console.WriteLine($"[TenantMap] '{nameForLookup}' Source ID {row.Id} -> Created Target ID {targetId}.");
                            }
                            else
                            {
                                targetId = row.Id;
                                Console.WriteLine($"[DryRun] Would create Tenant '{nameForLookup}' in Target (mock TargetId: {targetId}).");
                            }
                        }
                        allTenantPairs.Add((row.Id, targetId, nameForLookup));
                    }
                }
            }

            var schemaSync = new SchemaSync(sourceConn, targetConn);
            var migrator = new DataMigrator(sourceConn, targetConn, batchSize);

            // Bước 0: Tables only in MDC – chỉ đọc từ file (đã ghi ở Option 1 trước khi thay đổi cấu trúc ADC)
            var fromFile = await Helper.ReadTableListFromNumberedFileAsync(Helper.MdcOnlyTablesFilePath);
            var tablesOnlyInMdc = fromFile.Count > 0
                ? fromFile.ToHashSet(StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (fromFile.Count > 0)
                Console.WriteLine($"[DataSync] Tables only in MDC: {tablesOnlyInMdc.Count} (from file {Helper.MdcOnlyTablesFilePath})");
            else
                Console.WriteLine($"[DataSync] Tables only in MDC: 0 (file empty or missing; run Option 1 first to generate {Helper.MdcOnlyTablesFilePath})");
            
            // 2b. Smart User Sync (Before generic tables)
            var userMapping = new Dictionary<long, long>();
            if (allTenantPairs != null)
            {
                foreach (var (srcId, tgtId, displayName) in allTenantPairs)
                {
                    Console.WriteLine($"[Users] Syncing users for tenant: {displayName} (Source: {srcId} -> Target: {tgtId})");
                    await SyncUsersAsync(sourceConn, targetConn, userMapping, srcId, tgtId, dryRun);
                }
            }
            else
            {
                await SyncUsersAsync(sourceConn, targetConn, userMapping, sourceTenantId, targetTenantId, dryRun);
            }

            // Caching target tables is good for fuzzy matching.
            var existingAdcTables = (await schemaSync.GetExistingTargetTablesAsync()).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var sourceTables = (await schemaSync.GetExistingSourceTablesAsync())
                .Where(t => !TableSkipRules.ShouldSkipTable(t))
                .ToList();

            // Match Fuzzy / Explicit
            // We need to iterate specifically in ORDER defined by MigrationConfig
            // Any table NOT in MigrationConfig will be processed AFTER.
            
            var orderedTables = new List<string>();
            var sourceTableSet = new HashSet<string>(sourceTables, StringComparer.OrdinalIgnoreCase);

            // 1. Add Ordered Tables if they exist in Source
            foreach(var t in MigrationConfig.TableOrder)
            {
                if (sourceTableSet.Contains(t))
                {
                    orderedTables.Add(t);
                    sourceTableSet.Remove(t); // Handled
                }
            }
            
            // 2. Add Remaining Tables (that were not in the config list)
            foreach(var t in sourceTableSet)
            {
                orderedTables.Add(t);
            }
            
            Console.WriteLine($"[DataSync] Tables to Migrate (Ordered): {orderedTables.Count}");

            if (!dryRun)
            {
                using var sourceConnection = new SqlConnection(sourceConn);
                using var targetConnection = new SqlConnection(targetConn);
                await sourceConnection.OpenAsync();
                await targetConnection.OpenAsync();
                await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(targetConnection);
                await DataSyncCheckpointHelper.EnsureTableAsync(targetConnection);
                await FkConstraintHelper.DisableAllFkAsync(targetConnection);
                // When running ALL tenants, clear completed-tenant file so single-tenant tracking stays separate
                if (allTenantPairs != null)
                {
                    var completedPath = Helper.DataSyncCompletedTenantIdsFilePath;
                    if (File.Exists(completedPath)) File.Delete(completedPath);
                }
                try
                {
                var migrationBatch = Guid.NewGuid().ToString("N");
                var pkInfoCache = new Dictionary<string, PkColumnInfo?>(StringComparer.OrdinalIgnoreCase);
                var tableHasTenantIdTargetCache = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                var tableHasTenantIdSourceCache = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

                async Task<bool> GetTargetTableHasTenantIdAsync(string name)
                {
                    if (tableHasTenantIdTargetCache.TryGetValue(name, out var v)) return v;
                    var r = await DataMigrator.TableHasTenantIdColumnAsync(targetConnection, name);
                    tableHasTenantIdTargetCache[name] = r;
                    return r;
                }
                async Task<bool> GetSourceTableHasTenantIdAsync(string name)
                {
                    if (tableHasTenantIdSourceCache.TryGetValue(name, out var v)) return v;
                    var r = await DataMigrator.TableHasTenantIdColumnAsync(sourceConnection, name);
                    tableHasTenantIdSourceCache[name] = r;
                    return r;
                }

                var tenantsToRun = allTenantPairs != null
                    ? allTenantPairs
                    : new List<(int SourceId, int TargetId, string DisplayName)> { (sourceTenantId!.Value, targetTenantId!.Value, tenantName ?? "Single") };

                foreach (var (curSourceId, curTargetId, curDisplayName) in tenantsToRun)
                {
                    int? src = curSourceId;
                    int? tgt = curTargetId;
                    if (allTenantPairs != null)
                        Console.WriteLine($"[DataSync] --- Tenant: {curDisplayName} (SourceId: {curSourceId} -> TargetId: {curTargetId}) ---");

                    foreach (var table in orderedTables)
                    {
                        if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__")) continue;

                        var isNew = !existingAdcTables.Contains(table, StringComparer.OrdinalIgnoreCase);
                        string targetTable = table;

                        if (ExplicitTableMappings.ContainsKey(table))
                        {
                            string mappedTarget = ExplicitTableMappings[table];
                            targetTable = mappedTarget;
                            if (existingAdcTables.Contains(mappedTarget, StringComparer.OrdinalIgnoreCase))
                            {
                                targetTable = mappedTarget;
                                isNew = false;
                                Console.WriteLine($"[SmartMerge] Applied explicit mapping: {table} (MDC) -> {targetTable} (ADC)");
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                            else
                                Console.WriteLine($"[Map] Explicit target {targetTable} missing. Treating as new.");
                        }
                        else if (isNew)
                        {
                            string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                            if (bestMatch != null)
                            {
                                targetTable = bestMatch;
                                isNew = false;
                                Console.WriteLine($"[SmartMerge] Detected match: {table} (MDC) -> {targetTable} (ADC)");
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                        }

                        if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);

                        if (!pkInfoCache.TryGetValue(targetTable, out var pkInfo))
                        {
                            pkInfo = await DataMigrator.GetPkColumnInfoAsync(targetConnection, targetTable);
                            pkInfoCache[targetTable] = pkInfo;
                        }

                        // Checkpoint / Resume: skip tables already completed for this tenant
                        if (src.HasValue && tgt.HasValue)
                        {
                            if (await DataSyncCheckpointHelper.IsTableDoneAsync(targetConnection, src.Value, tgt.Value, targetTable))
                            {
                                Console.WriteLine($"[Checkpoint] Skipping table '{targetTable}' for tenant {src}->{tgt} (already completed).");
                                continue;
                            }
                        }

                        bool skipGlobalSinglePk = false;
                        if (pkInfo != null && pkInfo.PkColumnCount == 1 && !await GetTargetTableHasTenantIdAsync(targetTable))
                        {
                            // Bảng không có TenantId, PK 1 cột: chỉ seed một lần (tenant chạy đầu tiên), các tenant sau skip.
                            string? mappingTable = pkInfo.DataType switch
                            {
                                "int" => "IdMappingInt",
                                "bigint" => "IdMappingBigInt",
                                "uniqueidentifier" => "IdMappingGuid",
                                _ => null
                            };

                            if (mappingTable != null)
                            {
                                var existingMappings = await targetConnection.ExecuteScalarAsync<int>(
                                    $"SELECT COUNT(1) FROM [dbo].[{mappingTable}] WHERE TableName = @TableName AND ColumnName = @ColumnName",
                                    new { TableName = targetTable, ColumnName = pkInfo.ColumnName });
                                skipGlobalSinglePk = existingMappings > 0;
                            }
                            else
                            {
                                var tableEsc = targetTable.Replace("]", "]]");
                                var existingRows = await targetConnection.ExecuteScalarAsync<int>(
                                    $"SELECT COUNT(1) FROM [dbo].[{tableEsc}]");
                                skipGlobalSinglePk = existingRows > 0;
                            }

                            if (skipGlobalSinglePk)
                            {
                                Console.WriteLine($"[DataSync] Skipping global single-PK table '{targetTable}' (no TenantId, already seeded).");
                                continue;
                            }
                        }

                        if (tablesOnlyInMdc.Contains(targetTable))
                        {
                            Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: MDC-only (copy from MDC, no delete) | TenantId: {(tgt.HasValue ? tgt.ToString() : "all")}");
                            await migrator.MigrateTableAsync(table, isNewTable: false, targetTableName: targetTable, sourceTenantId: src, targetTenantId: tgt, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                            if (pkInfo != null && (pkInfo.DataType == "int" || pkInfo.DataType == "bigint" || pkInfo.DataType == "uniqueidentifier"))
                            {
                                var mappingTable = pkInfo.DataType == "int" ? "IdMappingInt" : pkInfo.DataType == "bigint" ? "IdMappingBigInt" : "IdMappingGuid";
                                var targetWhere = (tgt.HasValue && await GetTargetTableHasTenantIdAsync(targetTable)) ? $" WHERE TenantId = {tgt.Value}" : "";
                                var pkColEsc = pkInfo.ColumnName.Replace("]", "]]");
                                var tableEsc = targetTable.Replace("]", "]]");
                                var bulkSql = $@"
INSERT INTO [dbo].[{mappingTable}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)
SELECT @TableName, @ColumnName, [{pkColEsc}], [{pkColEsc}], @Batch, @TenantId FROM [dbo].[{tableEsc}]{targetWhere}";
                                var inserted = await targetConnection.ExecuteAsync(bulkSql,
                                    new { TableName = targetTable, ColumnName = pkInfo.ColumnName, Batch = migrationBatch, TenantId = (int?)tgt },
                                    commandTimeout: 600);
                                if (inserted > 0) Console.WriteLine($"   -> IdMapping (MDC-only, bulk): {inserted} row(s) -> [dbo].[{mappingTable}]");
                            }
                            Console.WriteLine($"   -> Done: {table}");
                        }
                        else
                        {
                            if (pkInfo == null)
                            {
                                Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Direct MigrateTable (no PK/IdMapping) | TenantId: {(src.HasValue ? src.ToString() : "all")}");
                                await migrator.MigrateTableAsync(table, isNewTable: isNew, targetTableName: targetTable, sourceTenantId: src, targetTenantId: tgt, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                                Console.WriteLine($"   -> Done: {table}");
                            }
                            else if (pkInfo.PkColumnCount == 1 && pkInfo.DataType != "int" && pkInfo.DataType != "bigint" && pkInfo.DataType != "uniqueidentifier")
                            {
                                Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Natural PK (insert missing only) | TenantId: {(src.HasValue ? src.ToString() : "all")}");
                                await migrator.MigrateTableNaturalPkAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, src, tgt, userMapping);
                                Console.WriteLine($"   -> Done: {table}");
                            }
                            else if (pkInfo.PkColumnCount > 1)
                            {
                                Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Composite PK (staging -> INSERT with IdMapping JOIN) | TenantId: {(src.HasValue ? src.ToString() : "all")}");
                                var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                                var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                                await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable, pkColumnNames, fkColumns, src, tgt);
                                Console.WriteLine($"   -> Done: {table}");
                            }
                            else
                            {
                                Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Staging + MERGE + IdMapping (single PK int/bigint/guid) | TenantId: {(tgt.HasValue ? tgt.ToString() : "null")}");
                                var whereClause = (src.HasValue && await GetSourceTableHasTenantIdAsync(table)) ? $" WHERE TenantId = {src.Value}" : "";
                                await migrator.CreateStagingTableAsync(sourceConnection, targetConnection, table, targetTable, pkInfo);
                                int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                                if (commandTimeoutOverride.HasValue)
                                    Console.WriteLine($"   -> Using extended timeout: {commandTimeoutOverride.Value}s for this table.");
                                await migrator.InsertTableWithIdMappingAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, migrationBatch, tgt, whereClause, src, tgt, userMapping, mergeChunkSize, commandTimeoutOverride);
                                Console.WriteLine($"   -> Done: {table}");
                            }
                        }

                        // Mark checkpoint after successful completion for this tenant + table
                        if (src.HasValue && tgt.HasValue)
                        {
                            await DataSyncCheckpointHelper.MarkTableDoneAsync(targetConnection, src.Value, tgt.Value, targetTable);
                        }
                    }

                await FkConstraintHelper.UpdateFkFromIdMappingAsync(targetConnection, migrationBatch, tgt);
                }
                }
                finally
                {
                    if (allTenantPairs != null)
                    {
                        await FkConstraintHelper.EnableAllFkAsync(targetConnection);
                        Console.WriteLine("[DataSync] Re-enabled all foreign keys.");
                    }
                    else
                    {
                        // Single-tenant run: persist completed tenant ID, then enable FK only when all tenants are done
                        var completedPath = Helper.DataSyncCompletedTenantIdsFilePath;
                        var dir = Path.GetDirectoryName(completedPath);
                        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);
                        var existingIds = new HashSet<int>();
                        if (File.Exists(completedPath))
                        {
                            foreach (var line in await File.ReadAllLinesAsync(completedPath))
                                if (int.TryParse(line.Trim(), out var id)) existingIds.Add(id);
                        }
                        if (sourceTenantId.HasValue) existingIds.Add(sourceTenantId.Value);
                        await File.WriteAllLinesAsync(completedPath, existingIds.OrderBy(x => x).Select(x => x.ToString()));

                        var allSourceIds = (await sourceConnection.QueryAsync<int>("SELECT Id FROM Tenants")).ToHashSet();
                        if (existingIds.SetEquals(allSourceIds))
                        {
                            await FkConstraintHelper.EnableAllFkAsync(targetConnection);
                            Console.WriteLine("[DataSync] All tenants completed. Re-enabled all foreign keys.");
                            if (File.Exists(completedPath)) File.Delete(completedPath);
                        }
                        else
                            Console.WriteLine($"[DataSync] Single-tenant run completed. Completed: {existingIds.Count}, Total source tenants: {allSourceIds.Count}. FK left disabled until all tenants are synced.");
                    }
                }
            }
            else
            {
                var dryRunTenants = allTenantPairs != null
                    ? allTenantPairs
                    : new List<(int SourceId, int TargetId, string DisplayName)> { (sourceTenantId!.Value, targetTenantId!.Value, tenantName ?? "Single") };
                foreach (var (curSourceId, curTargetId, curDisplayName) in dryRunTenants)
                {
                    if (allTenantPairs != null)
                        Console.WriteLine($"[DryRun] --- Tenant: {curDisplayName} (SourceId: {curSourceId} -> TargetId: {curTargetId}) ---");
                    foreach (var table in orderedTables)
                    {
                        if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__")) continue;

                        var isNew = !existingAdcTables.Contains(table, StringComparer.OrdinalIgnoreCase);
                        string targetTable = table;

                        if (ExplicitTableMappings.ContainsKey(table))
                        {
                            string mappedTarget = ExplicitTableMappings[table];
                            targetTable = mappedTarget;
                            if (existingAdcTables.Contains(mappedTarget, StringComparer.OrdinalIgnoreCase))
                            {
                                targetTable = mappedTarget;
                                isNew = false;
                                Console.WriteLine($"[SmartMerge] Applied explicit mapping: {table} (MDC) -> {targetTable} (ADC)");
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                            else
                                Console.WriteLine($"[Map] Explicit target {targetTable} missing. Treating as new.");
                        }
                        else if (isNew)
                        {
                            string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                            if (bestMatch != null)
                            {
                                targetTable = bestMatch;
                                isNew = false;
                                Console.WriteLine($"[SmartMerge] Detected match: {table} (MDC) -> {targetTable} (ADC)");
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                        }

                        if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);

                        Console.WriteLine($"[DryRun] Would migrate {table} -> {targetTable} (Tenant: {curSourceId} -> {curTargetId})");
                    }
                }
            }
        }

        static async Task RunListTablesOnlyInMdcAndCreateStructureAsync(string sourceConn, string targetConn, bool dryRun)
        {
            Console.WriteLine("\n--> [Option 7] Tables only in MDC: list (console + file) and create structure in ADC");
            var path = Helper.MdcOnlyTablesFilePath;
            var list = await Helper.GetTablesOnlyInMdcAsync(sourceConn, targetConn, path, writeToFile: true);
            Console.WriteLine($"Tables only in MDC (after skip rules): {list.Count}");
            Console.WriteLine($"Output file: {Path.GetFullPath(path)}");
            for (int i = 0; i < list.Count; i++)
                Console.WriteLine($"  {i + 1}. {list[i]}");
            if (list.Count == 0)
            {
                Console.WriteLine("Nothing to create.");
                return;
            }
            if (dryRun)
            {
                Console.WriteLine("[DryRun] Would create table structure in ADC for the above.");
                return;
            }
            var schemaSync = new SchemaSync(sourceConn, targetConn);
            foreach (var table in list)
            {
                if (ExplicitTableMappings.ContainsKey(table))
                {
                    Console.WriteLine($"[Skip] {table} (explicitly mapped to existing table)");
                    continue;
                }
                await schemaSync.SyncTableAsync(table);
                Console.WriteLine($"  Created [dbo].[{table}]");
            }
            Console.WriteLine("Done.");
        }

        /// <summary>
        /// Xóa data migration trên ADC dựa vào IdMapping (chỉ xóa các dòng có NewId trong IdMapping).
        /// Tận dụng index (TableName, ColumnName) INCLUDE (NewId) trên bảng IdMapping.
        /// </summary>
        static async Task RunClearMigrationDataAsync(string targetConnStr)
        {
            Console.WriteLine("\n--> [Option 8] Clear migration data (delete rows in ADC based on IdMapping)");
            Console.Write("[Optional] MigrationBatch (Enter = all batches, or paste batch ID): ");
            var batchInput = Console.ReadLine()?.Trim();
            Console.Write("[Optional] TenantId (Enter = all tenants, or number): ");
            var tenantInput = Console.ReadLine()?.Trim();
            int? filterTenantId = null;
            if (!string.IsNullOrEmpty(tenantInput) && int.TryParse(tenantInput, out int tid))
                filterTenantId = tid;

            await using var conn = new SqlConnection(targetConnStr);
            await conn.OpenAsync();
            await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(conn);
            await FkConstraintHelper.DisableAllFkAsync(conn);

            string? filterBatch = string.IsNullOrEmpty(batchInput) ? null : batchInput;
            var totalDeleted = 0;

            totalDeleted = await ProcessIdMappingTableAsync(conn, "IdMappingInt", filterBatch, filterTenantId);
            totalDeleted += await ProcessIdMappingTableAsync(conn, "IdMappingBigInt", filterBatch, filterTenantId);
            totalDeleted += await ProcessIdMappingTableAsync(conn, "IdMappingGuid", filterBatch, filterTenantId);

            await FkConstraintHelper.EnableAllFkAsync(conn);
            Console.WriteLine($"\n[Option 8] Done. Total rows deleted from data tables: {totalDeleted}.");

            // Clear DataSync checkpoints so that subsequent Option 3 runs start fresh and do not skip tables
            await DataSyncCheckpointHelper.ClearAllAsync(conn);
            Console.WriteLine("[Option 8] Cleared DataSyncCheckpoint table to keep checkpoints in sync with cleared data.");
        }

        static async Task RunEnableFkAsync(string targetConnStr)
        {
            Console.WriteLine("\n--> [Option 10] Enable FK (re-enable all foreign keys on target)");
            using var conn = new SqlConnection(targetConnStr);
            await conn.OpenAsync();
            await FkConstraintHelper.EnableAllFkAsync(conn);
            Console.WriteLine("[Option 10] Done. All foreign keys on target have been re-enabled.");
        }

        /// <summary>
        /// For one IdMapping table: get distinct (TableName, ColumnName), delete from data tables by NewId (in batches), then delete from IdMapping.
        /// Uses extended timeout (3600s) for large IdMapping / data tables.
        /// </summary>
        const int Option8CommandTimeoutSeconds = 3600;

        static async Task<int> ProcessIdMappingTableAsync(SqlConnection conn, string mappingTable, string? filterBatch, int? filterTenantId)
        {
            var tableList = await GetDistinctTableColumnFromIdMappingAsync(conn, mappingTable, filterBatch, filterTenantId);
            if (tableList.Count == 0) return 0;

            var dboTables = (await conn.QueryAsync<string>("SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo')"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            const int deleteBatchSize = 50000;
            int totalDeleted = 0;
            foreach (var (tableName, columnName) in tableList)
            {
                if (!dboTables.Contains(tableName))
                {
                    Console.WriteLine($"  [Skip] Table not found: {tableName}");
                    continue;
                }

                var tableEsc = tableName.Replace("]", "]]");
                var colEsc = columnName.Replace("]", "]]");

                var subWhere = " TableName = @TableName AND ColumnName = @ColumnName";
                if (!string.IsNullOrEmpty(filterBatch)) subWhere += " AND MigrationBatch = @Batch";
                if (filterTenantId.HasValue) subWhere += " AND TenantId = @TenantId";

                var prm = new { TableName = tableName, ColumnName = columnName, Batch = filterBatch, TenantId = filterTenantId, BatchSize = deleteBatchSize };
                var deleteDataSql = $@"DELETE TOP (@BatchSize) FROM [dbo].[{tableEsc}] WHERE [{colEsc}] IN (SELECT NewId FROM [dbo].[{mappingTable}] WHERE{subWhere})";

                int tableDeleted = 0;
                int deleted;
                do
                {
                    deleted = await conn.ExecuteAsync(deleteDataSql, prm, commandTimeout: Option8CommandTimeoutSeconds);
                    tableDeleted += deleted;
                } while (deleted == deleteBatchSize);

                if (tableDeleted > 0)
                {
                    Console.WriteLine($"  Deleted {tableDeleted} row(s) from [dbo].[{tableName}]");
                    totalDeleted += tableDeleted;
                }

                var deleteMappingSql = $@"DELETE FROM [dbo].[{mappingTable}] WHERE TableName = @TableName AND ColumnName = @ColumnName";
                if (!string.IsNullOrEmpty(filterBatch)) deleteMappingSql += " AND MigrationBatch = @Batch";
                if (filterTenantId.HasValue) deleteMappingSql += " AND TenantId = @TenantId";
                await conn.ExecuteAsync(deleteMappingSql, prm, commandTimeout: Option8CommandTimeoutSeconds);
            }
            return totalDeleted;
        }

        static async Task<List<(string TableName, string ColumnName)>> GetDistinctTableColumnFromIdMappingAsync(SqlConnection conn, string mappingTable, string? filterBatch, int? filterTenantId)
        {
            var sql = $"SELECT DISTINCT TableName, ColumnName FROM [dbo].[{mappingTable}] WHERE 1=1";
            if (!string.IsNullOrEmpty(filterBatch)) sql += " AND MigrationBatch = @Batch";
            if (filterTenantId.HasValue) sql += " AND TenantId = @TenantId";
            var rows = await conn.QueryAsync<(string TableName, string ColumnName)>(sql, new { Batch = filterBatch, TenantId = filterTenantId });
            return rows.ToList();
        }

        // Define Explicit Mappings (Source -> Target)
        private static readonly Dictionary<string, string> ExplicitTableMappings = new(StringComparer.OrdinalIgnoreCase)
        {
            { "ActualActivityConfiguration", "ActualActivityConfigurations" },
            { "IndirectClockEvent", "IndirectClockEvent" }
        };

        /// <summary>Tables that exist only in MDC with different structure from ADC; create new table with same name in ADC, do not fuzzy-match to singular/plural.</summary>
        private static readonly HashSet<string> NoFuzzyMatchTables = new(StringComparer.OrdinalIgnoreCase)
        {
            "IndirectClockEvents"
        };

        static async Task RunValidation(string sourceConn, string targetConn)
        {
            Console.WriteLine("\n--> [Step 4] Validation");
            
            // Tenant prompt again? Or rely on global args if we stored them?
            // Validation usually targets context.
            // Let's ask for Tenant ID to match Data Sync scope.
            Console.Write("\n[Input] Enter Tenant Name for Validation (or press Enter for ALL): ");
            string tenantName = Console.ReadLine()?.Trim();
            int? tenantId = null;

            if (!string.IsNullOrEmpty(tenantName))
            {
               using var source = new SqlConnection(sourceConn); // Resolve from Source to get ID
               // Reuse resolution logic... (Should refactor into helper but duplicating for speed)
               try 
               {
                   tenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                   if (tenantId == null) tenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });
                   
                   Console.WriteLine($"[Validator] Validating for TenantID: {tenantId}");
               }
               catch(Exception ex) { Console.WriteLine($"Error resolving tenant: {ex.Message}"); return; }
            }

            var validator = new Validator(sourceConn, targetConn);
            await validator.RunValidationAsync(tenantId);
        }

        static async Task RunRollback(string targetConn)
        {
            var file = RollbackLogger.GetCurrentFilePath();
            if (string.IsNullOrEmpty(file)) file = "rollback_generic.sql"; 
            
            Console.WriteLine($"\nCurrent Rollback File: {file}");
            Console.WriteLine("Enter filename to execute (or press Enter for current):");
            string input = Console.ReadLine();
            string targetFile = string.IsNullOrWhiteSpace(input) ? file : input;
            
            if (!File.Exists(targetFile)) 
            {
                Console.WriteLine("[Error] File not found.");
                return;
            }

            Console.WriteLine($"Executing rollback script: {targetFile}...");
            string script = File.ReadAllText(targetFile);
            
            using var conn = new SqlConnection(targetConn);
            // Split by GO if necessary, but usually simple commands work.
            // Dapper Execute allows multiple statements.
            try
            {
                await conn.ExecuteAsync(script);
                Console.WriteLine("[Success] Rollback executed.");
            }
            catch(Exception ex) 
            {
                Console.WriteLine($"[Error] {ex.Message}");
            }
        }

        static async Task RunFullMigration(string sourceConn, string targetConn, int batchSize, bool dryRun, string[] args, int mergeChunkSize = 0,
            string[]? veryHighTimeoutTables = null, int veryHighTimeoutSeconds = 0,
            string[]? highTimeoutTables = null, int highTimeoutSeconds = 0)
        {
             await RunSchemaSync(sourceConn, targetConn, dryRun);
             await RunObjectSync(sourceConn, targetConn, dryRun);
             await RunDataSync(sourceConn, targetConn, batchSize, dryRun, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
        }

        /// <summary>
        /// Sync data by explicit tiers (MigrationConfig.TierTables).
        /// Chạy theo tier: chọn Tier, trong Tier chạy hết tenant, và chỉ Enable FK
        /// sau khi TẤT CẢ tier + tenant hoàn thành.
        /// </summary>
        static async Task RunDataSyncByTier(
            string sourceConn,
            string targetConn,
            int batchSize,
            bool dryRun,
            int mergeChunkSize,
            string[]? veryHighTimeoutTables,
            int veryHighTimeoutSeconds,
            string[]? highTimeoutTables,
            int highTimeoutSeconds)
        {
            Console.WriteLine("\n--> [Step 3b] Data Sync by Tier");
            RollbackLogger.Initialize("data_tier");

            // 1. Input tiers
            Console.Write("\n[Input] Enter Tiers to run (e.g. 1,2,3 or all): ");
            var tierInput = Console.ReadLine()?.Trim();
            var tiersToRun = ParseTierInput(tierInput);
            if (tiersToRun.Count == 0)
            {
                Console.WriteLine("[DataSyncByTier] No valid tiers selected. Abort.");
                return;
            }

            // 2. Tenant prompt (giống RunDataSync)
            Console.Write("\n[Input] Enter Tenant Name to filter by (or press Enter for ALL): ");
            string tenantName = Console.ReadLine()?.Trim();
            int? sourceTenantId = null;
            int? targetTenantId = null;
            List<(int SourceId, int TargetId, string DisplayName)>? allTenantPairs = null;

            using (var source = new SqlConnection(sourceConn))
            using (var target = new SqlConnection(targetConn))
            {
                await source.OpenAsync();
                await target.OpenAsync();

                if (!string.IsNullOrEmpty(tenantName))
                {
                    // Single-tenant resolution (same as RunDataSync)
                    sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>(
                        "SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (sourceTenantId == null)
                        sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>(
                            "SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                    if (sourceTenantId == null)
                    {
                        Console.WriteLine($"[Error] Source Tenant '{tenantName}' not found.");
                        return;
                    }
                    Console.WriteLine($"[TenantFilter] Resolved Source ID: {sourceTenantId}");

                    var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>(
                        "SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (existingTargetId == null)
                        existingTargetId = await target.QueryFirstOrDefaultAsync<int?>(
                            "SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                    if (existingTargetId != null)
                    {
                        targetTenantId = existingTargetId;
                        Console.WriteLine($"[TenantMap] Found existing Target Tenant '{tenantName}' (ID: {targetTenantId}). Merging into it.");
                    }
                    else
                    {
                        if (!dryRun)
                        {
                            Console.WriteLine($"[TenantMap] Creating new Tenant '{tenantName}' in Target...");
                            var sourceTenantRow = await source.QuerySingleAsync<dynamic>(
                                "SELECT * FROM Tenants WHERE Id = @Id", new { Id = sourceTenantId });
                            var props = (IDictionary<string, object>)sourceTenantRow;
                            var cols = props.Keys.Where(k => k != "Id").ToList();
                            var vals = cols.Select(k => "@" + k).ToList();
                            string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                            targetTenantId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                            Console.WriteLine($"[TenantMap] Created Target Tenant. New ID: {targetTenantId}");
                        }
                        else
                        {
                            Console.WriteLine($"[DryRun] Would Create Tenant '{tenantName}' in Target.");
                            targetTenantId = sourceTenantId;
                        }
                    }
                    if (targetTenantId == null && dryRun) targetTenantId = sourceTenantId;
                }
                else
                {
                    // ALL tenants: mirror from RunDataSync
                    var sourceTenants = (await source.QueryAsync<(int Id, string? Name, string? TenancyName)>(
                        "SELECT Id, Name, TenancyName FROM Tenants ORDER BY Id")).ToList();
                    if (sourceTenants.Count == 0)
                    {
                        Console.WriteLine("[DataSyncByTier] No tenants found in Source.");
                        return;
                    }
                    Console.WriteLine($"[TenantFilter] ALL tenants: {sourceTenants.Count} tenant(s) in Source.");
                    allTenantPairs = new List<(int, int, string)>();
                    foreach (var row in sourceTenants)
                    {
                        string nameForLookup = (row.TenancyName ?? row.Name ?? row.Id.ToString()) ?? "";
                        var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>(
                            "SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = nameForLookup });
                        if (existingTargetId == null)
                            existingTargetId = await target.QueryFirstOrDefaultAsync<int?>(
                                "SELECT Id FROM Tenants WHERE Name = @Name", new { Name = nameForLookup });
                        int targetId;
                        if (existingTargetId != null)
                        {
                            targetId = existingTargetId.Value;
                            Console.WriteLine($"[TenantMap] '{nameForLookup}' Source ID {row.Id} -> Target ID {targetId} (existing).");
                        }
                        else
                        {
                            if (!dryRun)
                            {
                                var sourceTenantRow = await source.QuerySingleAsync<dynamic>(
                                    "SELECT * FROM Tenants WHERE Id = @Id", new { Id = row.Id });
                                var props = (IDictionary<string, object>)sourceTenantRow;
                                var cols = props.Keys.Where(k => k != "Id").ToList();
                                var vals = cols.Select(k => "@" + k).ToList();
                                string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                                targetId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                                Console.WriteLine($"[TenantMap] '{nameForLookup}' Source ID {row.Id} -> Created Target ID {targetId}.");
                            }
                            else
                            {
                                targetId = row.Id;
                                Console.WriteLine($"[DryRun] Would create Tenant '{nameForLookup}' in Target (mock TargetId: {targetId}).");
                            }
                        }
                        allTenantPairs.Add((row.Id, targetId, nameForLookup));
                    }
                }
            }

            // 3. Chuẩn bị Schema/Migrator
            var schemaSync = new SchemaSync(sourceConn, targetConn);
            var migrator = new DataMigrator(sourceConn, targetConn, batchSize);

            // 4. Lấy danh sách bảng tồn tại ở MDC, không bị skip
            var sourceTables = (await schemaSync.GetExistingSourceTablesAsync())
                .Where(t => !TableSkipRules.ShouldSkipTable(t))
                .ToList();

            // 4b. Build Tier 9 (Others) động: mọi bảng còn lại không nằm trong Tier 1-8
            var baseTierTables = new HashSet<string>(
                MigrationConfig.TierTables
                    .Where(kvp => kvp.Key >= 1 && kvp.Key <= 8)
                    .SelectMany(kvp => kvp.Value),
                StringComparer.OrdinalIgnoreCase);
            var tier9Tables = sourceTables
                .Where(t => !baseTierTables.Contains(t))
                .ToList();
            if (tier9Tables.Count > 0)
            {
                // Cập nhật TierTables[9] tại runtime để Option 9 có thể chọn
                MigrationConfig.TierTables[9] = tier9Tables;
            }

            // 5. Build danh sách bảng thuộc các Tier đã chọn
            var tierTables = new List<string>();
            foreach (var tier in tiersToRun.OrderBy(x => x))
            {
                if (!MigrationConfig.TierTables.TryGetValue(tier, out var tables)) continue;
                foreach (var t in tables)
                    if (sourceTables.Contains(t, StringComparer.OrdinalIgnoreCase))
                        tierTables.Add(t);
            }

            Console.WriteLine($"[DataSyncByTier] Tables to migrate in selected tiers: {tierTables.Count}");

            // 6. Tables only in MDC – chỉ đọc từ file (đã ghi ở Option 1 trước khi thay đổi cấu trúc ADC)
            var fromFileTier = await Helper.ReadTableListFromNumberedFileAsync(Helper.MdcOnlyTablesFilePath);
            var tablesOnlyInMdc = fromFileTier.Count > 0
                ? fromFileTier.ToHashSet(StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (fromFileTier.Count > 0)
                Console.WriteLine($"[DataSyncByTier] Tables only in MDC: {tablesOnlyInMdc.Count} (from file {Helper.MdcOnlyTablesFilePath})");
            else
                Console.WriteLine($"[DataSyncByTier] Tables only in MDC: 0 (file empty or missing; run Option 1 first to generate {Helper.MdcOnlyTablesFilePath})");

            // 7. Smart User Sync (dùng chung với Option 3)
            var userMapping = new Dictionary<long, long>();
            if (allTenantPairs != null)
            {
                foreach (var (srcId, tgtId, displayName) in allTenantPairs)
                {
                    Console.WriteLine($"[Users] Syncing users for tenant: {displayName} (Source: {srcId} -> {tgtId})");
                    await SyncUsersAsync(sourceConn, targetConn, userMapping, srcId, tgtId, dryRun);
                }
            }
            else
            {
                await SyncUsersAsync(sourceConn, targetConn, userMapping, sourceTenantId, targetTenantId, dryRun);
            }

            // 8. Thực thi migrate theo Tier + Tenant
            if (!dryRun)
            {
                using var sourceConnection = new SqlConnection(sourceConn);
                using var targetConnection = new SqlConnection(targetConn);
                await sourceConnection.OpenAsync();
                await targetConnection.OpenAsync();
                await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(targetConnection);
                await FkConstraintHelper.DisableAllFkAsync(targetConnection);

                try
                {
                    var migrationBatch = Guid.NewGuid().ToString("N");
                    var pkInfoCache = new Dictionary<string, PkColumnInfo?>(StringComparer.OrdinalIgnoreCase);
                    var tableHasTenantIdTargetCache = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                    var tableHasTenantIdSourceCache = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

                    async Task<bool> GetTargetTableHasTenantIdAsync(string name)
                    {
                        if (tableHasTenantIdTargetCache.TryGetValue(name, out var v)) return v;
                        var r = await DataMigrator.TableHasTenantIdColumnAsync(targetConnection, name);
                        tableHasTenantIdTargetCache[name] = r;
                        return r;
                    }
                    async Task<bool> GetSourceTableHasTenantIdAsync(string name)
                    {
                        if (tableHasTenantIdSourceCache.TryGetValue(name, out var v)) return v;
                        var r = await DataMigrator.TableHasTenantIdColumnAsync(sourceConnection, name);
                        tableHasTenantIdSourceCache[name] = r;
                        return r;
                    }

                    var existingAdcTables = (await schemaSync.GetExistingTargetTablesAsync())
                        .ToHashSet(StringComparer.OrdinalIgnoreCase);

                    var tenantsToRun = allTenantPairs != null
                        ? allTenantPairs
                        : new List<(int SourceId, int TargetId, string DisplayName)>
                          { (sourceTenantId!.Value, targetTenantId!.Value, tenantName ?? "Single") };

                    foreach (var (curSourceId, curTargetId, curDisplayName) in tenantsToRun)
                    {
                        int? src = curSourceId;
                        int? tgt = curTargetId;
                        Console.WriteLine($"[DataSyncByTier] --- Tenant: {curDisplayName} (SourceId: {curSourceId} -> TargetId: {curTargetId}) ---");

                        foreach (var table in tierTables)
                        {
                            if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__"))
                                continue;

                            var isNew = !existingAdcTables.Contains(table, StringComparer.OrdinalIgnoreCase);
                            string targetTable = table;

                            if (ExplicitTableMappings.ContainsKey(table))
                            {
                                string mappedTarget = ExplicitTableMappings[table];
                                targetTable = mappedTarget;
                                if (existingAdcTables.Contains(mappedTarget, StringComparer.OrdinalIgnoreCase))
                                {
                                    targetTable = mappedTarget;
                                    isNew = false;
                                    Console.WriteLine($"[SmartMerge] Applied explicit mapping: {table} (MDC) -> {targetTable} (ADC)");
                                    await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                                }
                                else
                                    Console.WriteLine($"[Map] Explicit target {targetTable} missing. Treating as new.");
                            }
                            else if (isNew)
                            {
                                string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                                if (bestMatch != null)
                                {
                                    targetTable = bestMatch;
                                    isNew = false;
                                    Console.WriteLine($"[SmartMerge] Detected match: {table} (MDC) -> {targetTable} (ADC)");
                                    await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                                }
                            }

                            if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);

                            if (!pkInfoCache.TryGetValue(targetTable, out var pkInfo))
                            {
                                pkInfo = await DataMigrator.GetPkColumnInfoAsync(targetConnection, targetTable);
                                pkInfoCache[targetTable] = pkInfo;
                            }

                            bool skipGlobalSinglePk = false;
                            if (pkInfo != null && pkInfo.PkColumnCount == 1 &&
                                !await GetTargetTableHasTenantIdAsync(targetTable))
                            {
                                string? mappingTable = pkInfo.DataType switch
                                {
                                    "int" => "IdMappingInt",
                                    "bigint" => "IdMappingBigInt",
                                    "uniqueidentifier" => "IdMappingGuid",
                                    _ => null
                                };

                                if (mappingTable != null)
                                {
                                    var existingMappings = await targetConnection.ExecuteScalarAsync<int>(
                                        $"SELECT COUNT(1) FROM [dbo].[{mappingTable}] WHERE TableName = @TableName AND ColumnName = @ColumnName",
                                        new { TableName = targetTable, ColumnName = pkInfo.ColumnName });
                                    skipGlobalSinglePk = existingMappings > 0;
                                }
                                else
                                {
                                    var tableEsc = targetTable.Replace("]", "]]");
                                    var existingRows = await targetConnection.ExecuteScalarAsync<int>(
                                        $"SELECT COUNT(1) FROM [dbo].[{tableEsc}]");
                                    skipGlobalSinglePk = existingRows > 0;
                                }

                                if (skipGlobalSinglePk)
                                {
                                    Console.WriteLine($"[DataSyncByTier] Skipping global single-PK table '{targetTable}' (no TenantId, already seeded).");
                                    continue;
                                }
                            }

                            if (tablesOnlyInMdc.Contains(targetTable))
                            {
                                Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: MDC-only (copy from MDC, no delete) | TenantId: {(tgt.HasValue ? tgt.ToString() : "all")}");
                                await migrator.MigrateTableAsync(table, isNewTable: false, targetTableName: targetTable,
                                    sourceTenantId: src, targetTenantId: tgt,
                                    userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);

                                if (pkInfo != null && (pkInfo.DataType == "int" || pkInfo.DataType == "bigint" || pkInfo.DataType == "uniqueidentifier"))
                                {
                                    var mappingTable = pkInfo.DataType == "int" ? "IdMappingInt"
                                        : pkInfo.DataType == "bigint" ? "IdMappingBigInt"
                                        : "IdMappingGuid";
                                    var targetWhere = (tgt.HasValue && await GetTargetTableHasTenantIdAsync(targetTable))
                                        ? $" WHERE TenantId = {tgt.Value}"
                                        : "";
                                    var pkColEsc = pkInfo.ColumnName.Replace("]", "]]");
                                    var tableEsc = targetTable.Replace("]", "]]");
                                    var bulkSql = $@"
INSERT INTO [dbo].[{mappingTable}] (TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)
SELECT @TableName, @ColumnName, [{pkColEsc}], [{pkColEsc}], @Batch, @TenantId FROM [dbo].[{tableEsc}]{targetWhere}";
                                    var inserted = await targetConnection.ExecuteAsync(bulkSql,
                                        new { TableName = targetTable, ColumnName = pkInfo.ColumnName, Batch = migrationBatch, TenantId = (int?)tgt },
                                        commandTimeout: 600);
                                    if (inserted > 0)
                                        Console.WriteLine($"   -> IdMapping (MDC-only, bulk): {inserted} row(s) -> [dbo].[{mappingTable}]");
                                }

                                Console.WriteLine($"   -> Done: {table}");
                            }
                            else
                            {
                                if (pkInfo == null)
                                {
                                    Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Direct MigrateTable (no PK/IdMapping) | TenantId: {(src.HasValue ? src.ToString() : "all")}");
                                    await migrator.MigrateTableAsync(table, isNewTable: isNew, targetTableName: targetTable,
                                        sourceTenantId: src, targetTenantId: tgt,
                                        userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                                    Console.WriteLine($"   -> Done: {table}");
                                }
                                else if (pkInfo.PkColumnCount == 1 &&
                                         pkInfo.DataType != "int" &&
                                         pkInfo.DataType != "bigint" &&
                                         pkInfo.DataType != "uniqueidentifier")
                                {
                                    Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Natural PK (insert missing only) | TenantId: {(src.HasValue ? src.ToString() : "all")}");
                                    await migrator.MigrateTableNaturalPkAsync(sourceConnection, targetConnection, table, targetTable,
                                        pkInfo, src, tgt, userMapping);
                                    Console.WriteLine($"   -> Done: {table}");
                                }
                                else if (pkInfo.PkColumnCount > 1)
                                {
                                    Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Composite PK (staging -> INSERT with IdMapping JOIN) | TenantId: {(src.HasValue ? src.ToString() : "all")}");
                                    var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                                    var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                                    await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable,
                                        pkColumnNames, fkColumns, src, tgt);
                                    Console.WriteLine($"   -> Done: {table}");
                                }
                                else
                                {
                                    Console.WriteLine($"[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: Staging + MERGE + IdMapping (single PK int/bigint/guid) | TenantId: {(tgt.HasValue ? tgt.ToString() : "null")}");
                                    var whereClause = (src.HasValue && await GetSourceTableHasTenantIdAsync(table))
                                        ? $" WHERE TenantId = {src.Value}"
                                        : "";
                                    await migrator.CreateStagingTableAsync(sourceConnection, targetConnection, table, targetTable, pkInfo);

                                    int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);

                                    if (commandTimeoutOverride.HasValue)
                                        Console.WriteLine($"   -> Using extended timeout: {commandTimeoutOverride.Value}s for this table.");

                                    await migrator.InsertTableWithIdMappingAsync(
                                        sourceConnection, targetConnection,
                                        table, targetTable, pkInfo,
                                        migrationBatch, tgt,
                                        whereClause, src, tgt,
                                        userMapping, mergeChunkSize, commandTimeoutOverride);

                                    Console.WriteLine($"   -> Done: {table}");
                                }
                            }
                        }

                        // Sau khi chạy HẾT bảng trong Tier cho tenant này: Update FK từ IdMapping
                        await FkConstraintHelper.UpdateFkFromIdMappingAsync(targetConnection, migrationBatch, tgt);
                    }

                    // Chỉ bật lại FK khi chạy đủ TẤT CẢ tier (1..9); chạy từng tier thì không bật
                    if (tiersToRun.Count == 9)
                    {
                        await FkConstraintHelper.EnableAllFkAsync(targetConnection);
                        Console.WriteLine("[DataSyncByTier] All tiers completed. Re-enabled all foreign keys.");
                    }
                    else
                    {
                        Console.WriteLine("[DataSyncByTier] Partial tiers run. Foreign keys left disabled.");
                    }
                }
                finally
                {
                    // no-op, FK đã xử lý ở trên
                }
            }
            else
            {
                // DryRun: in kế hoạch
                var dryRunTenants = allTenantPairs != null
                    ? allTenantPairs.Select(p => p.DisplayName).ToList()
                    : new List<string> { tenantName ?? "Single" };

                Console.WriteLine("\n[DryRun] Would run data sync by tier for tenants:");
                foreach (var tn in dryRunTenants)
                    Console.WriteLine($" - {tn}");

                Console.WriteLine("\n[DryRun] Tables in selected tiers:");
                foreach (var t in tierTables)
                    Console.WriteLine($" - {t}");
            }
        }

        static async Task SyncUsersAsync(string sourceConnStr, string targetConnStr, Dictionary<long, long> userMapping, int? sourceTenantId, int? targetTenantId, bool dryRun)
        {
            Console.WriteLine("\n[Users] Starting Smart User Sync...");
            
            using var source = new SqlConnection(sourceConnStr);
            using var target = new SqlConnection(targetConnStr);
            
            // 1. Fetch Source Users
            // Include TenantId to support (TenantId, UserName) matching
            string sourceSql = "SELECT Id, UserName, EmailAddress, TenantId FROM Users";
            if (sourceTenantId.HasValue) sourceSql += " WHERE TenantId = @TenantId";
            var sourceUsers = await source.QueryAsync<dynamic>(sourceSql, new { TenantId = sourceTenantId });
            
            // 2. Fetch Target Users (to find matches)
            // Include TenantId and key by (TenantId, UserName) to avoid duplicates in 'all tenant' mode
            string targetSql = "SELECT Id, UserName, TenantId FROM Users";
            if (targetTenantId.HasValue) targetSql += " WHERE TenantId = @TenantId";
            var targetRows = await target.QueryAsync<dynamic>(targetSql, new { TenantId = targetTenantId });
            var targetUsers = targetRows.ToDictionary(
                k =>
                {
                    int? tid = (int?)k.TenantId;
                    string name = (string)k.UserName;
                    return (tid?.ToString() ?? "null") + "|" + name;
                },
                v => (long)v.Id,
                StringComparer.OrdinalIgnoreCase); // Dictionary<(TenantId, UserName), Id>

            Console.WriteLine($"[Users] Found {sourceUsers.Count()} Source Users, {targetUsers.Count} Target Users.");

            // 3. Process Each Source User
            foreach (var sUser in sourceUsers)
            {
                string userName = sUser.UserName;
                long sourceId = sUser.Id;
                long targetId = 0;

                int? keyTenantId = targetTenantId ?? (int?)sUser.TenantId;
                string dictKey = (keyTenantId?.ToString() ?? "null") + "|" + userName;

                if (targetUsers.ContainsKey(dictKey))
                {
                    // Match found!
                    targetId = targetUsers[dictKey];
                    // Console.WriteLine($"   [Match] {dictKey} ({sourceId} -> {targetId})"); 
                }
                else
                {
                    // New User - Insert
                    if (!dryRun)
                    {
                        // We need to fetch FULL row to insert
                        // Excluding Id to let Identity generate it
                        var fullUser = await source.QuerySingleAsync<dynamic>("SELECT * FROM Users WHERE Id = @Id", new { Id = sourceId });
                        var props = (IDictionary<string, object>)fullUser;
                        var cols = props.Keys.Where(k => k != "Id").ToList();
                        
                        // Handle TenantId transformation in Insert
                        if (sourceTenantId != targetTenantId && props.ContainsKey("TenantId"))
                        {
                            props["TenantId"] = targetTenantId; 
                        }
                        
                        var vals = cols.Select(k => "@" + k).ToList();
                        string insertSql = $"INSERT INTO Users ({string.Join(",", cols)}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as bigint);";
                        
                        targetId = await target.ExecuteScalarAsync<long>(insertSql, (object)props);
                        Console.WriteLine($"   [Insert] Created User {userName} (NewId: {targetId})");
                    }
                    else
                    {
                        Console.WriteLine($"   [DryRun] Would Insert User {userName}");
                        targetId = sourceId; // Mock
                    }
                }
                
                // Add to Map
                if (!userMapping.ContainsKey(sourceId))
                {
                    userMapping.Add(sourceId, targetId);
                }
            }
            Console.WriteLine($"[Users] User Mapping Built: {userMapping.Count} entries.");
        }

        static string GetBestFuzzyMatch(string sourceTable, HashSet<string> targetTables)
        {
            if (NoFuzzyMatchTables.Contains(sourceTable))
                return null;
            if (sourceTable.EndsWith("s") && targetTables.Contains(sourceTable.Substring(0, sourceTable.Length - 1)))
            {
                return sourceTable.Substring(0, sourceTable.Length - 1);
            }
            if (!sourceTable.EndsWith("s") && targetTables.Contains(sourceTable + "s"))
            {
                 return sourceTable + "s";
            }
            return null;
        }

        /// <summary>
        /// <summary>
        /// Resolves extended timeout for large tables: VeryHigh (e.g. 7200s) takes precedence over High (e.g. 3600s).
        /// </summary>
        static int? GetExtendedTimeoutForTable(string targetTable,
            string[]? veryHighTimeoutTables, int veryHighTimeoutSeconds,
            string[]? highTimeoutTables, int highTimeoutSeconds)
        {
            if (veryHighTimeoutTables?.Contains(targetTable, StringComparer.OrdinalIgnoreCase) == true && veryHighTimeoutSeconds > 0)
                return veryHighTimeoutSeconds;
            if (highTimeoutTables?.Contains(targetTable, StringComparer.OrdinalIgnoreCase) == true && highTimeoutSeconds > 0)
                return highTimeoutSeconds;
            return null;
        }

        /// <summary>
        /// Parse tier input string (e.g. "1,2,3" or "all") into a list of tier numbers.
        /// </summary>
        static List<int> ParseTierInput(string? input)
        {
            var result = new List<int>();
            if (string.IsNullOrWhiteSpace(input))
                return result;

            input = input.Trim();
            if (string.Equals(input, "all", StringComparison.OrdinalIgnoreCase))
            {
                // Tiers 1..9 (9 = Others)
                result.AddRange(Enumerable.Range(1, 9));
                return result;
            }

            var parts = input.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                if (int.TryParse(part.Trim(), out var n) && n >= 1 && n <= 9)
                {
                    if (!result.Contains(n))
                        result.Add(n);
                }
            }
            result.Sort();
            return result;
        }

    }
}
