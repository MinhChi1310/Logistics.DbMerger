using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Dapper;
using Polly;
using Serilog;

namespace Logistics.DbMerger
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Directory.CreateDirectory("output");
            Serilog.Debugging.SelfLog.Enable(msg => System.Diagnostics.Debug.WriteLine(msg));

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                .WriteTo.File("output/migration-.log",
                    rollingInterval: RollingInterval.Day,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            try
            {
            Log.Information("=== Logistics DB Merger Tool ===");

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
            int metricsIntervalSeconds = config.GetSection("Settings:MetricsIntervalSeconds").Exists()
                ? config.GetValue<int>("Settings:MetricsIntervalSeconds")
                : 5;
            if (metricsIntervalSeconds < 0) metricsIntervalSeconds = 0; // negative = disabled

            if (batchSize <= 0) batchSize = 5000;
            if (mergeChunkSize < 0) mergeChunkSize = 0;

            if (string.IsNullOrEmpty(sourceConn) || string.IsNullOrEmpty(targetConn) || sourceConn.Contains("YOUR_"))
            {
                Log.Error("[Error] Please configure valid connection strings in appsettings.json");
                return;
            }

            // Command Line Args for automation (bypass menu if detected)
            if (args.Length > 0 && args.Any(a => a.StartsWith("--tenant") || a.StartsWith("--mode")))
            {
                // Extract tenant name from --tenant=Name or --tenant Name
                string? autoTenantName = null;
                for (int i = 0; i < args.Length; i++)
                {
                    if (args[i].StartsWith("--tenant="))
                        autoTenantName = args[i].Substring("--tenant=".Length).Trim('"', '\'');
                    else if (args[i] == "--tenant" && i + 1 < args.Length)
                        autoTenantName = args[i + 1].Trim('"', '\'');
                }
                Log.Information("[Auto-Run] Arguments detected. Tenant: {TenantName}. Running full migration...", autoTenantName ?? "ALL");
                await RunFullMigration(sourceConn, targetConn, batchSize, dryRun, autoTenantName, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds, metricsIntervalSeconds);
                return;
            }

            while (true)
            {
                Log.Information("\n[Main Menu] — Migration Workflow Order");
                Log.Information("───────────────────────────────────────");
                Log.Information("  [Full Migration]");
                Log.Information("  11. Full Migration (PreFlight → Schema → Data → FK → Validate)");
                Log.Information("  [Pre-Migration]");
                Log.Information("  1. Pre-Flight Checks (identity overlaps, partition filegroups)");
                Log.Information("  2. Generate Reports (MDC-only tables, comparison)");
                Log.Information("  [Schema]");
                Log.Information("  3. Sync Schema (Tables & Columns)");
                Log.Information("  4. Sync Objects (Procedures, Views, Functions)");
                Log.Information("  [Data Migration]");
                Log.Information("  5. Sync Data (Smart Merge & Tenant Filter)");
                Log.Information("  6. Sync Data by Tier (Tier -> Tenant)");
                Log.Information("  7. Enable FK (re-enable all foreign keys on target)");
                Log.Information("  [Validation]");
                Log.Information("  8. Validate / Verify (row counts, FK integrity, business logic)");
                Log.Information("  [Utilities]");
                Log.Information("  9. Rollback Last Action");
                Log.Information("  10. Clear Migration Data (delete rows based on IdMapping)");
                Log.Information("  0. Exit");
                Log.Information("───────────────────────────────────────");
                Console.Write("Select an option: ");
                
                var key = Console.ReadLine();
                try
                {
                    // Initialize ReportWriter for interactive menu if not already initialized
                    if (!ReportWriter.IsInitialized && key != "0")
                        ReportWriter.Initialize("Interactive", DateTime.UtcNow);

                    switch (key)
                    {
                        // Pre-Migration
                        case "1":
                            await RunPreFlight(sourceConn, targetConn);
                            break;
                        case "2":
                            await RunListTablesOnlyInMdcAndCreateStructureAsync(sourceConn, targetConn, dryRun);
                            break;
                        // Schema & Objects
                        case "3":
                            await RunSchemaSync(sourceConn, targetConn, dryRun);
                            break;
                        case "4":
                            await RunObjectSync(sourceConn, targetConn, dryRun);
                            break;
                        // Data Migration
                        case "5":
                            await RunDataSync(sourceConn, targetConn, batchSize, dryRun, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds, metricsIntervalSeconds);
                            break;
                        case "6":
                            await RunDataSyncByTier(sourceConn, targetConn, batchSize, dryRun, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds, metricsIntervalSeconds);
                            break;
                        case "7":
                            await RunEnableFkAsync(targetConn);
                            break;
                        // Validation
                        case "8":
                            await RunValidation(sourceConn, targetConn);
                            break;
                        // Utilities
                        case "9":
                            await RunRollback(targetConn);
                            break;
                        case "10":
                            await RunClearMigrationDataAsync(targetConn);
                            break;
                        case "11":
                            Console.Write("\n[Input] Enter Tenant Name (required): ");
                            var fullMigTenant = Console.ReadLine()?.Trim();
                            if (string.IsNullOrEmpty(fullMigTenant))
                            {
                                Log.Warning("Tenant name is required for full migration.");
                                break;
                            }
                            await RunFullMigration(sourceConn, targetConn, batchSize, dryRun, fullMigTenant, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds, metricsIntervalSeconds);
                            break;
                        case "0":
                            return;
                        default:
                            Log.Warning("Invalid selection.");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "[Fatal Error] {ErrorMessage}", ex.Message);
                }
            }
            }
            finally
            {
                try { Log.CloseAndFlush(); } catch { /* ensure original exception is not masked */ }
            }
        }

        static async Task RunSchemaSync(string sourceConn, string targetConn, bool dryRun)
        {
            Log.Information("\n--> [Step 1] Schema Sync");
            RollbackLogger.Initialize("schema");
            var schemaSync = new SchemaSync(sourceConn, targetConn);

            // Get "tables only in MDC" before any schema change to ADC; Option 3/9 will read from this file only
            Log.Information("Writing tables only in MDC (before schema change)...");
            await Helper.GetTablesOnlyInMdcAsync(sourceConn, targetConn, Helper.MdcOnlyTablesFilePath, writeToFile: true);
            Log.Information("Saved to {FilePath}", Helper.MdcOnlyTablesFilePath);

            Log.Information("Checking missing tables...");
            var missingTables = await schemaSync.GetMissingTablesAsync();
            Log.Information("Found {MissingTableCount} missing tables.", missingTables.Count);

            if (!dryRun)
            {
                foreach (var table in missingTables)
                {
                    // Skip if explicitly mapped (we map to existing table instead of creating new one with source name)
                    if (ExplicitTableMappings.ContainsKey(table))
                    {
                        Log.Information("[Schema] Skipping creation of '{Table}' (Explicitly mapped to '{MappedTarget}')", table, ExplicitTableMappings[table]);
                        continue;
                    }
                    await schemaSync.SyncTableAsync(table);
                }
            }
            else
            {
                Log.Information("[DryRun] Skipping table creation.");
            }

            // Sync Missing Columns for EXISTING Common Tables
            Log.Information("\nChecking for missing columns in common tables...");
            var sourceTables = await schemaSync.GetExistingSourceTablesAsync();
            var targetTables = await schemaSync.GetExistingTargetTablesAsync();
            
            // Identify common tables (by exact name)
            var commonTables = sourceTables.Intersect(targetTables, StringComparer.OrdinalIgnoreCase).ToList();
            Log.Information("Found {CommonTableCount} common tables. Scanning columns...", commonTables.Count);

            foreach (var table in commonTables)
            {
                if (table == "sysdiagrams" ||  table.StartsWith("__")) continue;// delete table == "Tenants" ||

                // This adds missing columns if any
                await schemaSync.SyncTableSchemaAsync(table, table, dryRun);
            }

            // Sync Missing Columns for EXPLICIT MAPPED Tables (that are technically "missing" via commonTables logic)
            Log.Information("Checking explicit mappings...");
            foreach (var kvp in ExplicitTableMappings)
            {
                var sourceTable = kvp.Key;
                var targetTable = kvp.Value;

                if (sourceTables.Contains(sourceTable, StringComparer.OrdinalIgnoreCase) && 
                    targetTables.Contains(targetTable, StringComparer.OrdinalIgnoreCase))
                {
                     Log.Information("[Mapping] Checking {SourceTable} -> {TargetTable}...", sourceTable, targetTable);
                     await schemaSync.SyncTableSchemaAsync(sourceTable, targetTable, dryRun);
                }
            }

            // After all tables and columns are synced, check constraints across all tables
            Log.Information("\nSyncing constraints across all tables...");
            await schemaSync.SyncAllConstraintsAsync(ExplicitTableMappings, dryRun);
            ReportWriter.WriteSchemaReport();
        }

        static async Task RunObjectSync(string sourceConn, string targetConn, bool dryRun)
        {
            Log.Information("\n--> [Step 2] Object Sync");
            RollbackLogger.Initialize("objects");
            var objectSync = new ObjectSync(sourceConn, targetConn);
            await objectSync.SyncObjectsAsync(dryRun);
        }

        static async Task RunDataSync(string sourceConn, string targetConn, int batchSize, bool dryRun, int mergeChunkSize = 0,
            string[]? veryHighTimeoutTables = null, int veryHighTimeoutSeconds = 0,
            string[]? highTimeoutTables = null, int highTimeoutSeconds = 0,
            int metricsIntervalSeconds = 5)
        {
            Log.Information("\n--> [Step 3] Data Sync");
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
                        Log.Error("[Error] Source Tenant '{TenantName}' not found.", tenantName);
                        return;
                    }
                    Log.Information("[TenantFilter] Resolved Source ID: {SourceTenantId}", sourceTenantId);

                    // 2. Resolve/Create Target Tenant
                    var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (existingTargetId == null)
                        existingTargetId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                    if (existingTargetId != null)
                    {
                        targetTenantId = existingTargetId;
                        Log.Information("[TenantMap] Found existing Target Tenant '{TenantName}' (ID: {TargetTenantId}). Merging into it.", tenantName, targetTenantId);

                        // Safety check: warn if previous migration data exists (could cause duplicates)
                        var hasPriorMapping = await target.ExecuteScalarAsync<int>(
                            "SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'IdMappingGuid') " +
                            "AND EXISTS (SELECT 1 FROM IdMappingGuid WHERE TenantId = @TenantId) THEN 1 ELSE 0 END",
                            new { TenantId = targetTenantId });
                        if (hasPriorMapping > 0)
                        {
                            Log.Warning("[Warning] IdMapping data already exists for Target TenantId {TargetTenantId}. Re-importing without clearing may cause duplicate rows.", targetTenantId);
                            Log.Warning("[Warning] Consider running Option 10 (Clear Migration Data) first.");
                        }
                    }
                    else
                    {
                        if (!dryRun)
                        {
                            Log.Information("[TenantMap] Creating new Tenant '{TenantName}' in Target...", tenantName);
                            var sourceTenantRow = await source.QuerySingleAsync<dynamic>("SELECT * FROM Tenants WHERE Id = @Id", new { Id = sourceTenantId });
                            var props = (IDictionary<string, object>)sourceTenantRow;
                            var cols = props.Keys.Where(k => k != "Id").ToList();
                            var vals = cols.Select(k => "@" + k).ToList();
                            string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols.Select(c => "[" + c.Replace("]", "]]") + "]"))}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                            targetTenantId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                            Log.Information("[TenantMap] Created Target Tenant. New ID: {TargetTenantId}", targetTenantId);
                        }
                        else
                        {
                            Log.Information("[DryRun] Would Create Tenant '{TenantName}' in Target.", tenantName);
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
                        Log.Information("[DataSync] No tenants found in Source.");
                        return;
                    }
                    Log.Information("[TenantFilter] ALL tenants: {TenantCount} tenant(s) in Source.", sourceTenants.Count);
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
                            Log.Information("[TenantMap] '{TenantName}' Source ID {SourceId} -> Target ID {TargetId} (existing).", nameForLookup, row.Id, targetId);
                        }
                        else
                        {
                            if (!dryRun)
                            {
                                var sourceTenantRow = await source.QuerySingleAsync<dynamic>("SELECT * FROM Tenants WHERE Id = @Id", new { Id = row.Id });
                                var props = (IDictionary<string, object>)sourceTenantRow;
                                var cols = props.Keys.Where(k => k != "Id").ToList();
                                var vals = cols.Select(k => "@" + k).ToList();
                                string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols.Select(c => "[" + c.Replace("]", "]]") + "]"))}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                                targetId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                                Log.Information("[TenantMap] '{TenantName}' Source ID {SourceId} -> Created Target ID {TargetId}.", nameForLookup, row.Id, targetId);
                            }
                            else
                            {
                                targetId = row.Id;
                                Log.Information("[DryRun] Would create Tenant '{TenantName}' in Target (mock TargetId: {TargetId}).", nameForLookup, targetId);
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
                Log.Information("[DataSync] Tables only in MDC: {Count} (from file {FilePath})", tablesOnlyInMdc.Count, Helper.MdcOnlyTablesFilePath);
            else
                Log.Information("[DataSync] Tables only in MDC: 0 (file empty or missing; run Option 1 first to generate {FilePath})", Helper.MdcOnlyTablesFilePath);
            
            // 2b. Smart User Sync (Before generic tables)
            var userMapping = new Dictionary<long, long>();
            if (allTenantPairs != null)
            {
                foreach (var (srcId, tgtId, displayName) in allTenantPairs)
                {
                    Log.Information("[Users] Syncing users for tenant: {DisplayName} (Source: {SourceId} -> Target: {TargetId})", displayName, srcId, tgtId);
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

            Log.Information("[DataSync] Tables to Migrate (Ordered): {TableCount}", orderedTables.Count);
            int totalTablesProcessed = 0;
            var overallSw = Stopwatch.StartNew();

            if (!dryRun)
            {
                using var sourceConnection = new SqlConnection(sourceConn);
                using var targetConnection = new SqlConnection(targetConn);
                await sourceConnection.OpenAsync();
                await targetConnection.OpenAsync();

                ResourceMonitor.Start(metricsIntervalSeconds, sourceConn, targetConn);

                // Concurrency guard: prevent two instances from running simultaneously
                var lockResult = await targetConnection.ExecuteScalarAsync<int>(
                    "EXEC sp_getapplock @Resource = 'LogisticsDbMerger_DataSync', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0");
                if (lockResult < 0)
                {
                    Log.Error("[Error] Another instance of the migration tool is running against this database. Aborting.");
                    return;
                }
                Log.Information("[DataSync] Acquired exclusive application lock.");

                await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(targetConnection);
                await DataSyncCheckpointHelper.EnsureTableAsync(targetConnection);
                await FkConstraintHelper.DisableAllFkAsync(targetConnection);
                // Clear global table checkpoint so global tables (Editions, AllowableAbsence, SubThreadType)
                // are always re-merged with latest source data on each run
                await DataSyncCheckpointHelper.ClearByTenantAsync(targetConnection, MigrationConfig.GlobalTableCheckpointSentinel);
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

                var abpMergeStrategy = AbpMergeStrategyMap;

                foreach (var (curSourceId, curTargetId, curDisplayName) in tenantsToRun)
                {
                    int? src = curSourceId;
                    int? tgt = curTargetId;
                    ReportWriter.SetDataSyncTenantInfo(curSourceId, curTargetId, curDisplayName);
                    if (allTenantPairs != null)
                        Log.Information("[DataSync] --- Tenant: {DisplayName} (SourceId: {SourceId} -> TargetId: {TargetId}) ---", curDisplayName, curSourceId, curTargetId);

                    foreach (var table in orderedTables)
                    {
                        if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__")) continue;

                        // ABP merge strategy check (FR42-44)
                        if (abpMergeStrategy.TryGetValue(table, out var abpStrategy))
                        {
                            if (abpStrategy == "skip")
                            {
                                Log.Information("[DataSync] Skipped {Table} (managed by target)", table);
                                continue;
                            }

                            if (!src.HasValue)
                            {
                                Log.Warning("[DataSync] WARNING: ABP table {Table} requires per-tenant migration. Skipping in all-tenants mode.", table);
                                continue;
                            }

                            Log.Information("[DataSync] ABP table {Table}: strategy={AbpStrategy}, tenant={SourceId}->{TargetId}", table, abpStrategy, src, tgt);
                        }

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
                                Log.Information("[SmartMerge] Applied explicit mapping: {SourceTable} (MDC) -> {TargetTable} (ADC)", table, targetTable);
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                            else
                                Log.Information("[Map] Explicit target {TargetTable} missing. Treating as new.", targetTable);
                        }
                        else if (isNew)
                        {
                            string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                            if (bestMatch != null)
                            {
                                targetTable = bestMatch;
                                isNew = false;
                                Log.Information("[SmartMerge] Detected match: {SourceTable} (MDC) -> {TargetTable} (ADC)", table, targetTable);
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
                                Log.Information("[Checkpoint] Skipping table '{TargetTable}' for tenant {SourceId}->{TargetId} (already completed).", targetTable, src, tgt);
                                ReportWriter.AddDataSyncTable(table, targetTable, 0, 0, "Checkpoint-skipped", null);
                                continue;
                            }
                        }

                        // Global table MERGE upsert: route to MergeGlobalTableAsync
                        if (MigrationConfig.GlobalTables.Contains(targetTable))
                        {
                            // Check global checkpoint (sentinel 0,0) so we only merge once when running ALL tenants
                            if (await DataSyncCheckpointHelper.IsTableDoneAsync(targetConnection, MigrationConfig.GlobalTableCheckpointSentinel, MigrationConfig.GlobalTableCheckpointSentinel, targetTable))
                            {
                                Log.Information("[DataSync] Skipping global table '{TargetTable}' (already merged this run).", targetTable);
                                ReportWriter.AddDataSyncTable(table, targetTable, 0, 0, "Global-skipped", null);
                                continue;
                            }
                            if (!MigrationConfig.GlobalTableNaturalKeys.TryGetValue(targetTable, out var matchKey))
                            {
                                Log.Error("[DataSync] Global table '{TargetTable}' has no natural key configured in GlobalTableNaturalKeys. Skipping.", targetTable);
                                continue;
                            }
                            Log.Information("[DataSync] Global table '{TargetTable}' — using MERGE upsert (match key: {MatchKey})", targetTable, matchKey);
                            if (!dryRun)
                            {
                                int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                                await migrator.MergeGlobalTableAsync(
                                    sourceConnection, targetConnection, table, targetTable,
                                    matchKey, migrationBatch, userMapping, commandTimeoutOverride);
                                await DataSyncCheckpointHelper.MarkTableDoneAsync(targetConnection, MigrationConfig.GlobalTableCheckpointSentinel, MigrationConfig.GlobalTableCheckpointSentinel, targetTable);
                            }
                            else
                            {
                                Log.Information("[DryRun] Would MERGE global table '{TargetTable}' (match key: {MatchKey})", targetTable, matchKey);
                            }
                            continue;
                        }

                        // Fallback: skip non-global tables without TenantId that are already seeded
                        bool skipGlobalSinglePk = false;
                        if (pkInfo != null && pkInfo.PkColumnCount == 1 && !await GetTargetTableHasTenantIdAsync(targetTable))
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
                                Log.Information("[DataSync] Skipping global single-PK table '{TargetTable}' (no TenantId, already seeded).", targetTable);
                                ReportWriter.AddDataSyncTable(table, targetTable, 0, 0, "Already-seeded", null);
                                continue;
                            }
                        }

                        // Polly retry policy for transient SQL errors (deadlock, timeout)
                        var retryPolicy = Policy
                            .Handle<SqlException>(ex => ex.Number == 1205 || ex.Number == -2)
                            .WaitAndRetryAsync(3,
                                attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                                (exception, timeSpan, attempt, context) =>
                                {
                                    Log.Warning("[DataSync] Retry {Attempt}/3 for {TargetTable}: {ErrorMessage} (waiting {WaitSeconds}s)", attempt, targetTable, exception.Message, timeSpan.TotalSeconds);
                                });

                        try
                        {
                        await retryPolicy.ExecuteAsync(async () =>
                        {
                        if (tablesOnlyInMdc.Contains(targetTable))
                        {
                            var tenantDisplay = tgt.HasValue ? tgt.ToString() : "all";
                            if (pkInfo != null && pkInfo.PkColumnCount == 1 && (pkInfo.DataType == "int" || pkInfo.DataType == "bigint" || pkInfo.DataType == "uniqueidentifier"))
                            {
                                // MDC-only table with identity PK: use staging+MERGE+IdMapping to generate new IDs
                                // This prevents PK collisions when multiple tenants have overlapping identity ranges
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: MDC-only (staging + MERGE + IdMapping) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                var whereClause = (src.HasValue && await GetSourceTableHasTenantIdAsync(table)) ? " WHERE TenantId = @TenantId" : "";
                                int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                                if (commandTimeoutOverride.HasValue)
                                    Log.Information("   -> Using extended timeout: {TimeoutSeconds}s for this table.", commandTimeoutOverride.Value);
                                await migrator.InsertTableWithIdMappingAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, migrationBatch, tgt, whereClause, src, tgt, userMapping, mergeChunkSize, commandTimeoutOverride);
                            }
                            else if (pkInfo != null && pkInfo.PkColumnCount > 1)
                            {
                                // MDC-only table with composite PK
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: MDC-only (composite PK staging + INSERT) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                                var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                                await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable, pkColumnNames, fkColumns, src, tgt, userMapping);
                            }
                            else
                            {
                                // MDC-only table with no PK or natural key: direct copy
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: MDC-only (direct copy, no IdMapping) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                await migrator.MigrateTableAsync(table, isNewTable: false, targetTableName: targetTable, sourceTenantId: src, targetTenantId: tgt, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                            }
                            Log.Information("   -> Done: {Table}", table);
                        }
                        else
                        {
                            if (pkInfo == null)
                            {
                                var tenantDisplay = src.HasValue ? src.ToString() : "all";
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Direct MigrateTable (no PK/IdMapping) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                await migrator.MigrateTableAsync(table, isNewTable: isNew, targetTableName: targetTable, sourceTenantId: src, targetTenantId: tgt, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                                Log.Information("   -> Done: {Table}", table);
                            }
                            else if (pkInfo.PkColumnCount == 1 && pkInfo.DataType != "int" && pkInfo.DataType != "bigint" && pkInfo.DataType != "uniqueidentifier")
                            {
                                var tenantDisplay = src.HasValue ? src.ToString() : "all";
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Natural PK (insert missing only) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                await migrator.MigrateTableNaturalPkAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, src, tgt, userMapping);
                                Log.Information("   -> Done: {Table}", table);
                            }
                            else if (pkInfo.PkColumnCount > 1)
                            {
                                var tenantDisplay = src.HasValue ? src.ToString() : "all";
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Composite PK (staging -> INSERT with IdMapping JOIN) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                                var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                                await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable, pkColumnNames, fkColumns, src, tgt, userMapping);
                                Log.Information("   -> Done: {Table}", table);
                            }
                            else
                            {
                                var tenantDisplay = tgt.HasValue ? tgt.ToString() : "null";
                                Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Staging + MERGE + IdMapping (single PK int/bigint/guid) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                var whereClause = (src.HasValue && await GetSourceTableHasTenantIdAsync(table)) ? " WHERE TenantId = @TenantId" : "";
                                int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                                if (commandTimeoutOverride.HasValue)
                                    Log.Information("   -> Using extended timeout: {TimeoutSeconds}s for this table.", commandTimeoutOverride.Value);
                                await migrator.InsertTableWithIdMappingAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, migrationBatch, tgt, whereClause, src, tgt, userMapping, mergeChunkSize, commandTimeoutOverride);
                                Log.Information("   -> Done: {Table}", table);
                            }
                        }

                        totalTablesProcessed++;
                        // Mark checkpoint ONLY after successful completion for this tenant + table
                        if (src.HasValue && tgt.HasValue)
                        {
                            await DataSyncCheckpointHelper.MarkTableDoneAsync(targetConnection, src.Value, tgt.Value, targetTable);
                        }
                        }); // end retryPolicy.ExecuteAsync
                        }
                        catch (Exception ex)
                        {
                            Log.Error("[Error] Table {TargetTable} failed after retries: {ErrorMessage}", targetTable, ex.Message);
                            Log.Information("[DataSync] Skipping checkpoint for {TargetTable} — will retry on resume", targetTable);
                            ReportWriter.AddDataSyncTable(table, targetTable, 0, 0, "Failed", ex.Message);
                            // Continue to next table
                        }
                    }

                    await FkConstraintHelper.UpdateFkFromIdMappingAsync(targetConnection, migrationBatch, tgt, tgt);
                }
                ResourceMonitor.Stop();
                ResourceMonitor.LogSummary();
                ResourceMonitor.SaveReport(ReportWriter.ReportDirectory ?? "output");

                overallSw.Stop();
                var elapsedSeconds = overallSw.Elapsed.TotalSeconds;
                Log.Information("[DataSync] Migration summary: {TablesProcessed} tables, {RowsMigrated} rows in {ElapsedMs}ms ({ElapsedSeconds:F1}s)", totalTablesProcessed, migrator.TotalRowsMigrated, overallSw.ElapsedMilliseconds, elapsedSeconds);
                ReportWriter.WriteDataSyncReport(migrator.TotalRowsMigrated, overallSw.ElapsedMilliseconds);
                }
                finally
                {
                    if (allTenantPairs != null)
                    {
                        await FkConstraintHelper.EnableAllFkAsync(targetConnection);
                        Log.Information("[DataSync] Re-enabled all foreign keys.");
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
                            Log.Information("[DataSync] All tenants completed. Re-enabled all foreign keys.");
                            if (File.Exists(completedPath)) File.Delete(completedPath);
                        }
                        else
                            Log.Information("[DataSync] Single-tenant run completed. Completed: {CompletedCount}, Total source tenants: {TotalCount}. FK left disabled until all tenants are synced.", existingIds.Count, allSourceIds.Count);
                    }

                    await targetConnection.ExecuteAsync("EXEC sp_releaseapplock @Resource = 'LogisticsDbMerger_DataSync', @LockOwner = 'Session'");
                }
            }
            else
            {
                var dryRunTenants = allTenantPairs != null
                    ? allTenantPairs
                    : new List<(int SourceId, int TargetId, string DisplayName)> { (sourceTenantId!.Value, targetTenantId!.Value, tenantName ?? "Single") };

                var abpMergeStrategy = AbpMergeStrategyMap;

                foreach (var (curSourceId, curTargetId, curDisplayName) in dryRunTenants)
                {
                    if (allTenantPairs != null)
                        Log.Information("[DryRun] --- Tenant: {DisplayName} (SourceId: {SourceId} -> TargetId: {TargetId}) ---", curDisplayName, curSourceId, curTargetId);
                    foreach (var table in orderedTables)
                    {
                        if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__")) continue;

                        // ABP merge strategy check (FR42-44) — DryRun path
                        if (abpMergeStrategy.TryGetValue(table, out var abpStrategyDry))
                        {
                            if (abpStrategyDry == "skip")
                            {
                                Log.Information("[DryRun] Would skip {Table} (managed by target)", table);
                                continue;
                            }
                            Log.Information("[DryRun] ABP table {Table}: strategy={AbpStrategy}", table, abpStrategyDry);
                        }

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
                                Log.Information("[SmartMerge] Applied explicit mapping: {SourceTable} (MDC) -> {TargetTable} (ADC)", table, targetTable);
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                            else
                                Log.Information("[Map] Explicit target {TargetTable} missing. Treating as new.", targetTable);
                        }
                        else if (isNew)
                        {
                            string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                            if (bestMatch != null)
                            {
                                targetTable = bestMatch;
                                isNew = false;
                                Log.Information("[SmartMerge] Detected match: {SourceTable} (MDC) -> {TargetTable} (ADC)", table, targetTable);
                                await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                            }
                        }

                        if (!isNew && targetTable.Equals(table, StringComparison.OrdinalIgnoreCase))
                            await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);

                        Log.Information("[DryRun] Would migrate {SourceTable} -> {TargetTable} (Tenant: {SourceId} -> {TargetId})", table, targetTable, curSourceId, curTargetId);
                    }
                }
            }
        }

        static async Task RunListTablesOnlyInMdcAndCreateStructureAsync(string sourceConn, string targetConn, bool dryRun)
        {
            Log.Information("\n--> [Option 7] Tables only in MDC: list (console + file) and create structure in ADC");
            var path = Helper.MdcOnlyTablesFilePath;
            var list = await Helper.GetTablesOnlyInMdcAsync(sourceConn, targetConn, path, writeToFile: true);
            Log.Information("Tables only in MDC (after skip rules): {Count}", list.Count);
            Log.Information("Output file: {FilePath}", Path.GetFullPath(path));
            for (int i = 0; i < list.Count; i++)
                Log.Information("  {Number}. {TableName}", i + 1, list[i]);
            if (list.Count == 0)
            {
                Log.Information("Nothing to create.");
                return;
            }
            if (dryRun)
            {
                Log.Information("[DryRun] Would create table structure in ADC for the above.");
                return;
            }
            var schemaSync = new SchemaSync(sourceConn, targetConn);
            foreach (var table in list)
            {
                if (ExplicitTableMappings.ContainsKey(table))
                {
                    Log.Information("[Skip] {Table} (explicitly mapped to existing table)", table);
                    continue;
                }
                await schemaSync.SyncTableAsync(table);
                Log.Information("  Created [dbo].[{Table}]", table);
            }
            Log.Information("Done.");
        }

        /// <summary>
        /// Xóa data migration trên ADC dựa vào IdMapping (chỉ xóa các dòng có NewId trong IdMapping).
        /// Tận dụng index (TableName, ColumnName) INCLUDE (NewId) trên bảng IdMapping.
        /// </summary>
        static async Task RunClearMigrationDataAsync(string targetConnStr)
        {
            Log.Information("\n--> [Option 10] Clear migration data (delete rows in ADC based on IdMapping)");
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
            int errors = 0;

            try
            {
                // Process each IdMapping table — errors per table are caught and logged, not thrown
                foreach (var mappingTable in new[] { "IdMappingInt", "IdMappingBigInt", "IdMappingGuid" })
                {
                    try
                    {
                        totalDeleted += await ProcessIdMappingTableAsync(conn, mappingTable, filterBatch, filterTenantId);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("[Clear] Error processing {MappingTable}: {ErrorMessage}. Continuing with next...", mappingTable, ex.Message);
                        errors++;
                    }
                }

                // When clearing a specific tenant, also clear global table rows (TenantId IS NULL in IdMapping)
                if (filterTenantId.HasValue)
                {
                    Log.Warning("[Clear] Also clearing global/shared table rows (TenantId IS NULL in IdMapping). These will be re-imported on next migration run.");
                    foreach (var mappingTable in new[] { "IdMappingInt", "IdMappingBigInt", "IdMappingGuid" })
                    {
                        try
                        {
                            totalDeleted += await ProcessIdMappingTableAsync(conn, mappingTable, filterBatch, filterTenantId: null, globalRowsOnly: true);
                        }
                        catch (Exception ex)
                        {
                            Log.Error("[Clear] Error processing global rows in {MappingTable}: {ErrorMessage}. Continuing...", mappingTable, ex.Message);
                            errors++;
                        }
                    }
                }
            }
            finally
            {
                // Always re-enable FKs and clear checkpoints, even if errors occurred
                try
                {
                    await FkConstraintHelper.EnableAllFkAsync(conn);
                }
                catch (Exception ex)
                {
                    Log.Error("[Clear] Error re-enabling FKs: {ErrorMessage}", ex.Message);
                }

                Log.Information("\n[Option 10] Done. Total rows deleted: {TotalDeleted}. Errors: {ErrorCount}.", totalDeleted, errors);

                // Clear DataSync checkpoints — per-tenant if filtering, all if not
                try
                {
                    if (filterTenantId.HasValue)
                    {
                        await DataSyncCheckpointHelper.ClearByTenantAsync(conn, filterTenantId.Value);
                        await DataSyncCheckpointHelper.ClearByTenantAsync(conn, MigrationConfig.GlobalTableCheckpointSentinel);
                        Log.Information("[Option 10] Cleared DataSyncCheckpoint for TenantId {TenantId} and global tables.", filterTenantId.Value);
                    }
                    else
                    {
                        await DataSyncCheckpointHelper.ClearAllAsync(conn);
                        Log.Information("[Option 10] Cleared all DataSyncCheckpoint rows.");
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("[Clear] Error clearing checkpoints: {ErrorMessage}", ex.Message);
                }
            }
        }

        static async Task RunEnableFkAsync(string targetConnStr)
        {
            Log.Information("\n--> [Option 7] Enable FK (re-enable all foreign keys on target)");
            using var conn = new SqlConnection(targetConnStr);
            await conn.OpenAsync();
            await FkConstraintHelper.EnableAllFkAsync(conn);
            Log.Information("[Option 7] Done. All foreign keys on target have been re-enabled.");
        }

        /// <summary>
        /// For one IdMapping table: get distinct (TableName, ColumnName), delete from data tables by NewId (in batches), then delete from IdMapping.
        /// Uses extended timeout (3600s) for large IdMapping / data tables.
        /// </summary>
        const int Option8CommandTimeoutSeconds = 3600;

        static async Task<int> ProcessIdMappingTableAsync(SqlConnection conn, string mappingTable, string? filterBatch, int? filterTenantId, bool globalRowsOnly = false)
        {
            var tableList = await GetDistinctTableColumnFromIdMappingAsync(conn, mappingTable, filterBatch, filterTenantId, globalRowsOnly);
            if (tableList.Count == 0) return 0;

            var dboTables = (await conn.QueryAsync<string>("SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo')"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            const int deleteBatchSize = 50000;
            int totalDeleted = 0;
            foreach (var (tableName, columnName) in tableList)
            {
                if (!dboTables.Contains(tableName))
                {
                    Log.Information("  [Skip] Table not found: {TableName}", tableName);
                    continue;
                }

                var tableEsc = tableName.Replace("]", "]]");
                var colEsc = columnName.Replace("]", "]]");

                var subWhere = " TableName = @TableName AND ColumnName = @ColumnName";
                if (!string.IsNullOrEmpty(filterBatch)) subWhere += " AND MigrationBatch = @Batch";
                if (globalRowsOnly)
                    subWhere += " AND TenantId IS NULL";
                else if (filterTenantId.HasValue)
                    subWhere += " AND TenantId = @TenantId";

                var prm = new { TableName = tableName, ColumnName = columnName, Batch = filterBatch, TenantId = filterTenantId, BatchSize = deleteBatchSize };
                var deleteDataSql = $@"DELETE TOP (@BatchSize) FROM [dbo].[{tableEsc}] WHERE [{colEsc}] IN (SELECT NewId FROM [dbo].[{mappingTable}] WHERE{subWhere})";

                int tableDeleted = 0;
                int deleted;
                do
                {
                    deleted = await conn.ExecuteAsync(deleteDataSql, prm, commandTimeout: Option8CommandTimeoutSeconds);
                    tableDeleted += deleted;
                    // Log progress for large tables (every 500K rows)
                    if (tableDeleted > 0 && tableDeleted % 500000 < deleteBatchSize)
                        Log.Information("  [{TableName}] {Deleted:N0} rows deleted so far...", tableName, tableDeleted);
                } while (deleted == deleteBatchSize);

                if (tableDeleted > 0)
                {
                    Log.Information("  Deleted {DeletedCount:N0} row(s) from [dbo].[{TableName}]{GlobalTag}", tableDeleted, tableName, globalRowsOnly ? " (global)" : "");
                    totalDeleted += tableDeleted;
                }

                var deleteMappingSql = $@"DELETE FROM [dbo].[{mappingTable}] WHERE TableName = @TableName AND ColumnName = @ColumnName";
                if (!string.IsNullOrEmpty(filterBatch)) deleteMappingSql += " AND MigrationBatch = @Batch";
                if (globalRowsOnly)
                    deleteMappingSql += " AND TenantId IS NULL";
                else if (filterTenantId.HasValue)
                    deleteMappingSql += " AND TenantId = @TenantId";
                await conn.ExecuteAsync(deleteMappingSql, prm, commandTimeout: Option8CommandTimeoutSeconds);
            }
            return totalDeleted;
        }

        static async Task<List<(string TableName, string ColumnName)>> GetDistinctTableColumnFromIdMappingAsync(SqlConnection conn, string mappingTable, string? filterBatch, int? filterTenantId, bool globalRowsOnly = false)
        {
            var sql = $"SELECT DISTINCT TableName, ColumnName FROM [dbo].[{mappingTable}] WHERE 1=1";
            if (!string.IsNullOrEmpty(filterBatch)) sql += " AND MigrationBatch = @Batch";
            if (globalRowsOnly)
                sql += " AND TenantId IS NULL";
            else if (filterTenantId.HasValue)
                sql += " AND TenantId = @TenantId";
            var rows = await conn.QueryAsync<(string TableName, string ColumnName)>(sql, new { Batch = filterBatch, TenantId = filterTenantId },
                commandTimeout: Option8CommandTimeoutSeconds);

            // Order by reverse MigrationConfig tier order (delete children before parents)
            var reverseOrder = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var tierTables = MigrationConfig.TableOrder;
            for (int i = 0; i < tierTables.Count; i++)
                reverseOrder[tierTables[i]] = tierTables.Count - i; // higher = delete first
            return rows.OrderByDescending(r => reverseOrder.TryGetValue(r.TableName, out var order) ? order : 0).ToList();
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

        /// <summary>
        /// ABP system table merge strategy (FR42-44): per-table handling for ABP framework tables.
        /// Shared between RunDataSync (live + dry-run) and RunDataSyncByTier.
        /// </summary>
        private static readonly Dictionary<string, string> AbpMergeStrategyMap = new(StringComparer.OrdinalIgnoreCase)
        {
            // SKIP — managed by target
            { "Tenants", "skip" },
            { "Users", "skip" },               // Handled by SyncUsersAsync

            // MERGE PER-TENANT — skip host records (TenantId = NULL), remap TenantId
            { "Roles", "merge-per-tenant" },
            { "Permissions", "merge-per-tenant" },
            { "Setting", "merge-per-tenant" },  // Singular — verified in mdc_prod.sql

            // MERGE PER-TENANT WITH USER MAPPING — skip host, remap TenantId + UserId
            { "UserLogins", "merge-per-tenant-with-user-mapping" },
            { "UserClaims", "merge-per-tenant-with-user-mapping" },
            { "UserRoles", "merge-per-tenant-with-user-mapping" },
            { "UserOrganizationUnits", "merge-per-tenant-with-user-mapping" },

            // MERGE PER-TENANT — additional ABP tables (FR42-44)
            { "AuditLogs", "merge-per-tenant" },
            { "Notifications", "merge-per-tenant" },
            { "Features", "merge-per-tenant" },
        };

        static async Task RunValidation(string sourceConn, string targetConn)
        {
            Log.Information("\n--> [Step 4] Validation");

            Console.Write("\n[Input] Enter Tenant Name for Validation (or press Enter for ALL): ");
            string tenantName = Console.ReadLine()?.Trim();
            int? sourceTenantId = null;
            int? targetTenantId = null;

            if (!string.IsNullOrEmpty(tenantName))
            {
               try
               {
                   using var source = new SqlConnection(sourceConn);
                   sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                   if (sourceTenantId == null) sourceTenantId = await source.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                   using var target = new SqlConnection(targetConn);
                   targetTenantId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                   if (targetTenantId == null) targetTenantId = await target.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                   Log.Information("[Validator] Source TenantId: {SourceTenantId}, Target TenantId: {TargetTenantId}", sourceTenantId, targetTenantId);
                   if (sourceTenantId == null) { Log.Warning("[Validator] Tenant '{TenantName}' not found in source — will validate all rows", tenantName); }
                   if (targetTenantId == null) { Log.Warning("[Validator] Tenant '{TenantName}' not found in target — will validate all rows", tenantName); }
               }
               catch(Exception ex) { Log.Error("[Error] Error resolving tenant: {ErrorMessage}", ex.Message); return; }
            }

            var validator = new Validator(sourceConn, targetConn);
            await validator.RunValidationAsync(sourceTenantId, targetTenantId);
        }

        static async Task RunPreFlight(string sourceConn, string targetConn)
        {
            Log.Information("\n--> [Pre-Flight] Running all safety checks...");

            var (overlaps, failed, safe, skipped) = await PreFlightValidator.RunIdentityRangeCheckAsync(sourceConn, targetConn);
            var partitionRemaps = await PreFlightValidator.RunPartitionFileGroupCheckAsync(sourceConn, targetConn);
            var (collationMismatch, sourceCollation, targetCollation) = await PreFlightValidator.RunCollationCheckAsync(sourceConn, targetConn);

            int totalIssues = overlaps + failed + partitionRemaps + (collationMismatch ? 1 : 0);
            if (totalIssues == 0)
                Log.Information("[PreFlight] All checks passed — safe to proceed with migration.");
            else
                Log.Warning("[PreFlight] {TotalIssues} issue(s) found — review findings above before proceeding.", totalIssues);

            ReportWriter.WritePreFlightReport(overlaps, failed, safe, skipped, partitionRemaps, collationMismatch, sourceCollation, targetCollation);
        }

        static async Task RunRollback(string targetConn)
        {
            var file = RollbackLogger.GetCurrentFilePath();
            if (string.IsNullOrEmpty(file)) file = Path.Combine(RollbackLogger.GetRunFolder(), "rollback_generic.sql");

            // List available rollback runs
            var rollbacksDir = Path.Combine("output", "rollbacks");
            if (Directory.Exists(rollbacksDir))
            {
                var runs = Directory.GetDirectories(rollbacksDir).OrderDescending().ToArray();
                if (runs.Length > 0)
                {
                    Log.Information("\nAvailable rollback runs:");
                    foreach (var run in runs)
                    {
                        var files = Directory.GetFiles(run, "*.sql");
                        Log.Information("  {RunFolder} ({FileCount} file(s))", Path.GetFileName(run), files.Length);
                    }
                }
            }

            Log.Information("\nCurrent Rollback File: {File}", file);
            Log.Information("Enter filename/path to execute (or press Enter for current):");
            string input = Console.ReadLine();
            string targetFile = string.IsNullOrWhiteSpace(input) ? file : input;

            if (!File.Exists(targetFile))
            {
                Log.Error("[Error] File not found: {TargetFile}", targetFile);
                return;
            }

            Log.Information("Executing rollback script: {TargetFile}...", targetFile);
            string script = File.ReadAllText(targetFile);

            // Split on GO batch separators (GO is not T-SQL, it's a batch separator)
            var batches = System.Text.RegularExpressions.Regex.Split(script, @"^\s*GO\s*$",
                System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

            using var conn = new SqlConnection(targetConn);
            int executed = 0, failed = 0;
            foreach (var batch in batches)
            {
                var trimmed = batch.Trim();
                if (string.IsNullOrEmpty(trimmed)) continue;
                try
                {
                    await conn.ExecuteAsync(trimmed, commandTimeout: 120);
                    executed++;
                }
                catch (Exception ex)
                {
                    Log.Error("[Rollback] Batch failed: {ErrorMessage}", ex.Message);
                    failed++;
                }
            }
            Log.Information("[Rollback] Complete: {Executed} batch(es) executed, {Failed} failed.", executed, failed);
        }

        static async Task RunFullMigration(string sourceConn, string targetConn, int batchSize, bool dryRun, string? tenantName, int mergeChunkSize = 0,
            string[]? veryHighTimeoutTables = null, int veryHighTimeoutSeconds = 0,
            string[]? highTimeoutTables = null, int highTimeoutSeconds = 0,
            int metricsIntervalSeconds = 5)
        {
             // Initialize report folder for this run
             ReportWriter.Initialize(tenantName ?? "ALL", DateTime.UtcNow);

             // Resolve tenant IDs early so reports show correct values
             if (!string.IsNullOrEmpty(tenantName))
             {
                 using var srcConn = new SqlConnection(sourceConn);
                 using var tgtConn = new SqlConnection(targetConn);
                 var srcId = await srcConn.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName })
                          ?? await srcConn.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });
                 var tgtId = await tgtConn.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName })
                          ?? await tgtConn.QueryFirstOrDefaultAsync<int?>("SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });
                 if (srcId.HasValue)
                     ReportWriter.SetDataSyncTenantInfo(srcId.Value, tgtId ?? 0, tenantName);
             }

             // Inject tenant name into Console.In so RunDataSync/RunValidation can read it non-interactively
             // Each method calls Console.ReadLine() for tenant name — feed it the value (or empty for ALL)
             var tenantInput = tenantName ?? "";
             var originalIn = Console.In;
             try
             {
                 // RunDataSync reads tenant name, RunValidation reads tenant name — provide both
                 var autoInput = new System.IO.StringReader(tenantInput + Environment.NewLine + tenantInput + Environment.NewLine);
                 Console.SetIn(autoInput);

                 await RunPreFlight(sourceConn, targetConn);
                 await RunSchemaSync(sourceConn, targetConn, dryRun);
                 await RunDataSync(sourceConn, targetConn, batchSize, dryRun, mergeChunkSize, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds, metricsIntervalSeconds);
                 // Re-enable FKs after data sync (RunDataSync leaves them disabled for single-tenant runs)
                 await RunEnableFkAsync(targetConn);
                 ReportWriter.WriteFkReport(FkConstraintHelper.LastDisabledCount, FkConstraintHelper.LastEnabledCount);
                 await RunValidation(sourceConn, targetConn);
             }
             finally
             {
                 Console.SetIn(originalIn);
                 try { ReportWriter.WriteSummaryReport(); } catch { /* do not mask original exception */ }
             }
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
            int highTimeoutSeconds,
            int metricsIntervalSeconds = 5)
        {
            Log.Information("\n--> [Step 3b] Data Sync by Tier");
            RollbackLogger.Initialize("data_tier");

            // 1. Input tiers
            Console.Write("\n[Input] Enter Tiers to run (e.g. 1,2,3 or all): ");
            var tierInput = Console.ReadLine()?.Trim();
            var tiersToRun = ParseTierInput(tierInput);
            if (tiersToRun.Count == 0)
            {
                Log.Warning("[DataSyncByTier] No valid tiers selected. Abort.");
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
                        Log.Error("[Error] Source Tenant '{TenantName}' not found.", tenantName);
                        return;
                    }
                    Log.Information("[TenantFilter] Resolved Source ID: {SourceTenantId}", sourceTenantId);

                    var existingTargetId = await target.QueryFirstOrDefaultAsync<int?>(
                        "SELECT Id FROM Tenants WHERE TenancyName = @Name", new { Name = tenantName });
                    if (existingTargetId == null)
                        existingTargetId = await target.QueryFirstOrDefaultAsync<int?>(
                            "SELECT Id FROM Tenants WHERE Name = @Name", new { Name = tenantName });

                    if (existingTargetId != null)
                    {
                        targetTenantId = existingTargetId;
                        Log.Information("[TenantMap] Found existing Target Tenant '{TenantName}' (ID: {TargetTenantId}). Merging into it.", tenantName, targetTenantId);
                    }
                    else
                    {
                        if (!dryRun)
                        {
                            Log.Information("[TenantMap] Creating new Tenant '{TenantName}' in Target...", tenantName);
                            var sourceTenantRow = await source.QuerySingleAsync<dynamic>(
                                "SELECT * FROM Tenants WHERE Id = @Id", new { Id = sourceTenantId });
                            var props = (IDictionary<string, object>)sourceTenantRow;
                            var cols = props.Keys.Where(k => k != "Id").ToList();
                            var vals = cols.Select(k => "@" + k).ToList();
                            string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols.Select(c => "[" + c.Replace("]", "]]") + "]"))}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                            targetTenantId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                            Log.Information("[TenantMap] Created Target Tenant. New ID: {TargetTenantId}", targetTenantId);
                        }
                        else
                        {
                            Log.Information("[DryRun] Would Create Tenant '{TenantName}' in Target.", tenantName);
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
                        Log.Information("[DataSyncByTier] No tenants found in Source.");
                        return;
                    }
                    Log.Information("[TenantFilter] ALL tenants: {TenantCount} tenant(s) in Source.", sourceTenants.Count);
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
                            Log.Information("[TenantMap] '{TenantName}' Source ID {SourceId} -> Target ID {TargetId} (existing).", nameForLookup, row.Id, targetId);
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
                                string insertSql = $"INSERT INTO Tenants ({string.Join(",", cols.Select(c => "[" + c.Replace("]", "]]") + "]"))}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as int);";
                                targetId = await target.ExecuteScalarAsync<int>(insertSql, (object)sourceTenantRow);
                                Log.Information("[TenantMap] '{TenantName}' Source ID {SourceId} -> Created Target ID {TargetId}.", nameForLookup, row.Id, targetId);
                            }
                            else
                            {
                                targetId = row.Id;
                                Log.Information("[DryRun] Would create Tenant '{TenantName}' in Target (mock TargetId: {TargetId}).", nameForLookup, targetId);
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
            // Use a local tier map to avoid mutating the static MigrationConfig.TierTables
            var tierMap = new Dictionary<int, List<string>>(MigrationConfig.TierTables);
            if (tier9Tables.Count > 0)
            {
                tierMap[9] = tier9Tables;
            }

            // 5. Build danh sách bảng thuộc các Tier đã chọn
            var tierTables = new List<string>();
            foreach (var tier in tiersToRun.OrderBy(x => x))
            {
                if (!tierMap.TryGetValue(tier, out var tables)) continue;
                foreach (var t in tables)
                    if (sourceTables.Contains(t, StringComparer.OrdinalIgnoreCase))
                        tierTables.Add(t);
            }

            Log.Information("[DataSyncByTier] Tables to migrate in selected tiers: {TableCount}", tierTables.Count);
            int totalTablesProcessed = 0;
            var overallSw = Stopwatch.StartNew();

            // 6. Tables only in MDC – chỉ đọc từ file (đã ghi ở Option 1 trước khi thay đổi cấu trúc ADC)
            var fromFileTier = await Helper.ReadTableListFromNumberedFileAsync(Helper.MdcOnlyTablesFilePath);
            var tablesOnlyInMdc = fromFileTier.Count > 0
                ? fromFileTier.ToHashSet(StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (fromFileTier.Count > 0)
                Log.Information("[DataSyncByTier] Tables only in MDC: {Count} (from file {FilePath})", tablesOnlyInMdc.Count, Helper.MdcOnlyTablesFilePath);
            else
                Log.Information("[DataSyncByTier] Tables only in MDC: 0 (file empty or missing; run Option 1 first to generate {FilePath})", Helper.MdcOnlyTablesFilePath);

            // 7. Smart User Sync (dùng chung với Option 3)
            var userMapping = new Dictionary<long, long>();
            if (allTenantPairs != null)
            {
                foreach (var (srcId, tgtId, displayName) in allTenantPairs)
                {
                    Log.Information("[Users] Syncing users for tenant: {DisplayName} (Source: {SourceId} -> {TargetId})", displayName, srcId, tgtId);
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

                ResourceMonitor.Start(metricsIntervalSeconds, sourceConn, targetConn);

                // Concurrency guard: prevent two instances from running simultaneously
                var lockResult = await targetConnection.ExecuteScalarAsync<int>(
                    "EXEC sp_getapplock @Resource = 'LogisticsDbMerger_DataSync', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0");
                if (lockResult < 0)
                {
                    Log.Error("[Error] Another instance of the migration tool is running against this database. Aborting.");
                    return;
                }
                Log.Information("[DataSync] Acquired exclusive application lock.");

                await IdMappingSetup.CreateIdMappingTablesIfNotExistsAsync(targetConnection);
                await DataSyncCheckpointHelper.EnsureTableAsync(targetConnection);
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
                        Log.Information("[DataSyncByTier] --- Tenant: {DisplayName} (SourceId: {SourceId} -> TargetId: {TargetId}) ---", curDisplayName, curSourceId, curTargetId);

                        foreach (var table in tierTables)
                        {
                            if (table == "sysdiagrams" || table == "Tenants" || table == "Users" || table.StartsWith("__"))
                                continue;

                            // ABP merge strategy check (FR42-44) — same as RunDataSync
                            if (AbpMergeStrategyMap.TryGetValue(table, out var abpStrategy))
                            {
                                if (abpStrategy == "skip")
                                {
                                    Log.Information("[DataSyncByTier] Skipped {Table} (managed by target)", table);
                                    continue;
                                }

                                if (!src.HasValue)
                                {
                                    Log.Warning("[DataSyncByTier] WARNING: ABP table {Table} requires per-tenant migration. Skipping in all-tenants mode.", table);
                                    continue;
                                }

                                Log.Information("[DataSyncByTier] ABP table {Table}: strategy={AbpStrategy}, tenant={SourceId}->{TargetId}", table, abpStrategy, src, tgt);
                            }

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
                                    Log.Information("[SmartMerge] Applied explicit mapping: {SourceTable} (MDC) -> {TargetTable} (ADC)", table, targetTable);
                                    await schemaSync.SyncTableSchemaAsync(table, targetTable, dryRun);
                                }
                                else
                                    Log.Information("[Map] Explicit target {TargetTable} missing. Treating as new.", targetTable);
                            }
                            else if (isNew)
                            {
                                string? bestMatch = GetBestFuzzyMatch(table, existingAdcTables);
                                if (bestMatch != null)
                                {
                                    targetTable = bestMatch;
                                    isNew = false;
                                    Log.Information("[SmartMerge] Detected match: {SourceTable} (MDC) -> {TargetTable} (ADC)", table, targetTable);
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
                                    Log.Information("[Checkpoint] Skipping table '{TargetTable}' for tenant {SourceId}->{TargetId} (already completed).", targetTable, src, tgt);
                                    continue;
                                }
                            }

                            // Global table MERGE upsert: route to MergeGlobalTableAsync
                            if (MigrationConfig.GlobalTables.Contains(targetTable))
                            {
                                if (await DataSyncCheckpointHelper.IsTableDoneAsync(targetConnection, MigrationConfig.GlobalTableCheckpointSentinel, MigrationConfig.GlobalTableCheckpointSentinel, targetTable))
                                {
                                    Log.Information("[DataSyncByTier] Skipping global table '{TargetTable}' (already merged this run).", targetTable);
                                    continue;
                                }
                                if (!MigrationConfig.GlobalTableNaturalKeys.TryGetValue(targetTable, out var matchKey))
                            {
                                Log.Error("[DataSync] Global table '{TargetTable}' has no natural key configured in GlobalTableNaturalKeys. Skipping.", targetTable);
                                continue;
                            }
                                Log.Information("[DataSyncByTier] Global table '{TargetTable}' — using MERGE upsert (match key: {MatchKey})", targetTable, matchKey);
                                int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                                await migrator.MergeGlobalTableAsync(
                                    sourceConnection, targetConnection, table, targetTable,
                                    matchKey, migrationBatch, userMapping, commandTimeoutOverride);
                                await DataSyncCheckpointHelper.MarkTableDoneAsync(targetConnection, MigrationConfig.GlobalTableCheckpointSentinel, MigrationConfig.GlobalTableCheckpointSentinel, targetTable);
                                continue;
                            }

                            // Fallback: skip non-global tables without TenantId that are already seeded
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
                                    Log.Information("[DataSyncByTier] Skipping global single-PK table '{TargetTable}' (no TenantId, already seeded).", targetTable);
                                    continue;
                                }
                            }

                            // Polly retry policy for transient SQL errors (deadlock, timeout)
                            var retryPolicy = Policy
                                .Handle<SqlException>(ex => ex.Number == 1205 || ex.Number == -2)
                                .WaitAndRetryAsync(3,
                                    attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                                    (exception, timeSpan, attempt, context) =>
                                    {
                                        Log.Warning("[DataSyncByTier] Retry {Attempt}/3 for {TargetTable}: {ErrorMessage} (waiting {WaitSeconds}s)", attempt, targetTable, exception.Message, timeSpan.TotalSeconds);
                                    });

                            try
                            {
                            await retryPolicy.ExecuteAsync(async () =>
                            {
                            if (tablesOnlyInMdc.Contains(targetTable))
                            {
                                var tenantDisplay = tgt.HasValue ? tgt.ToString() : "all";
                                if (pkInfo != null && pkInfo.PkColumnCount == 1 && (pkInfo.DataType == "int" || pkInfo.DataType == "bigint" || pkInfo.DataType == "uniqueidentifier"))
                                {
                                    // MDC-only table with identity PK: use staging+MERGE+IdMapping to generate new IDs
                                    // This prevents PK collisions when multiple tenants have overlapping identity ranges
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: MDC-only (staging + MERGE + IdMapping) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                    var whereClause = (src.HasValue && await GetSourceTableHasTenantIdAsync(table)) ? " WHERE TenantId = @TenantId" : "";
                                    int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);
                                    if (commandTimeoutOverride.HasValue)
                                        Log.Information("   -> Using extended timeout: {TimeoutSeconds}s for this table.", commandTimeoutOverride.Value);
                                    await migrator.InsertTableWithIdMappingAsync(sourceConnection, targetConnection, table, targetTable, pkInfo, migrationBatch, tgt, whereClause, src, tgt, userMapping, mergeChunkSize, commandTimeoutOverride);
                                }
                                else if (pkInfo != null && pkInfo.PkColumnCount > 1)
                                {
                                    // MDC-only table with composite PK
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: MDC-only (composite PK staging + INSERT) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                    var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                                    var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                                    await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable, pkColumnNames, fkColumns, src, tgt, userMapping);
                                }
                                else
                                {
                                    // MDC-only table with no PK or natural key: direct copy
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: MDC-only (direct copy, no IdMapping) | TenantId: {TenantId}", table, targetTable, tenantDisplay);
                                    await migrator.MigrateTableAsync(table, isNewTable: false, targetTableName: targetTable, sourceTenantId: src, targetTenantId: tgt, userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                                }
                                Log.Information("   -> Done: {Table}", table);
                            }
                            else
                            {
                                if (pkInfo == null)
                                {
                                    var tenantDisplay2 = src.HasValue ? src.ToString() : "all";
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Direct MigrateTable (no PK/IdMapping) | TenantId: {TenantId}", table, targetTable, tenantDisplay2);
                                    await migrator.MigrateTableAsync(table, isNewTable: isNew, targetTableName: targetTable,
                                        sourceTenantId: src, targetTenantId: tgt,
                                        userMapping: userMapping, externalSourceConn: sourceConnection, externalTargetConn: targetConnection);
                                    Log.Information("   -> Done: {Table}", table);
                                }
                                else if (pkInfo.PkColumnCount == 1 &&
                                         pkInfo.DataType != "int" &&
                                         pkInfo.DataType != "bigint" &&
                                         pkInfo.DataType != "uniqueidentifier")
                                {
                                    var tenantDisplay2 = src.HasValue ? src.ToString() : "all";
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Natural PK (insert missing only) | TenantId: {TenantId}", table, targetTable, tenantDisplay2);
                                    await migrator.MigrateTableNaturalPkAsync(sourceConnection, targetConnection, table, targetTable,
                                        pkInfo, src, tgt, userMapping);
                                    Log.Information("   -> Done: {Table}", table);
                                }
                                else if (pkInfo.PkColumnCount > 1)
                                {
                                    var tenantDisplay2 = src.HasValue ? src.ToString() : "all";
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Composite PK (staging -> INSERT with IdMapping JOIN) | TenantId: {TenantId}", table, targetTable, tenantDisplay2);
                                    var pkColumnNames = await DataMigrator.GetPkColumnNamesAsync(targetConnection, targetTable);
                                    var fkColumns = await DataMigrator.GetFkColumnsForTableAsync(targetConnection, targetTable);
                                    await migrator.MigrateCompositeKeyTableAsync(sourceConnection, targetConnection, table, targetTable,
                                        pkColumnNames, fkColumns, src, tgt, userMapping);
                                    Log.Information("   -> Done: {Table}", table);
                                }
                                else
                                {
                                    var tenantDisplay2 = tgt.HasValue ? tgt.ToString() : "null";
                                    Log.Information("[Insert] Table: {SourceTable} -> [dbo].[{TargetTable}] | Mode: Staging + MERGE + IdMapping (single PK int/bigint/guid) | TenantId: {TenantId}", table, targetTable, tenantDisplay2);
                                    var whereClause = (src.HasValue && await GetSourceTableHasTenantIdAsync(table))
                                        ? " WHERE TenantId = @TenantId"
                                        : "";
                                    int? commandTimeoutOverride = GetExtendedTimeoutForTable(targetTable, veryHighTimeoutTables, veryHighTimeoutSeconds, highTimeoutTables, highTimeoutSeconds);

                                    if (commandTimeoutOverride.HasValue)
                                        Log.Information("   -> Using extended timeout: {TimeoutSeconds}s for this table.", commandTimeoutOverride.Value);

                                    await migrator.InsertTableWithIdMappingAsync(
                                        sourceConnection, targetConnection,
                                        table, targetTable, pkInfo,
                                        migrationBatch, tgt,
                                        whereClause, src, tgt,
                                        userMapping, mergeChunkSize, commandTimeoutOverride);

                                    Log.Information("   -> Done: {Table}", table);
                                }
                            }

                        totalTablesProcessed++;
                        // Mark checkpoint ONLY after successful completion for this tenant + table
                        if (src.HasValue && tgt.HasValue)
                        {
                            await DataSyncCheckpointHelper.MarkTableDoneAsync(targetConnection, src.Value, tgt.Value, targetTable);
                        }
                        }); // end retryPolicy.ExecuteAsync
                        }
                        catch (Exception ex)
                        {
                            Log.Error("[Error] Table {TargetTable} failed after retries: {ErrorMessage}", targetTable, ex.Message);
                            Log.Information("[DataSyncByTier] Skipping checkpoint for {TargetTable} — will retry on resume", targetTable);
                            // Continue to next table
                        }
                        }

                        // Sau khi chạy HẾT bảng trong Tier cho tenant này: Update FK từ IdMapping
                        await FkConstraintHelper.UpdateFkFromIdMappingAsync(targetConnection, migrationBatch, tgt, tgt);
                    }

                    ResourceMonitor.Stop();
                    ResourceMonitor.LogSummary();
                    ResourceMonitor.SaveReport(ReportWriter.ReportDirectory ?? "output");

                    overallSw.Stop();
                    var elapsedSeconds = overallSw.Elapsed.TotalSeconds;
                    Log.Information("[DataSyncByTier] Migration summary: {TablesProcessed} tables, {RowsMigrated} rows in {ElapsedMs}ms ({ElapsedSeconds:F1}s)", totalTablesProcessed, migrator.TotalRowsMigrated, overallSw.ElapsedMilliseconds, elapsedSeconds);

                    // Re-enable FK when all tiers with tables are covered, OR when all data-bearing tiers (those with actual source tables) are run
                    var tiersWithTables = tierMap.Where(kvp => kvp.Value.Count > 0).Select(kvp => kvp.Key).ToHashSet();
                    if (tiersWithTables.Count > 0 && new HashSet<int>(tiersToRun).IsSupersetOf(tiersWithTables))
                    {
                        await FkConstraintHelper.EnableAllFkAsync(targetConnection);
                        Log.Information("[DataSyncByTier] All tiers with data completed. Re-enabled all foreign keys.");
                    }
                    else
                    {
                        Log.Warning("[DataSyncByTier] Partial tiers run ({RunCount}/{TotalCount}). Foreign keys left disabled — run Option 7 to re-enable manually.", tiersToRun.Count, tiersWithTables.Count);
                    }
                }
                finally
                {
                    // no-op, FK đã xử lý ở trên
                    await targetConnection.ExecuteAsync("EXEC sp_releaseapplock @Resource = 'LogisticsDbMerger_DataSync', @LockOwner = 'Session'");
                }
            }
            else
            {
                // DryRun: in kế hoạch
                var dryRunTenants = allTenantPairs != null
                    ? allTenantPairs.Select(p => p.DisplayName).ToList()
                    : new List<string> { tenantName ?? "Single" };

                Log.Information("\n[DryRun] Would run data sync by tier for tenants:");
                foreach (var tn in dryRunTenants)
                    Log.Information(" - {TenantName}", tn);

                Log.Information("\n[DryRun] Tables in selected tiers:");
                foreach (var t in tierTables)
                    Log.Information(" - {Table}", t);
            }
        }

        static async Task SyncUsersAsync(string sourceConnStr, string targetConnStr, Dictionary<long, long> userMapping, int? sourceTenantId, int? targetTenantId, bool dryRun)
        {
            Log.Information("\n[Users] Starting Smart User Sync...");
            
            using var source = new SqlConnection(sourceConnStr);
            using var target = new SqlConnection(targetConnStr);
            
            // 1. Fetch Source Users (tenant-specific + host users with TenantId IS NULL)
            string sourceSql = "SELECT Id, UserName, EmailAddress, TenantId FROM Users";
            if (sourceTenantId.HasValue) sourceSql += " WHERE TenantId = @TenantId OR TenantId IS NULL";
            var sourceUsers = await source.QueryAsync<dynamic>(sourceSql, new { TenantId = sourceTenantId });

            // 2. Fetch Target Users (tenant-specific + host users with TenantId IS NULL)
            string targetSql = "SELECT Id, UserName, TenantId FROM Users";
            if (targetTenantId.HasValue) targetSql += " WHERE TenantId = @TenantId OR TenantId IS NULL";
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

            Log.Information("[Users] Found {SourceUserCount} Source Users, {TargetUserCount} Target Users.", sourceUsers.Count(), targetUsers.Count);

            // 2b. Disable self-referencing FKs on Users table to allow insert in any order
            var disabledSelfFks = new List<string>();
            if (!dryRun)
            {
                var selfFks = await target.QueryAsync<string>(@"
                    SELECT fk.name FROM sys.foreign_keys fk
                    WHERE fk.parent_object_id = OBJECT_ID('dbo.Users')
                      AND fk.referenced_object_id = OBJECT_ID('dbo.Users')");
                foreach (var fkName in selfFks)
                {
                    await target.ExecuteAsync($"ALTER TABLE [dbo].[Users] NOCHECK CONSTRAINT [{fkName.Replace("]", "]]")}]");
                    disabledSelfFks.Add(fkName);
                    Log.Information("[Users] Disabled self-ref FK: {FkName}", fkName);
                }
            }

            try
            {
            // 3. Process Each Source User
            foreach (var sUser in sourceUsers)
            {
                string userName = sUser.UserName;
                long sourceId = sUser.Id;
                long targetId = 0;

                // For host users (TenantId=NULL), preserve null key to match target dictionary.
                // For tenant users, use targetTenantId so source tenant maps to target tenant.
                int? sourceTid = (int?)sUser.TenantId;
                int? keyTenantId = sourceTid == null ? null : (targetTenantId ?? sourceTid);
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
                        // Preserve NULL for host users (TenantId IS NULL) — they must remain host-level
                        if (sourceTenantId != targetTenantId && props.ContainsKey("TenantId") && props["TenantId"] != null && props["TenantId"] != DBNull.Value)
                        {
                            props["TenantId"] = targetTenantId;
                        }
                        
                        var vals = cols.Select(k => "@" + k).ToList();
                        string insertSql = $"INSERT INTO Users ({string.Join(",", cols.Select(c => "[" + c.Replace("]", "]]") + "]"))}) VALUES ({string.Join(",", vals)}); SELECT CAST(SCOPE_IDENTITY() as bigint);";
                        
                        targetId = await target.ExecuteScalarAsync<long>(insertSql, (object)props);
                        Log.Information("   [Insert] Created User {UserName} (NewId: {TargetId})", userName, targetId);
                    }
                    else
                    {
                        Log.Information("   [DryRun] Would Insert User {UserName}", userName);
                        targetId = sourceId; // Mock
                    }
                }
                
                // Add to Map
                if (!userMapping.ContainsKey(sourceId))
                {
                    userMapping.Add(sourceId, targetId);
                }
            }
            }
            finally
            {
            // 4. Re-enable self-referencing FKs on Users table (always, even on error)
            if (!dryRun && disabledSelfFks.Count > 0)
            {
                foreach (var fkName in disabledSelfFks)
                {
                    try
                    {
                        await target.ExecuteAsync($"ALTER TABLE [dbo].[Users] WITH CHECK CHECK CONSTRAINT [{fkName.Replace("]", "]]")}]", commandTimeout: 300);
                        Log.Information("[Users] Re-enabled self-ref FK (trusted): {FkName}", fkName);
                    }
                    catch (Exception ex)
                    {
                        Log.Warning("[Users] Could not re-enable FK {FkName}: {ErrorMessage}", fkName, ex.Message);
                    }
                }
            }
            }

            Log.Information("[Users] User Mapping Built: {MappingCount} entries.", userMapping.Count);
        }

        static string GetBestFuzzyMatch(string sourceTable, HashSet<string> targetTables)
        {
            if (NoFuzzyMatchTables.Contains(sourceTable))
                return null;
            // Only match plural/singular by trailing 's' — require at least 4 chars to avoid false matches like "As" -> "A"
            if (sourceTable.Length >= 4 && sourceTable.EndsWith("s"))
            {
                var candidate = sourceTable.Substring(0, sourceTable.Length - 1);
                // Safety: don't fuzzy match if both forms exist in target (they're distinct tables)
                if (targetTables.Contains(candidate) && !targetTables.Contains(sourceTable))
                {
                    Log.Information("[DataSync] Fuzzy match: {SourceTable} -> {TargetTable} (singular)", sourceTable, candidate);
                    return candidate;
                }
            }
            if (sourceTable.Length >= 3 && !sourceTable.EndsWith("s"))
            {
                var candidate = sourceTable + "s";
                // Safety: don't fuzzy match if both forms exist in target
                if (targetTables.Contains(candidate) && !targetTables.Contains(sourceTable))
                {
                    Log.Information("[DataSync] Fuzzy match: {SourceTable} -> {TargetTable} (plural)", sourceTable, candidate);
                    return candidate;
                }
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
