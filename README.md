# Logistics.DbMerger

A .NET 9.0 console application for merging the **MdcProd** (Source) SQL Server database into **AdcProd** (Target). Built for the GOSEI Logistics platform to consolidate two separate operational databases into one.

## Purpose

MDC and ADC are two separate Logistics platform instances that share the same schema structure but have diverged over time (different table names, extra columns, different tenant IDs, different user IDs). This tool automates:

1. **Pre-Flight Checks** — Detects identity range overlaps and missing partition filegroups before any changes are made.
2. **Schema Sync** — Creates missing tables in ADC and adds missing columns. All DDL uses IF NOT EXISTS guards for idempotent re-runs. Partition schemes remapped to PRIMARY when target filegroups don't exist.
3. **Object Sync** — Migrates missing Stored Procedures, Views, and Functions from MDC to ADC. Skips backup/test objects (`_bak`, `_test`, `_OLD`). Uses SHA2_256 hash comparison to detect identical vs diverged definitions. Never overwrites existing ADC objects.
4. **Data Sync** — Copies row data with smart transformations:
   - **Per-table transactions** — Each table's bulk insert is wrapped in an external SqlTransaction. Failure rolls back only that table.
   - **Tenant ID remapping** — Resolves or creates the tenant in ADC and rewrites `TenantId` columns across all tables.
   - **User ID remapping** — Matches users by `UserName` and rewrites audit fields (`CreatorUserId`, `LastModifierUserId`, `CreatedBy`, `ModifiedBy`, etc.).
   - **ABP system table handling** — Per-table merge strategy for 9 ABP framework tables. Host-level records (TenantId = NULL) are skipped.
   - **Tiered migration order** — Processes tables in 8 dependency tiers (reference → core → transactional → audit) to preserve FK integrity.
   - **Truncation logging** — All string truncation events logged with `[Truncation]` tag showing original and target lengths.
   - **Checkpoint/Resume** — Per-table checkpoints allow resuming from failures without re-migrating completed tables.
   - **Table filtering** — Backup tables (`_bak`, `_test`, `_OLD`, date-stamped) automatically filtered via regex patterns.
5. **Validation** — Post-migration row count verification, FK integrity checks (DBCC CHECKCONSTRAINTS), and business logic queries.
6. **Rollback** — Generates per-step SQL rollback scripts (`DROP TABLE`, `DROP COLUMN`) for safe undo.

## Architecture

| File | Responsibility |
|---|---|
| `Program.cs` | Entry point, interactive menu, tenant/user resolution, migration orchestration, ABP merge strategy |
| `SchemaSync.cs` | Table creation, column evolution, constraint/index sync with IF NOT EXISTS guards, partition remap |
| `DataMigrator.cs` | `SqlBulkCopy`-based data transfer with per-table transactions, truncation logging, ID remapping |
| `ObjectSync.cs` | SP/View/Function sync with hash comparison, backup filtering, skip-existing logic |
| `PreFlightValidator.cs` | Identity range overlap detection, partition filegroup check (read-only) |
| `MigrationConfig.cs` | Defines the 8-tier table processing order (185 tables) |
| `TableSkipRules.cs` | Regex-based filtering of backup/temp/system tables and objects |
| `Validator.cs` | Post-migration integrity checks (row counts, FK violations, business rules) |
| `DataSyncCheckpointHelper.cs` | Per-table checkpoint tracking for resume-from-failure |
| `IdMappingSetup.cs` | Creates IdMapping tables (Int, BigInt, Guid) in target |
| `FkConstraintHelper.cs` | FK disable/enable, FK column updates from IdMapping |
| `RollbackLogger.cs` | Generates timestamped SQL rollback scripts |
| `appsettings.json` | Connection strings and runtime settings |

## Prerequisites

- .NET 9.0 SDK
- Network access to both MDC and ADC SQL Server instances
- SQL login with `CREATE TABLE`, `INSERT`, `ALTER`, and `SELECT` permissions on both databases

## Configuration

Edit `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "SourceMdc": "Server=MDC_SERVER;Database=MdcProd;User Id=...;Password=...;TrustServerCertificate=True;",
    "TargetAdc": "Server=ADC_SERVER;Database=AdcProd;User Id=...;Password=...;TrustServerCertificate=True;"
  },
  "Settings": {
    "BatchSize": 5000,
    "DryRun": true
  }
}
```

> **DryRun = true** prints what the tool *would* do without modifying either database. Always start with a dry run.

## Usage

### Interactive Mode (Recommended)

```powershell
cd Logistics.DbMerger
dotnet run
```

The menu will display:

```
[Main Menu] — Migration Workflow Order
───────────────────────────────────────
  [Pre-Migration]
  1. Pre-Flight Checks (identity overlaps, partition filegroups)
  2. Generate Reports (MDC-only tables, comparison)
  [Schema & Objects]
  3. Sync Schema (Tables & Columns)
  4. Sync Objects (Procedures, Views, Functions)
  [Data Migration]
  5. Sync Data (Smart Merge & Tenant Filter)
  6. Sync Data by Tier (Tier -> Tenant)
  7. Enable FK (re-enable all foreign keys on target)
  [Validation]
  8. Validate / Verify (row counts, FK integrity, business logic)
  [Utilities]
  9. Rollback Last Action
  10. Clear Migration Data (delete rows based on IdMapping)
  0. Exit
───────────────────────────────────────
```

### Step-by-Step Migration Guide

#### 1. Configure Connection Strings
Edit `appsettings.json` with your source (MDC) and target (ADC) connection strings. Set `DryRun: true` for your first run.

#### 2. Pre-Flight Checks (Menu Option 1)
Run BEFORE any migration. Reports:
- **Identity range overlaps** — tables where source MAX(Id) >= target MAX(Id)
- **Missing partition filegroups** — schemes that will be remapped to PRIMARY

Review findings. No data is modified.

#### 3. Generate Reports (Menu Option 2)
Lists MDC-only tables and optionally creates their structure in ADC. Review which tables will be synced.

#### 4. Sync Schema (Menu Option 3)
Creates missing tables, adds missing columns, syncs constraints, indexes, and partition schemes. All DDL is idempotent (safe to re-run). Generates rollback scripts.

#### 5. Sync Objects (Menu Option 4)
Syncs stored procedures, views, and functions from MDC to ADC:
- **Backup/test objects filtered** (`_bak`, `_test`, `_OLD`, date-stamped)
- **Existing ADC objects never overwritten** — diverged definitions logged as warnings
- **Identical objects skipped** — hash comparison detects exact matches

#### 6. Sync Data (Menu Option 5 or 6)
Prompts for tenant selection. For each table:
- Checks checkpoint — skips already-completed tables
- Wraps SqlBulkCopy in per-table transaction — failure rolls back only that table
- Remaps TenantId and UserId columns
- ABP system tables handled with per-table merge strategy (host records skipped)
- String truncation events logged with `[Truncation]` tag

**Option 5**: Smart merge with auto-detection of migration path per table.
**Option 6**: Tier-by-tier migration (useful for debugging specific table tiers).

#### 7. Enable FK (Menu Option 7)
Re-enables all foreign keys on the target database after data migration.

#### 8. Validate (Menu Option 8)
Post-migration verification:
- Row count comparison per table per tenant
- `DBCC CHECKCONSTRAINTS` for FK integrity
- Business logic queries (active contacts, timeband ranges, leave requests)

#### 9. Repeat
Run steps 2-8 for each tenant. The PRD recommends **3+ staging rehearsals** before production cutover.

### Automated Mode

```powershell
dotnet run -- --tenant="Kewdale"
```

Runs the full migration pipeline for the specified tenant without the interactive menu.

### DryRun Mode

Set `"DryRun": true` in appsettings.json. The tool logs what it *would* do without modifying either database. Always start with a dry run.

## Key Design Decisions

- **Per-table transactions**: Each table's bulk insert is wrapped in an external `SqlTransaction`. Failure rolls back only that table; previously completed tables remain safe via checkpoint.
- **Identity preservation**: `SqlBulkCopy` uses `KeepIdentity | CheckConstraints` so source PKs are preserved and constraints validated.
- **Buffered transform**: When `TenantId` or `UserId` transformation is needed, data is loaded into a `DataTable` for in-memory mutation before bulk insert.
- **Streaming mode**: When no transformation is required, data streams directly from `SqlDataReader` to `SqlBulkCopy` for optimal performance.
- **Idempotent DDL**: All schema sync uses IF NOT EXISTS guards — safe to re-run after crashes without manual cleanup.
- **Safe-by-default**: New columns added via `ALTER TABLE` are always `NULL` to avoid breaking existing data.
- **Never overwrite ADC objects**: ObjectSync detects 148 diverged stored procedures and logs warnings without overwriting.

## Dependencies

| Package | Purpose |
|---|---|
| `Microsoft.Data.SqlClient` | SQL Server connectivity |
| `Dapper` | Lightweight ORM for metadata queries |
| `Microsoft.Extensions.Configuration` | `appsettings.json` loading |

## Extending the Tool

- **Add new table mappings**: Edit `ExplicitTableMappings` dictionary in `Program.cs`.
- **Change migration order**: Edit the tier lists in `MigrationConfig.cs`.
- **Add column injection rules**: See `KnownAdcOnlyDefaults` dictionary in `DataMigrator.cs`.
- **Add validation queries**: Extend `Validator.cs` with new business logic checks.
- **Add pre-flight checks**: Add new static methods to `PreFlightValidator.cs` and call from `RunPreFlight` in `Program.cs`.
- **Add ABP table strategies**: Edit `abpMergeStrategy` dictionary in `Program.cs`.
- **Add table skip patterns**: Edit regex patterns in `TableSkipRules.cs`.
