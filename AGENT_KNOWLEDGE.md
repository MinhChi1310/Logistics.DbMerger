# Agent Knowledge: Logistics.DbMerger

## Overview
`Logistics.DbMerger` is a .NET 8 console application designed to merge the **MdcProd** (Source) database into **AdcProd** (Target) for the GOSEI Logistics platform. The databases share similar schemas but have diverged with different table names, extra columns, and mismatched ID spaces for tenants and users.

## Project Structure & Architecture

- **Program.cs**: Entry point. Drives execution via an interactive console menu or command-line arguments. Runs the migration steps sequentially: `Schema Sync` -> `Object Sync` -> `Data Sync` -> `Validate` -> `Rollback`.
- **MigrationConfig.cs**: Defines `TableOrder`, a crucial list of 185 tables ordered into 8 dependency tiers (Reference → Core → Operational → Transactional → High Volume → Support → Rules/Audit → Notifications / Settings). Data migration must strictly follow this order to prevent Foreign Key constraints from failing during bulk inserts.
- **SchemaSync.cs**: The workhorse for discovering schema definitions in MDC (tables, columns, indexes, constraints) and generating high-fidelity dynamic `CREATE TABLE` and `ALTER TABLE` SQL scripts to execute against ADC.
    - Default policy for newly added columns is `NULL` to avoid breaking existing row constraints.
- **DataMigrator.cs**: Handles the actual copy of rows. Uses `SqlBulkCopy` for high performance.
    - **Identity Column Handling**: It uses `KeepIdentity` so the target keeps source primary keys.
    - **Data Transformation**: When syncing multiple tenants or transforming users, it loads `SqlDataReader` data into an in-memory `DataTable` to rewrite `TenantId` values, and map Audit User IDs (like `CreatorUserId` or `LastModifierUserId`) via an ID dictionary before committing the bulk copy.
    - **Custom Column Injections**: Contains explicit logic for tables that have diverged significantly. For example, injecting static defaults like `OldSAPID = NULL` or `PartTimeFlex = 0` when migrating the `Contact` table. 
- **Validator.cs**: Executes post-migration SQL queries to verify row counts match expected values, detect FK integrity violations, and run business logic sanity checks.
- **RollbackLogger.cs**: Accumulates safe undo statements (`DROP TABLE`, `ALTER TABLE DROP COLUMN`) inside an output file timestamped per run to safely rollback any errors.

## Execution Requirements
- The tool uses `Microsoft.Data.SqlClient` and `Dapper`. 
- Settings like `ConnectionStrings` are configured via `appsettings.json`.
- A dry-run mode (`DryRun: true`) exists to preview changes without modifying databases.

## Key Considerations for AI Agents modifying this code:
1. **Adding Tables**: Any new table mapped from MDC -> ADC must be added to the tiers in `MigrationConfig.cs` in the correct dependency order.
2. **Column Divergence**: If ADC has new required columns that MDC lacks, explicit mapping and default values must be provided in `DataMigrator.cs` during the `SELECT` query generation. 
3. **Data Transformations**: The `dt.Load(reader)` memory-buffer approach in `DataMigrator.cs` is functional but could cause memory spikes on very large tables (like Tier 5 timebands/clock events) if `_batchSize` is set too high. Modifications to performance should focus on this pipeline. 
4. **Fuzzy Naming**: The schema sync logic and data migration logic handles singular/plural table names (e.g. `IndirectClockEvents` in source -> `IndirectClockEvent` in target).

