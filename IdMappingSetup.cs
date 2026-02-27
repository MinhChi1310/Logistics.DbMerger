using Microsoft.Data.SqlClient;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Creates and ensures the three IdMapping tables (Int, BigInt, Guid) on the target ADC database.
    /// </summary>
    public static class IdMappingSetup
    {
        public static async Task CreateIdMappingTablesIfNotExistsAsync(SqlConnection targetConn)
        {
            if (targetConn.State != System.Data.ConnectionState.Open)
                await targetConn.OpenAsync();

            var ddl = @"
-- IdMappingInt
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IdMappingInt' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[IdMappingInt] (
        [TableName]     NVARCHAR(128) NOT NULL,
        [ColumnName]    NVARCHAR(128) NOT NULL,
        [OldId]         INT NOT NULL,
        [NewId]         INT NOT NULL,
        [MigrationBatch] NVARCHAR(64) NOT NULL,
        [TenantId]      INT NULL,
        [CreatedDate]   DATETIME2(2) NOT NULL DEFAULT SYSDATETIME()
    );
    CREATE NONCLUSTERED INDEX [IX_IdMappingInt_TableName_OldId] ON [dbo].[IdMappingInt] ([TableName], [OldId]) INCLUDE ([NewId], [ColumnName]);
    CREATE NONCLUSTERED INDEX [IX_IdMappingInt_MigrationBatch_TableName] ON [dbo].[IdMappingInt] ([MigrationBatch], [TableName]);
    CREATE NONCLUSTERED INDEX [IX_IdMappingInt_TenantId_TableName] ON [dbo].[IdMappingInt] ([TenantId], [TableName]) WHERE [TenantId] IS NOT NULL;
    CREATE NONCLUSTERED INDEX [IX_IdMappingInt_TableName_ColumnName] ON [dbo].[IdMappingInt] ([TableName], [ColumnName]) INCLUDE ([NewId]);
END

-- IdMappingBigInt
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IdMappingBigInt' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[IdMappingBigInt] (
        [TableName]     NVARCHAR(128) NOT NULL,
        [ColumnName]    NVARCHAR(128) NOT NULL,
        [OldId]         BIGINT NOT NULL,
        [NewId]         BIGINT NOT NULL,
        [MigrationBatch] NVARCHAR(64) NOT NULL,
        [TenantId]      INT NULL,
        [CreatedDate]   DATETIME2(2) NOT NULL DEFAULT SYSDATETIME()
    );
    CREATE NONCLUSTERED INDEX [IX_IdMappingBigInt_TableName_OldId] ON [dbo].[IdMappingBigInt] ([TableName], [OldId]) INCLUDE ([NewId], [ColumnName]);
    CREATE NONCLUSTERED INDEX [IX_IdMappingBigInt_MigrationBatch_TableName] ON [dbo].[IdMappingBigInt] ([MigrationBatch], [TableName]);
    CREATE NONCLUSTERED INDEX [IX_IdMappingBigInt_TenantId_TableName] ON [dbo].[IdMappingBigInt] ([TenantId], [TableName]) WHERE [TenantId] IS NOT NULL;
    CREATE NONCLUSTERED INDEX [IX_IdMappingBigInt_TableName_ColumnName] ON [dbo].[IdMappingBigInt] ([TableName], [ColumnName]) INCLUDE ([NewId]);
END

-- IdMappingGuid
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IdMappingGuid' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[IdMappingGuid] (
        [TableName]     NVARCHAR(128) NOT NULL,
        [ColumnName]    NVARCHAR(128) NOT NULL,
        [OldId]         UNIQUEIDENTIFIER NOT NULL,
        [NewId]         UNIQUEIDENTIFIER NOT NULL,
        [MigrationBatch] NVARCHAR(64) NOT NULL,
        [TenantId]      INT NULL,
        [CreatedDate]   DATETIME2(2) NOT NULL DEFAULT SYSDATETIME()
    );
    CREATE NONCLUSTERED INDEX [IX_IdMappingGuid_TableName_OldId] ON [dbo].[IdMappingGuid] ([TableName], [OldId]) INCLUDE ([NewId], [ColumnName]);
    CREATE NONCLUSTERED INDEX [IX_IdMappingGuid_MigrationBatch_TableName] ON [dbo].[IdMappingGuid] ([MigrationBatch], [TableName]);
    CREATE NONCLUSTERED INDEX [IX_IdMappingGuid_TenantId_TableName] ON [dbo].[IdMappingGuid] ([TenantId], [TableName]) WHERE [TenantId] IS NOT NULL;
    CREATE NONCLUSTERED INDEX [IX_IdMappingGuid_TableName_ColumnName] ON [dbo].[IdMappingGuid] ([TableName], [ColumnName]) INCLUDE ([NewId]);
END
";
            using var cmd = new SqlCommand(ddl, targetConn);
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();

            var ensureIndexDdl = @"
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IdMappingInt' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IdMappingInt_TableName_ColumnName' AND object_id = OBJECT_ID('dbo.IdMappingInt'))
   CREATE NONCLUSTERED INDEX [IX_IdMappingInt_TableName_ColumnName] ON [dbo].[IdMappingInt] ([TableName], [ColumnName]) INCLUDE ([NewId]);
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IdMappingBigInt' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IdMappingBigInt_TableName_ColumnName' AND object_id = OBJECT_ID('dbo.IdMappingBigInt'))
   CREATE NONCLUSTERED INDEX [IX_IdMappingBigInt_TableName_ColumnName] ON [dbo].[IdMappingBigInt] ([TableName], [ColumnName]) INCLUDE ([NewId]);
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IdMappingGuid' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IdMappingGuid_TableName_ColumnName' AND object_id = OBJECT_ID('dbo.IdMappingGuid'))
   CREATE NONCLUSTERED INDEX [IX_IdMappingGuid_TableName_ColumnName] ON [dbo].[IdMappingGuid] ([TableName], [ColumnName]) INCLUDE ([NewId]);
";
            using var cmd2 = new SqlCommand(ensureIndexDdl, targetConn);
            cmd2.CommandTimeout = 60;
            await cmd2.ExecuteNonQueryAsync();

            Console.WriteLine("[IdMapping] IdMappingInt, IdMappingBigInt, IdMappingGuid tables ensured.");
        }
    }
}
