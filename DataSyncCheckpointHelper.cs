using Microsoft.Data.SqlClient;
using System;
using System.Threading.Tasks;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Helper for per-tenant, per-table DataSync checkpoints to support resume in Option 3.
    /// </summary>
    public static class DataSyncCheckpointHelper
    {
        public static async Task EnsureTableAsync(SqlConnection targetConn)
        {
            if (targetConn.State != System.Data.ConnectionState.Open)
                await targetConn.OpenAsync();

            var ddl = @"
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DataSyncCheckpoint' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[DataSyncCheckpoint] (
        [Id]             INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [SourceTenantId] INT             NOT NULL,
        [TargetTenantId] INT             NOT NULL,
        [TableName]      NVARCHAR(256)   NOT NULL,
        [LastUpdated]    DATETIME2(2)    NOT NULL DEFAULT SYSDATETIME()
    );

    CREATE UNIQUE INDEX [IX_DataSyncCheckpoint_Tenant_Table]
        ON [dbo].[DataSyncCheckpoint]([SourceTenantId], [TargetTenantId], [TableName]);
END
";

            using var cmd = new SqlCommand(ddl, targetConn);
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task<bool> IsTableDoneAsync(
            SqlConnection targetConn,
            int sourceTenantId,
            int targetTenantId,
            string tableName)
        {
            const string sql = @"
SELECT COUNT(1)
FROM [dbo].[DataSyncCheckpoint]
WHERE [SourceTenantId] = @SourceTenantId
  AND [TargetTenantId] = @TargetTenantId
  AND [TableName]      = @TableName;";

            using var cmd = new SqlCommand(sql, targetConn);
            cmd.Parameters.AddWithValue("@SourceTenantId", sourceTenantId);
            cmd.Parameters.AddWithValue("@TargetTenantId", targetTenantId);
            cmd.Parameters.AddWithValue("@TableName", tableName);

            var result = (int)await cmd.ExecuteScalarAsync();
            return result > 0;
        }

        public static async Task MarkTableDoneAsync(
            SqlConnection targetConn,
            int sourceTenantId,
            int targetTenantId,
            string tableName)
        {
            const string sql = @"
MERGE [dbo].[DataSyncCheckpoint] AS target
USING (VALUES (@SourceTenantId, @TargetTenantId, @TableName)) AS src(SourceTenantId, TargetTenantId, TableName)
   ON target.SourceTenantId = src.SourceTenantId
  AND target.TargetTenantId = src.TargetTenantId
  AND target.TableName      = src.TableName
WHEN MATCHED THEN
    UPDATE SET LastUpdated = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (SourceTenantId, TargetTenantId, TableName, LastUpdated)
    VALUES (src.SourceTenantId, src.TargetTenantId, src.TableName, SYSDATETIME());";

            using var cmd = new SqlCommand(sql, targetConn);
            cmd.Parameters.AddWithValue("@SourceTenantId", sourceTenantId);
            cmd.Parameters.AddWithValue("@TargetTenantId", targetTenantId);
            cmd.Parameters.AddWithValue("@TableName", tableName);
            await cmd.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Clears all checkpoint rows. Used by Option 8 when clearing migration data to keep data and checkpoints consistent.
        /// </summary>
        public static async Task ClearAllAsync(SqlConnection targetConn)
        {
            if (targetConn.State != System.Data.ConnectionState.Open)
                await targetConn.OpenAsync();

            var sql = @"
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'DataSyncCheckpoint' AND schema_id = SCHEMA_ID('dbo'))
    TRUNCATE TABLE [dbo].[DataSyncCheckpoint];";

            using var cmd = new SqlCommand(sql, targetConn);
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}

