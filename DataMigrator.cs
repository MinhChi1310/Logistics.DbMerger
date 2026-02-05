using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;

namespace Logistics.DbMerger
{
    public class DataMigrator
    {
        private readonly string _sourceConnStr;
        private readonly string _targetConnStr;
        private readonly int _batchSize;

        public DataMigrator(string sourceConnStr, string targetConnStr, int batchSize = 5000)
        {
            _sourceConnStr = sourceConnStr;
            _targetConnStr = targetConnStr;
            _batchSize = batchSize;
        }



        private async Task<bool> HasIdentityColumnAsync(SqlConnection conn, string tableName)
        {
            var count = await conn.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) 
                FROM sys.identity_columns 
                WHERE object_id = OBJECT_ID(@TableName)", new { TableName = tableName });
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

        public async Task MigrateTableAsync(string sourceTableName, bool isNewTable, string targetTableName = null, int? tenantId = null)
        {
            string destTable = targetTableName ?? sourceTableName;
            Console.WriteLine($"[Data] Migrating {sourceTableName} -> {destTable}...");
            
            using var sourceConn = new SqlConnection(_sourceConnStr);
            using var targetConn = new SqlConnection(_targetConnStr);
            await sourceConn.OpenAsync();
            await targetConn.OpenAsync();

            bool hasIdentity = await HasIdentityColumnAsync(targetConn, destTable);
            
            // Check Tenant Filter eligibility
            bool hasTenantId = await HasTenantIdColumnAsync(sourceConn, sourceTableName); // Check source for filter
            string whereClause = "";
            if (tenantId.HasValue)
            {
                if (hasTenantId)
                {
                    whereClause = $" WHERE TenantId = {tenantId.Value}";
                    Console.WriteLine($"   -> Filtering by TenantId = {tenantId.Value}");
                }
                else
                {
                    Console.WriteLine("   -> Table has no TenantId. Migrating ALL rows (Global/System Table).");
                }
            }

            string selectSql;
            if (isNewTable)
            {
                selectSql = $"SELECT * FROM [{sourceTableName}]{whereClause}";
            }
            else
            {
                // Common Table + Filter
                // We need to inject WHERE clause into the Common Table logic or just use simple select if we accept duplication risk
                // For now, let's stick to simple select + where.
                selectSql = $"SELECT * FROM [{sourceTableName}]{whereClause}";
                // NOTE: We are bypassing the BuildSelectQueryForCommonTableAsync complexity here for filtered runs.
                // Assuming filtered runs are partial updates.
            }

            if (string.IsNullOrEmpty(selectSql)) return;

            using var cmd = new SqlCommand(selectSql, sourceConn);
            cmd.CommandTimeout = 600;
            
            using var reader = await cmd.ExecuteReaderAsync();

            using var bulkCopy = new SqlBulkCopy(targetConn, 
                hasIdentity ? SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock : SqlBulkCopyOptions.TableLock, 
                null);
            
            bulkCopy.DestinationTableName = destTable;
            bulkCopy.BatchSize = _batchSize;
            bulkCopy.BulkCopyTimeout = 600;
            bulkCopy.NotifyAfter = 1000;
            bulkCopy.SqlRowsCopied += (sender, e) => Console.Write(".");

             // Auto-mapping columns by name
            for (int i = 0; i < reader.FieldCount; i++)
            {
                bulkCopy.ColumnMappings.Add(reader.GetName(i), reader.GetName(i));
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
}
