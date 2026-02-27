using Microsoft.Data.SqlClient;
using Dapper;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Helper methods for table lists and report output.
    /// </summary>
    public static class Helper
    {
        /// <summary>
        /// Gets tables that exist only in MDC (source) and not in ADC (target).
        /// Applies TableSkipRules: skips backup/archive (date suffixes), temporary tables (TEMP*, tmp_*, etc.).
        /// Returns ordered list and writes a numbered list to file.
        /// </summary>
        /// <param name="sourceConnStr">MDC connection string.</param>
        /// <param name="targetConnStr">ADC connection string.</param>
        /// <param name="outputFilePath">Optional. Default: "mdc_only_tables.txt".</param>
        /// <param name="writeToFile">If true, writes the list to file. If false, only returns the list (no file).</param>
        /// <returns>List of table names (only in MDC, after skip rules).</returns>
        public static async Task<List<string>> GetTablesOnlyInMdcAsync(
            string sourceConnStr,
            string targetConnStr,
            string? outputFilePath = null,
            bool writeToFile = true)
        {
            var mdcTables = await GetTableListFilteredAsync(sourceConnStr);
            var adcTables = await GetTableListFilteredAsync(targetConnStr);

            var onlyInMdc = mdcTables
                .Except(adcTables, StringComparer.OrdinalIgnoreCase)
                .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (writeToFile)
            {
                var path = outputFilePath ?? "mdc_only_tables.txt";
                await WriteNumberedTableListToFileAsync(onlyInMdc, path, "Tables only in MDC (not in ADC)");
            }

            return onlyInMdc;
        }

        /// <summary>
        /// Gets tables that exist in both MDC and ADC (common tables).
        /// Applies the same TableSkipRules as Method 1 (skips backup/archive, temporary tables, etc.).
        /// Returns ordered list and writes a numbered list to file.
        /// </summary>
        /// <param name="sourceConnStr">MDC connection string.</param>
        /// <param name="targetConnStr">ADC connection string.</param>
        /// <param name="outputFilePath">Optional. Default: "mdc_adc_common_tables.txt".</param>
        /// <returns>List of table names (in both MDC and ADC, after skip rules).</returns>
        public static async Task<List<string>> GetTablesInBothMdcAndAdcAsync(
            string sourceConnStr,
            string targetConnStr,
            string? outputFilePath = null)
        {
            var mdcTables = await GetTableListFilteredAsync(sourceConnStr);
            var adcTables = await GetTableListFilteredAsync(targetConnStr);

            var inBoth = mdcTables
                .Intersect(adcTables, StringComparer.OrdinalIgnoreCase)
                .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var path = outputFilePath ?? "mdc_adc_common_tables.txt";
            await WriteNumberedTableListToFileAsync(inBoth, path, "Tables in both MDC and ADC");

            return inBoth;
        }

        /// <summary>
        /// Gets tables (in both MDC and ADC) that have at least one column existing only in MDC.
        /// Returns Dictionary[tableName, List of column names that exist only in MDC].
        /// Applies same TableSkipRules for table list. Outputs report to file.
        /// </summary>
        /// <param name="sourceConnStr">MDC connection string.</param>
        /// <param name="targetConnStr">ADC connection string.</param>
        /// <param name="outputFilePath">Optional. Default: "mdc_only_columns_report.txt".</param>
        /// <returns>Dictionary: table name -> list of column names (only in MDC), only tables with at least one such column.</returns>
        public static async Task<Dictionary<string, List<string>>> GetTablesWithColumnsOnlyInMdcAsync(
            string sourceConnStr,
            string targetConnStr,
            string? outputFilePath = null)
        {
            var mdcTables = await GetTableListFilteredAsync(sourceConnStr);
            var adcTables = await GetTableListFilteredAsync(targetConnStr);
            var commonTables = mdcTables
                .Intersect(adcTables, StringComparer.OrdinalIgnoreCase)
                .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (commonTables.Count == 0)
            {
                await WriteMdcOnlyColumnsReportToFileAsync(new Dictionary<string, List<string>>(), outputFilePath ?? "mdc_only_columns_report.txt");
                return new Dictionary<string, List<string>>();
            }

            var mdcCols = await GetColumnsByTableAsync(sourceConnStr, commonTables);
            var adcCols = await GetColumnsByTableAsync(targetConnStr, commonTables);

            var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in commonTables)
            {
                var mdcSet = mdcCols.TryGetValue(table, out var m) ? m : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var adcSet = adcCols.TryGetValue(table, out var a) ? a : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var onlyInMdc = mdcSet.Except(adcSet, StringComparer.OrdinalIgnoreCase).OrderBy(c => c, StringComparer.OrdinalIgnoreCase).ToList();
                if (onlyInMdc.Count > 0)
                    result[table] = onlyInMdc;
            }

            var path = outputFilePath ?? "mdc_only_columns_report.txt";
            await WriteMdcOnlyColumnsReportToFileAsync(result, path);
            return result;
        }

        /// <summary>
        /// Gets tables (in both MDC and ADC) that have at least one column existing only in ADC.
        /// Returns Dictionary[tableName, List of column names that exist only in ADC].
        /// Applies same TableSkipRules for table list. Outputs report to file with numbered table list.
        /// </summary>
        /// <param name="sourceConnStr">MDC connection string.</param>
        /// <param name="targetConnStr">ADC connection string.</param>
        /// <param name="outputFilePath">Optional. Default: "adc_only_columns_report.txt".</param>
        /// <returns>Dictionary: table name -> list of column names (only in ADC), only tables with at least one such column.</returns>
        public static async Task<Dictionary<string, List<string>>> GetTablesWithColumnsOnlyInAdcAsync(
            string sourceConnStr,
            string targetConnStr,
            string? outputFilePath = null)
        {
            var mdcTables = await GetTableListFilteredAsync(sourceConnStr);
            var adcTables = await GetTableListFilteredAsync(targetConnStr);
            var commonTables = mdcTables
                .Intersect(adcTables, StringComparer.OrdinalIgnoreCase)
                .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (commonTables.Count == 0)
            {
                await WriteAdcOnlyColumnsReportToFileAsync(new Dictionary<string, List<string>>(), outputFilePath ?? "adc_only_columns_report.txt");
                return new Dictionary<string, List<string>>();
            }

            var mdcCols = await GetColumnsByTableAsync(sourceConnStr, commonTables);
            var adcCols = await GetColumnsByTableAsync(targetConnStr, commonTables);

            var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in commonTables)
            {
                var mdcSet = mdcCols.TryGetValue(table, out var m) ? m : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var adcSet = adcCols.TryGetValue(table, out var a) ? a : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var onlyInAdc = adcSet.Except(mdcSet, StringComparer.OrdinalIgnoreCase).OrderBy(c => c, StringComparer.OrdinalIgnoreCase).ToList();
                if (onlyInAdc.Count > 0)
                    result[table] = onlyInAdc;
            }

            var path = outputFilePath ?? "adc_only_columns_report.txt";
            await WriteAdcOnlyColumnsReportToFileAsync(result, path);
            return result;
        }

        /// <summary>
        /// Writes the Dictionary[table, List&lt;column&gt;] report to file (tables with columns only in ADC).
        /// Includes a numbered list of tables and per-table column detail.
        /// </summary>
        public static async Task WriteAdcOnlyColumnsReportToFileAsync(
            Dictionary<string, List<string>> tableToAdcOnlyColumns,
            string filePath)
        {
            var ordered = tableToAdcOnlyColumns.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase).ToList();
            var lines = new List<string>
            {
                "=== Tables with columns only in ADC (scope: tables in both MDC and ADC) ===",
                $"Total tables with at least one ADC-only column: {tableToAdcOnlyColumns.Count}",
                $"Total ADC-only columns: {tableToAdcOnlyColumns.Sum(x => x.Value.Count)}",
                "",
                "--- Numbered list of tables ---"
            };
            for (var i = 0; i < ordered.Count; i++)
                lines.Add($"{i + 1}. {ordered[i].Key}");
            lines.Add("");
            lines.Add("--- Detail (Table | Column names) ---");
            foreach (var kv in ordered)
            {
                lines.Add("");
                lines.Add($"Table: {kv.Key}  ({kv.Value.Count} column(s) only in ADC)");
                for (var j = 0; j < kv.Value.Count; j++)
                    lines.Add($"  {j + 1}. {kv.Value[j]}");
            }
            await File.WriteAllLinesAsync(filePath, lines);
        }

        /// <summary>
        /// Writes the Dictionary[table, List&lt;column&gt;] report to file (tables with columns only in MDC).
        /// </summary>
        public static async Task WriteMdcOnlyColumnsReportToFileAsync(
            Dictionary<string, List<string>> tableToMdcOnlyColumns,
            string filePath)
        {
            var lines = new List<string>
            {
                "=== Tables with columns only in MDC (scope: tables in both MDC and ADC) ===",
                $"Total tables with at least one MDC-only column: {tableToMdcOnlyColumns.Count}",
                $"Total MDC-only columns: {tableToMdcOnlyColumns.Sum(x => x.Value.Count)}",
                "",
                "--- Detail (Table | Column names) ---"
            };
            foreach (var kv in tableToMdcOnlyColumns.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
            {
                lines.Add("");
                lines.Add($"Table: {kv.Key}  ({kv.Value.Count} column(s) only in MDC)");
                for (var i = 0; i < kv.Value.Count; i++)
                    lines.Add($"  {i + 1}. {kv.Value[i]}");
            }
            await File.WriteAllLinesAsync(filePath, lines);
        }

        /// <summary>
        /// Gets tables (in both MDC and ADC) that have at least one column with the same name but different type.
        /// Returns Dictionary[tableName, List of ColumnTypeMismatchRow (Mdc + Adc schema)].
        /// Applies same TableSkipRules. Outputs report to file with numbered table list.
        /// </summary>
        /// <param name="sourceConnStr">MDC connection string.</param>
        /// <param name="targetConnStr">ADC connection string.</param>
        /// <param name="outputFilePath">Optional. Default: "column_type_mismatch_report.txt".</param>
        /// <returns>Dictionary: table name -> list of columns (same name, different type) with MDC and ADC schema.</returns>
        public static async Task<Dictionary<string, List<ColumnTypeMismatchRow>>> GetTablesWithColumnTypeMismatchAsync(
            string sourceConnStr,
            string targetConnStr,
            string? outputFilePath = null)
        {
            var mdcTables = await GetTableListFilteredAsync(sourceConnStr);
            var adcTables = await GetTableListFilteredAsync(targetConnStr);
            var commonTables = mdcTables
                .Intersect(adcTables, StringComparer.OrdinalIgnoreCase)
                .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (commonTables.Count == 0)
            {
                await WriteColumnTypeMismatchReportToFileAsync(new Dictionary<string, List<ColumnTypeMismatchRow>>(), outputFilePath ?? "column_type_mismatch_report.txt");
                return new Dictionary<string, List<ColumnTypeMismatchRow>>();
            }

            var mdcSchemas = await GetColumnSchemasByTableAsync(sourceConnStr, commonTables);
            var adcSchemas = await GetColumnSchemasByTableAsync(targetConnStr, commonTables);

            var result = new Dictionary<string, List<ColumnTypeMismatchRow>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in commonTables)
            {
                var mdcList = mdcSchemas.TryGetValue(table, out var ml) ? ml : new List<ColumnSchema>();
                var adcByCol = (adcSchemas.TryGetValue(table, out var al) ? al : new List<ColumnSchema>())
                    .GroupBy(c => c.ColumnName, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
                var rows = new List<ColumnTypeMismatchRow>();
                foreach (var mdcCol in mdcList)
                {
                    if (!adcByCol.TryGetValue(mdcCol.ColumnName, out var adcCol))
                        continue;
                    if (TypesEqual(mdcCol, adcCol))
                        continue;
                    rows.Add(new ColumnTypeMismatchRow { Mdc = mdcCol, Adc = adcCol });
                }
                if (rows.Count > 0)
                    result[table] = rows.OrderBy(r => r.Mdc.ColumnName, StringComparer.OrdinalIgnoreCase).ToList();
            }

            var path = outputFilePath ?? "column_type_mismatch_report.txt";
            await WriteColumnTypeMismatchReportToFileAsync(result, path);
            return result;
        }

        /// <summary>
        /// Writes the column type mismatch report to file (numbered table list + detail per table).
        /// </summary>
        public static async Task WriteColumnTypeMismatchReportToFileAsync(
            Dictionary<string, List<ColumnTypeMismatchRow>> tableToMismatches,
            string filePath)
        {
            var ordered = tableToMismatches.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase).ToList();
            var lines = new List<string>
            {
                "=== Tables with columns (same name, different type) - scope: tables in both MDC and ADC ===",
                $"Total tables with at least one type mismatch: {tableToMismatches.Count}",
                $"Total columns with type mismatch: {tableToMismatches.Sum(x => x.Value.Count)}",
                "",
                "--- Numbered list of tables ---"
            };
            for (var i = 0; i < ordered.Count; i++)
                lines.Add($"{i + 1}. {ordered[i].Key}");
            lines.Add("");
            lines.Add("--- Detail (Table | Column | MDC type | ADC type) ---");
            foreach (var kv in ordered)
            {
                lines.Add("");
                lines.Add($"Table: {kv.Key}  ({kv.Value.Count} column(s) with type mismatch)");
                for (var j = 0; j < kv.Value.Count; j++)
                {
                    var r = kv.Value[j];
                    var mdcType = FormatType(r.Mdc);
                    var adcType = FormatType(r.Adc);
                    lines.Add($"  {j + 1}. {r.Mdc.ColumnName}  |  MDC: {mdcType}  |  ADC: {adcType}");
                }
            }
            await File.WriteAllLinesAsync(filePath, lines);
        }

        private static string FormatType(ColumnSchema c)
        {
            var t = c.DataType ?? "";
            if (t.Equals("nvarchar", StringComparison.OrdinalIgnoreCase) || t.Equals("varchar", StringComparison.OrdinalIgnoreCase) ||
                t.Equals("nchar", StringComparison.OrdinalIgnoreCase) || t.Equals("char", StringComparison.OrdinalIgnoreCase))
            {
                var len = c.CharacterMaximumLength == -1 ? "max" : (c.CharacterMaximumLength?.ToString() ?? "");
                return $"{t}({len})";
            }
            if (t.Equals("decimal", StringComparison.OrdinalIgnoreCase) || t.Equals("numeric", StringComparison.OrdinalIgnoreCase))
                return $"{t}({c.NumericPrecision ?? 0},{c.NumericScale ?? 0})";
            return t;
        }

        private static bool TypesEqual(ColumnSchema mdc, ColumnSchema adc)
        {
            if (!string.Equals(mdc.DataType, adc.DataType, StringComparison.OrdinalIgnoreCase))
                return false;
            if ((mdc.CharacterMaximumLength ?? -1) != (adc.CharacterMaximumLength ?? -1))
                return false;
            if ((mdc.NumericPrecision ?? 0) != (adc.NumericPrecision ?? 0))
                return false;
            if ((mdc.NumericScale ?? 0) != (adc.NumericScale ?? 0))
                return false;
            return true;
        }

        /// <summary>
        /// Gets full column schema per table (dbo) for the given table names.
        /// </summary>
        private static async Task<Dictionary<string, List<ColumnSchema>>> GetColumnSchemasByTableAsync(string connStr, List<string> tableNames)
        {
            if (tableNames.Count == 0)
                return new Dictionary<string, List<ColumnSchema>>(StringComparer.OrdinalIgnoreCase);
            using var conn = new SqlConnection(connStr);
            var rows = (await conn.QueryAsync<ColumnSchema>(
                @"SELECT TABLE_NAME AS TableName, COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType,
                    CHARACTER_MAXIMUM_LENGTH AS CharacterMaximumLength,
                    NUMERIC_PRECISION AS NumericPrecision, NUMERIC_SCALE AS NumericScale
                  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN @Names ORDER BY TABLE_NAME, ORDINAL_POSITION",
                new { Names = tableNames })).ToList();
            return rows
                .GroupBy(r => r.TableName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets columns per table (dbo) for the given table names. Returns table -> set of column names.
        /// </summary>
        private static async Task<Dictionary<string, HashSet<string>>> GetColumnsByTableAsync(string connStr, List<string> tableNames)
        {
            if (tableNames.Count == 0)
                return new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            using var conn = new SqlConnection(connStr);
            var rows = (await conn.QueryAsync<(string TableName, string ColumnName)>(
                "SELECT TABLE_NAME AS TableName, COLUMN_NAME AS ColumnName FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN @Names",
                new { Names = tableNames })).ToList();
            return rows
                .GroupBy(r => r.TableName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => new HashSet<string>(g.Select(x => x.ColumnName), StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Default path for "tables only in MDC" list (output folder). Option 3 reads this file when present.
        /// </summary>
        public static readonly string MdcOnlyTablesFilePath = Path.Combine("output", "mdc_only_tables.txt");

        /// <summary>
        /// Reads table names from a file produced by WriteNumberedTableListToFileAsync (lines like "1. TableName").
        /// Returns empty list if file does not exist or is invalid.
        /// </summary>
        public static async Task<List<string>> ReadTableListFromNumberedFileAsync(string filePath)
        {
            if (!File.Exists(filePath))
                return new List<string>();
            var lines = await File.ReadAllLinesAsync(filePath);
            var list = new List<string>();
            var inNumberedSection = false;
            foreach (var line in lines)
            {
                if (line.TrimStart().StartsWith("--- Numbered list ---", StringComparison.OrdinalIgnoreCase))
                    inNumberedSection = true;
                else if (inNumberedSection)
                {
                    var trimmed = line.Trim();
                    var dotIndex = trimmed.IndexOf('.');
                    if (dotIndex > 0 && int.TryParse(trimmed.AsSpan(0, dotIndex).Trim(), out _))
                    {
                        var name = trimmed.Substring(dotIndex + 1).Trim();
                        if (!string.IsNullOrEmpty(name))
                            list.Add(name);
                    }
                }
            }
            return list;
        }

        /// <summary>
        /// Writes a list of table names to a file with numbering (1. TableA, 2. TableB, ...).
        /// Ensures the directory of filePath exists before writing.
        /// </summary>
        /// <param name="title">Optional. Header title for the report. If null, a generic title is used.</param>
        public static async Task WriteNumberedTableListToFileAsync(List<string> tableNames, string filePath, string? title = null)
        {
            var dir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);
            var header = title ?? "Table list";
            var lines = new List<string>
            {
                $"=== {header} ===",
                $"Total: {tableNames.Count} table(s)",
                "",
                "--- Numbered list ---"
            };
            for (var i = 0; i < tableNames.Count; i++)
                lines.Add($"{i + 1}. {tableNames[i]}");
            await File.WriteAllLinesAsync(filePath, lines);
        }

        /// <summary>
        /// Gets dbo base table names from the database and filters out tables that should be skipped
        /// (backup/archive with date suffixes, temporary tables, tmp_*, etc.) per TableSkipRules.
        /// </summary>
        private static async Task<List<string>> GetTableListFilteredAsync(string connStr)
        {
            using var conn = new SqlConnection(connStr);
            var tables = (await conn.QueryAsync<string>(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'")).ToList();
            return tables.Where(t => !TableSkipRules.ShouldSkipTable(t)).ToList();
        }
    }

    /// <summary>
    /// Same column name in both MDC and ADC but different type. Holds both schemas for comparison.
    /// </summary>
    public class ColumnTypeMismatchRow
    {
        public ColumnSchema Mdc { get; set; } = null!;
        public ColumnSchema Adc { get; set; } = null!;
    }
}
