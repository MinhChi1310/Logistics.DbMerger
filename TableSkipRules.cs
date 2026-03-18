using System.Text.RegularExpressions;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Central place for table and object skip rules (system/temp/backup/etc.).
    /// Used by schema sync, object sync, FK map, column diff, Helper table lists, etc.
    /// </summary>
    public static class TableSkipRules
    {
        // Compiled regex patterns for performance (called hundreds of times per migration)
        private static readonly Regex DateSuffix8 = new(@"\d{8}$", RegexOptions.Compiled);
        private static readonly Regex UnderscoreDateSuffix = new(@"_\d{6,8}$", RegexOptions.Compiled);
        private static readonly Regex BackupSuffix = new(@"_(bak|backup|old|test)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        // System / meta tables we always skip
        private static readonly HashSet<string> SystemTables = new(StringComparer.OrdinalIgnoreCase)
        {
            "sysdiagrams",
            "Tenants",
            "Users",
            // Site-specific report staging tables (no PK, not operational data)
            "EPNextWeek",
            "EPThisWeek",
            "KewdaleNextWeek",
            "KewdaleThisWeek",
            // Temp user copy
            "Users_tmp"
        };

        /// <summary>
        /// Determines if a table should be skipped during schema sync and data migration.
        /// Skips system tables, temp tables, backup/archive tables, and date-stamped snapshots.
        /// </summary>
        public static bool ShouldSkipTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                return true;

            var name = tableName.Trim();

            // System / meta tables
            if (SystemTables.Contains(name) || name.StartsWith("__", StringComparison.OrdinalIgnoreCase))
                return true;

            // Prefix: tmp_*
            if (name.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
                return true;

            // Prefix: TEMP* but NOT Template*
            if (name.StartsWith("temp", StringComparison.OrdinalIgnoreCase) &&
                !name.StartsWith("template", StringComparison.OrdinalIgnoreCase))
                return true;

            // Suffix: _bak, _backup, _old, _test (case-insensitive)
            if (BackupSuffix.IsMatch(name))
                return true;

            // Date-stamped suffix: trailing 8 digits (e.g. contact14112022, VolumeDetailMaster20221129)
            if (DateSuffix8.IsMatch(name))
                return true;

            // Underscore + date suffix: _NNNNNN or _NNNNNNNN (e.g. ContactTemplateScheduleOverride_211206)
            if (UnderscoreDateSuffix.IsMatch(name))
                return true;

            return false;
        }

        /// <summary>
        /// Determines if a database object (stored procedure, function, etc.) should be skipped
        /// during object sync. Filters out backup/test/old copies and date-stamped versions.
        /// </summary>
        public static bool ShouldSkipObject(string objectName)
        {
            if (string.IsNullOrWhiteSpace(objectName))
                return true;

            var name = objectName.Trim();

            // Suffix: _bak, _backup, _old, _test (case-insensitive)
            if (BackupSuffix.IsMatch(name))
                return true;

            // Date-stamped suffix: trailing 8 digits (e.g. InsertBulkVolume_20220802)
            if (DateSuffix8.IsMatch(name))
                return true;

            // Underscore + date suffix (e.g. sp_Report_211206)
            if (UnderscoreDateSuffix.IsMatch(name))
                return true;

            return false;
        }
    }
}
