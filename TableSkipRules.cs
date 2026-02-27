using System.Text.RegularExpressions;

namespace Logistics.DbMerger
{
    /// <summary>
    /// Central place for table skip rules (system/temp/backup/etc.).
    /// Used by schema sync, FK map, column diff, Helper table lists, etc.
    /// </summary>
    public static class TableSkipRules
    {
        public static bool ShouldSkipTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                return true;

            var name = tableName.Trim();

            // System / meta tables we always skip
            if (name.Equals("sysdiagrams", StringComparison.OrdinalIgnoreCase) ||
                name.Equals("Tenants", StringComparison.OrdinalIgnoreCase) ||
                name.Equals("Users", StringComparison.OrdinalIgnoreCase) ||
                name.StartsWith("__", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            // Temporary tables (TEMP*, tmp_*, TempCommercialVolume, etc.)
            var tempNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "TEMPTimeband",
                "TEMPTimebandDetail",
                "TEMPVolumeDetail",
                "TempCommercialVolume"
            };

            if (tempNames.Contains(name) || name.StartsWith("tmp_", StringComparison.OrdinalIgnoreCase))
                return true;

            if (name.StartsWith("temp", StringComparison.OrdinalIgnoreCase) &&
                !name.StartsWith("template", StringComparison.OrdinalIgnoreCase))
                return true;

            // Contains "temp" anywhere but not "template" (e.g. KronosEmployeeTemps, ClockingTransactionSyncTempDatas)
            if (name.IndexOf("temp", StringComparison.OrdinalIgnoreCase) >= 0 &&
                name.IndexOf("template", StringComparison.OrdinalIgnoreCase) < 0)
                return true;

            if (name.IndexOf("_bak", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            // Backup/archive tables with date suffix (e.g. Contact_20230615, contact14112022, Timeband15052025, VolumeDetail20250919)
            if (Regex.IsMatch(name, @"\d{8}$") || Regex.IsMatch(name, @"\d{6}$"))
                return true;

            return false;
        }
    }
}
