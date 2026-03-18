using Serilog;
using System.IO;

namespace Logistics.DbMerger
{
    public static class RollbackLogger
    {
        private static string _filePath;
        private static readonly List<string> _allFilePaths = new();

        // Per-run folder: output/rollbacks/run_20260318_093000/
        private static string _runFolder;

        // Default legacy init
        static RollbackLogger()
        {
            _runFolder = Path.Combine("output", "rollbacks", $"run_{DateTime.Now:yyyyMMdd_HHmmss}");
            Directory.CreateDirectory(_runFolder);
            Initialize("general");
        }

        public static void Initialize(string context)
        {
            _filePath = Path.Combine(_runFolder, $"rollback_{context}.sql");
            if (!_allFilePaths.Contains(_filePath))
                _allFilePaths.Add(_filePath);
            if (!File.Exists(_filePath))
            {
                File.AppendAllText(_filePath, $"-- Rollback Script ({context}) Generated at {DateTime.Now}\n\n");
            }
        }

        /// <summary>
        /// Returns the run folder path for this session (e.g. output/rollbacks/run_20260318_093000).
        /// </summary>
        public static string GetRunFolder() => _runFolder;

        public static string GetCurrentFilePath() => _filePath;

        /// <summary>
        /// Returns all rollback file paths generated during this session.
        /// </summary>
        public static IReadOnlyList<string> GetAllFilePaths() => _allFilePaths;

        public static void LogTableCreation(string tableName)
        {
            var sql = $"IF OBJECT_ID('[dbo].[{tableName.Replace("]", "]]")}]', 'U') IS NOT NULL DROP TABLE [dbo].[{tableName.Replace("]", "]]")}];\n";
            try
            {
                File.AppendAllText(_filePath, sql);
                Log.Information("[Rollback] Added DROP TABLE for {TableName}", tableName);
            }
            catch (Exception ex)
            {
                Log.Warning("[Rollback] Failed to write rollback script for {TableName}: {ErrorMessage}", tableName, ex.Message);
            }
        }

        public static void LogObjectCreation(string objectName, string typeDesc)
        {
            string dropVerb = typeDesc switch
            {
                "Stored Procedures" => "PROCEDURE",
                "Views" => "VIEW",
                "Scalar Functions" => "FUNCTION",
                "Table Functions" => "FUNCTION",
                "Inline Functions" => "FUNCTION",
                "Triggers" => "TRIGGER",
                "Sequences" => "SEQUENCE",
                "Synonyms" => "SYNONYM",
                _ => typeDesc.TrimEnd('s').ToUpperInvariant() // Best-effort: "Procedures" -> "PROCEDURE"
            };

            var sql = $"IF OBJECT_ID('[dbo].[{objectName.Replace("]", "]]")}]') IS NOT NULL DROP {dropVerb} [dbo].[{objectName.Replace("]", "]]")}];\n";
            try
            {
                File.AppendAllText(_filePath, sql);
                Log.Information("[Rollback] Added DROP {DropVerb} for {ObjectName}", dropVerb, objectName);
            }
            catch (Exception ex)
            {
                Log.Warning("[Rollback] Failed to write rollback script for {ObjectName}: {ErrorMessage}", objectName, ex.Message);
            }
        }

        public static void LogCustomScript(string script)
        {
            try
            {
                File.AppendAllText(_filePath, script);
            }
            catch (Exception ex)
            {
                Log.Warning("[Rollback] Failed to write custom rollback script: {ErrorMessage}", ex.Message);
            }
        }
    }
}
