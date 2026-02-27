namespace Logistics.DbMerger
{
    public class ColumnSchema
    {
        public string TableName { get; set; } = string.Empty;
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public int? CharacterMaximumLength { get; set; }
        public byte? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
    }
}
