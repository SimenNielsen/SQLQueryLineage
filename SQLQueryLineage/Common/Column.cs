namespace SQLQueryLineage.Common
{
    public class Column
    {
        public string name { get; set; }
        public string alias { get; set; }
        public TableAlias tableAlias { get; set; }
        public string? logic { get; set; }
        public List<Column> sourceColumns { get; set; }
        public string? upstreamReferenceAlias { get; set; }

        public Column(string name, string? alias = null, string? logic = null, TableAlias tableAlias = null)
        {
            this.name = name != null ? name.ToLower() : null;
            this.logic = logic;
            this.alias = alias == null ? this.name : alias.ToLower();
            this.tableAlias = tableAlias;
            this.sourceColumns = new List<Column>();
        }

        public void AddSourceColumn(Column sourceColumn)
        {
            this.sourceColumns.Add(sourceColumn);
        }
        public List<Column> GetSourceColumns()
        {
            return this.sourceColumns;
        }
        public void SetSourceColumns(List<Column> sourceColumns)
        {
            this.sourceColumns = sourceColumns;
        }
    }
}
