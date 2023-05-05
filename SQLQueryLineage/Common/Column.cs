namespace SQLQueryLineage.Common
{
    public class Column
    {
        public string name;
        public string alias;
        public TableAlias tableAlias;
        public string? logic;
        public List<Column> sourceColumns;
        public string? upstreamReferenceAlias;

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
