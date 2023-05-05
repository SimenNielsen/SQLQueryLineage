namespace SQLQueryLineage.Common
{
    public enum UpstreamType
    {
        TABLE,
        QUERY
    }
    public class UpstreamReference
    {
        public readonly UpstreamType type;
        public readonly TableAlias table;
        public readonly TableLineage query;
        public string CTEAlias;
        public string alias;
        public UpstreamReference(UpstreamType type, string alias = null, TableAlias table = null, TableLineage query = null, string CTEAlias = null)
        {
            this.type = type;
            this.alias = alias;
            this.table = table;
            this.query = query;
            this.CTEAlias = CTEAlias;
        }
    }
}
