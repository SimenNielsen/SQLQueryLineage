namespace SQLQueryLineage.Common;

public class TableLineage
{
    public List<UpstreamReference> upstream { get; set; } // Select, Update
    public List<UpstreamReference> ctes { get; set; } // Select, Update
    public List<Column> transformColumns { get; set; }
    public List<TableAlias> executions { get; set; }
    public TableAlias target { get; set; } //Insert, Into, Update etc..
    public TableLineage()
    {
        this.upstream = new List<UpstreamReference>();
        this.ctes = new List<UpstreamReference>();
        this.transformColumns = new List<Column>();
        this.executions = new List<TableAlias>();
    }
}
