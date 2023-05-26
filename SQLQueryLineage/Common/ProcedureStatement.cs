namespace SQLQueryLineage.Common;

public class ProcedureStatement
{
    public ProcedureStatementType Type { get; set; }
    public List<Column> Columns { get; set; }
    public TableAlias Target { get; set; }
    public ProcedureStatement(ProcedureStatementType type)
    {
        this.Type = type;
        this.Columns = new List<Column>();
    }
    public void AddColumn(Column column)
    {
        this.Columns.Add(column);
    }
    public List<Column> GetColumns()
    {
        return this.Columns;
    }
    public TableAlias GetTarget()
    {
        return Target;
    }
    public void SetTarget(TableAlias target)
    {
        this.Target = target;
    }
}
