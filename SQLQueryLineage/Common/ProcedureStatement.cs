namespace SQLQueryLineage.Common;

public class ProcedureStatement
{
    public ProcedureStatementType Type;
    public List<Column> Columns;
    public TableAlias Target;
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
