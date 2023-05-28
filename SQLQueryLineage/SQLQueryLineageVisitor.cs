using Microsoft.SqlServer.TransactSql.ScriptDom;
using SQLQueryLineage.Common;

namespace SQLQueryLineage;

public class SQLQueryLineageVisitor : TSqlFragmentVisitor
{
    public List<ProcedureStatement> ProcedureEvents = new List<ProcedureStatement>();
    public SQLQueryLineageVisitor() { }
    public override void ExplicitVisit(CreateViewStatement node)
    {
        var statement = Parsers.ParseSelect(node.SelectStatement);
        var targetView = ProcParserUtils.GetSchemaObjectTable(node.SchemaObjectName, new TableLineage());
        statement.SetTarget(targetView);
        if (statement != null)
        {
            ProcedureEvents.Add(statement);
        }
        //base.ExplicitVisit(node);
    }
    public override void ExplicitVisit(SelectStatement node)
    {
        var statement = Parsers.ParseSelect(node);
        if (statement != null )
        {
            ProcedureEvents.Add(statement);
        }
        base.ExplicitVisit(node);
    }
    public override void ExplicitVisit(InsertStatement node)
    {
        var statement = Parsers.ParseInsert(node);
        if (statement != null)
        {
            ProcedureEvents.Add(statement);
        }
        base.ExplicitVisit(node);
    }
    public override void ExplicitVisit(UpdateStatement node)
    {
        var statement = Parsers.ParseUpdate(node);
        if (statement != null)
        {
            ProcedureEvents.Add(statement);
        }
        base.ExplicitVisit(node);
    }
    public override void ExplicitVisit(ExecuteStatement node)
    {
        var statement = Parsers.ParseExecute(node);
        if (statement != null)
        {
            ProcedureEvents.Add(statement);
        }
        base.ExplicitVisit(node);
    }
    public override void ExplicitVisit(MergeStatement node)
    {
        var statement = Parsers.ParseMerge(node);
        if (statement != null)
        {
            ProcedureEvents.Add(statement);
        }
        base.ExplicitVisit(node);
    }
}