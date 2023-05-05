using SQLQueryLineage.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQLQueryLineage;

public static class Parsers
{
    public static ProcedureStatement? ParseSelect(SelectStatement selectStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.SELECT);
        List<UpstreamReference> ctes = ProcParserUtils.GetCommonTableExpressionLineage(selectStatement.WithCtesAndXmlNamespaces);
        // Get select elements and compare with CTE lineage column aliases
        TableLineage selectLineage = new TableLineage();
        selectLineage.ctes = ctes;
        selectLineage = ProcParserUtils.GetQueryExpressionLineage(selectStatement.QueryExpression, selectLineage);
        if (selectStatement.Into != null) //only select, e.g. a view
        {
            procedureStatement.Type = ProcedureStatementType.SELECT_INTO;
            var intoTable = ProcParserUtils.GetSchemaObjectTable(selectStatement.Into, selectLineage);
            selectLineage.target = intoTable;
            procedureStatement.SetTarget(selectLineage.target);
        }
        
        foreach (var column in selectLineage.transformColumns)
        {
            procedureStatement.AddColumn(column);
        }
        return procedureStatement;
    }
    public static ProcedureStatement? ParseInsert(InsertStatement insertStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.INSERT);
        List<UpstreamReference> ctes = ProcParserUtils.GetCommonTableExpressionLineage(insertStatement.WithCtesAndXmlNamespaces);
        var source = insertStatement.InsertSpecification.InsertSource;
        TableLineage lineage = new TableLineage();
        lineage.ctes = ctes;
        lineage = ProcParserUtils.GetInsertSource(source, lineage);
        var target = insertStatement.InsertSpecification.Target;
        switch(target)
        {
            case NamedTableReference ntr:
                var alias = ntr.Alias?.Value;
                if(alias != null)
                {
                    var existingUpstream = ProcParserUtils.GetTableUpstream(alias, lineage);
                    if(existingUpstream != null)
                    {
                        lineage.target = existingUpstream.table;
                    }
                }
                else
                {
                    //table name might still be alias
                    var schemaObject = ntr.SchemaObject;
                    var existingUpstream = ProcParserUtils.GetTableUpstream(schemaObject.BaseIdentifier.Value, lineage);
                    if (existingUpstream != null)
                    {
                        lineage.target = existingUpstream.table;
                    }
                    else
                    {
                        lineage.target = ProcParserUtils.GetSchemaObjectTable(schemaObject, lineage, alias: alias);
                    }
                }
                break;
            default:
                break;
        }
        var targetColumns = insertStatement.InsertSpecification.Columns;
        if(targetColumns.Count == 0)
        {
            procedureStatement.Columns = lineage.transformColumns;
        }
        else
        {
            if(targetColumns.Count == lineage.transformColumns.Count)
            {
                for (var i = 0; i < targetColumns.Count; i++)
                {
                    var pcol = ProcParserUtils.GetColumnReferenceExpressionColumn(targetColumns[i], lineage);
                    pcol.AddSourceColumn(lineage.transformColumns[i]);
                    procedureStatement.AddColumn(pcol);
                }
            }
        }
        
        procedureStatement.SetTarget(lineage.target);
        return procedureStatement;
    }
    public static ProcedureStatement? ParseUpdate(UpdateStatement updateStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.INSERT);
        List<UpstreamReference> ctes = ProcParserUtils.GetCommonTableExpressionLineage(updateStatement.WithCtesAndXmlNamespaces);
        TableLineage lineage = new TableLineage();
        lineage.ctes = ctes;
        var fromClause = updateStatement.UpdateSpecification.FromClause;
        if(fromClause != null)
        {
            lineage = ProcParserUtils.GetFromClauseLineage(updateStatement.UpdateSpecification.FromClause, lineage);
        }
        var target = updateStatement.UpdateSpecification.Target;
        switch (target)
        {
            case NamedTableReference ntr:
                var alias = ntr.Alias?.Value;
                if (alias != null)
                {
                    var existingUpstream = ProcParserUtils.GetTableUpstream(alias, lineage);
                    if (existingUpstream != null)
                    {
                        lineage.target = existingUpstream.table;
                    }
                }
                else
                {
                    //table name might still be alias
                    var schemaObject = ntr.SchemaObject;
                    var existingUpstream = ProcParserUtils.GetTableUpstream(schemaObject.BaseIdentifier.Value, lineage);
                    if (existingUpstream != null)
                    {
                        lineage.target = existingUpstream.table;
                    }
                    else
                    {
                        lineage.target = ProcParserUtils.GetSchemaObjectTable(schemaObject, lineage, alias: alias);
                    }
                }
                break;
            default:
                throw new NotSupportedException();
        }
        var setClauses = updateStatement.UpdateSpecification.SetClauses;
        foreach (var setclause in setClauses)
        {
            var setColumn = ProcParserUtils.GetSetClauseColumn(setclause, lineage);
            procedureStatement.AddColumn(setColumn);
        }

        procedureStatement.SetTarget(lineage.target);
        return procedureStatement;
    }
    public static ProcedureStatement? ParseExecute(ExecuteStatement executeStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.EXECUTE);
        TableLineage lineage = new TableLineage();
        var specification = executeStatement.ExecuteSpecification;
        var entity = specification.ExecutableEntity;
        switch (entity)
        {
            case ExecutableProcedureReference epr:
                var procedure = epr.ProcedureReference.ProcedureReference;
                var tableAlias = ProcParserUtils.GetSchemaObjectTable(procedure.Name, lineage);
                lineage.executions.Add(tableAlias);
                procedureStatement.SetTarget(tableAlias);
                break;
        }
        return procedureStatement;
    }
}

