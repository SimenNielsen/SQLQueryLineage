using SQLQueryLineage.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQLQueryLineage;

public static class Parsers
{
    public static ProcedureStatement? ParseSelect(SelectStatement selectStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.SELECT);
        try
        {
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
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        return procedureStatement;
    }
    public static ProcedureStatement? ParseInsert(InsertStatement insertStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.INSERT);
        try
        {
            List<UpstreamReference> ctes = ProcParserUtils.GetCommonTableExpressionLineage(insertStatement.WithCtesAndXmlNamespaces);
            var source = insertStatement.InsertSpecification.InsertSource;
            TableLineage lineage = new TableLineage();
            lineage.ctes = ctes;
            lineage = ProcParserUtils.GetInsertSource(source, lineage);
            var target = ProcParserUtils.GetTableReferenceTableAlias(insertStatement.InsertSpecification.Target, lineage);
            lineage.target = target;
            var targetColumns = insertStatement.InsertSpecification.Columns;
            if (targetColumns.Count == 0)
            {
                procedureStatement.Columns = lineage.transformColumns;
            }
            else
            {
                if (targetColumns.Count == lineage.transformColumns.Count)
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
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        return procedureStatement;
    }
    public static ProcedureStatement? ParseUpdate(UpdateStatement updateStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.UPDATE);
        try
        {
            List<UpstreamReference> ctes = ProcParserUtils.GetCommonTableExpressionLineage(updateStatement.WithCtesAndXmlNamespaces);
            TableLineage lineage = new TableLineage();
            lineage.ctes = ctes;
            var fromClause = updateStatement.UpdateSpecification.FromClause;
            if (fromClause != null)
            {
                lineage = ProcParserUtils.GetFromClauseLineage(updateStatement.UpdateSpecification.FromClause, lineage);
            }
            // Target might be alias set in the FromClause.
            var target = ProcParserUtils.GetTableReferenceTableAlias(updateStatement.UpdateSpecification.Target, lineage);
            lineage.target = target;
            if (fromClause == null) // Target may act as an upstream source if the from clause is empty.
            {
                ProcParserUtils.AddUpstreamReferenceTableIfNotExists(target, lineage);
            }
            var setClauses = updateStatement.UpdateSpecification.SetClauses;
            foreach (var setclause in setClauses)
            {
                var setColumn = ProcParserUtils.GetSetClauseColumn(setclause, lineage);
                procedureStatement.AddColumn(setColumn);
            }

            procedureStatement.SetTarget(lineage.target);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        return procedureStatement;
    }
    public static ProcedureStatement? ParseExecute(ExecuteStatement executeStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.EXECUTE);
        try
        {
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
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        return procedureStatement;
    }
    public static ProcedureStatement? ParseMerge(MergeStatement mergeStatement)
    {
        var procedureStatement = new ProcedureStatement(ProcedureStatementType.MERGE);
        try
        {
            List<UpstreamReference> ctes = ProcParserUtils.GetCommonTableExpressionLineage(mergeStatement.WithCtesAndXmlNamespaces);
            TableLineage lineage = new TableLineage();
            lineage.ctes = ctes;
            var sourceLineage = ProcParserUtils.GetTableReferenceLineage(mergeStatement.MergeSpecification.TableReference, lineage);
            var target = ProcParserUtils.GetTableReferenceTableAlias(mergeStatement.MergeSpecification.Target, lineage);
            lineage.target = target;
            var actions = mergeStatement.MergeSpecification.ActionClauses;
            var targetColumns = new List<Column>();
            foreach (var action in actions)
            {
                var parsedColumns = ProcParserUtils.GetActionClauseTargetColumns(action, lineage);
                targetColumns.AddRange(parsedColumns);
            }
            targetColumns.ForEach(c => { procedureStatement.AddColumn(c); });
            procedureStatement.SetTarget(lineage.target);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        return procedureStatement;
    }
}

