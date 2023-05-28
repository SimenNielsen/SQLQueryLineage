using System.Data.Common;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using SQLQueryLineage;

namespace SQLQueryLineage.Common;
public static class ProcParserUtils {
    public static string defaultSchema = "unassigned";
    public static string defaultDatabase = "dbo";
    public static TableAlias GetSchemaObjectTable(SchemaObjectName schemaObjectName, TableLineage lineage, string alias = null){
        var table = schemaObjectName.BaseIdentifier.Value;
        var schema = schemaObjectName.SchemaIdentifier?.Value;
        var database = schemaObjectName.DatabaseIdentifier?.Value;
        var tableAlias = new TableAlias(table, schemaName: schema, databaseName: database, alias: alias);
        return tableAlias;
    }
    public static UpstreamReference SearchUpstreamTableWithoutAlias(TableLineage lineage, string tableName, string schemaName, string databaseName){
        tableName = tableName.ToLower();
        schemaName = schemaName.ToLower();
        databaseName = databaseName.ToLower();
        foreach (var upstream in lineage.upstream.FindAll(e => e.type == UpstreamType.TABLE)){
            if(upstream.table.tableName == tableName && upstream.table.schemaName == schemaName && upstream.table.databaseName == databaseName)
            {
                return upstream;
            }
        }
        return null;
    }
    public static List<UpstreamReference> GetCommonTableExpressionLineage(WithCtesAndXmlNamespaces withCtesAndXmlNamespaces)
    {
        List<UpstreamReference> result = new List<UpstreamReference>();
        if(withCtesAndXmlNamespaces == null) return result;
        foreach(var cte in withCtesAndXmlNamespaces.CommonTableExpressions){
            if(cte is not CommonTableExpression commonTableExpression){
                continue;
            }
            TableLineage queryExpressionLineage = new TableLineage();
            queryExpressionLineage.ctes = result;
            GetQueryExpressionLineage(commonTableExpression.QueryExpression, queryExpressionLineage);
            string expressionName = cte.ExpressionName.Value.ToLower();
            var upstreamRef = new UpstreamReference(UpstreamType.QUERY, CTEAlias: expressionName, query: queryExpressionLineage);
            result.Add(upstreamRef);
        }
        return result;
    }
    public static TableLineage GetQueryExpressionLineage(QueryExpression queryExpression, TableLineage lineage){
        switch (queryExpression)
        {
            case QuerySpecification querySpecification:
                GetQuerySpecificationLineage(querySpecification, lineage);
                break;
            case BinaryQueryExpression binaryQueryExpression:
                 GetBinaryQueryExpressionLineage(binaryQueryExpression, lineage);
                break;
            case QueryParenthesisExpression queryParenthesisExpression:
                GetQueryExpressionLineage(queryParenthesisExpression.QueryExpression, lineage);
                break;
            default:
                throw new NotSupportedException($"QueryExpression type not supported: {queryExpression.GetType().Name}");
        }
        return lineage;
    }

    public static TableLineage GetBinaryQueryExpressionLineage(BinaryQueryExpression binaryQueryExpression, TableLineage lineage){
        var leftLineage = new TableLineage();
        leftLineage = GetQueryExpressionLineage(binaryQueryExpression.FirstQueryExpression, leftLineage);
        var rightLineage = new TableLineage();
        rightLineage = GetQueryExpressionLineage(binaryQueryExpression.SecondQueryExpression, rightLineage);
        for(int i = 0; i < leftLineage.transformColumns.Count; i++)
        {
            var transforColumn = new Column(leftLineage.transformColumns[i].alias);
            transforColumn.AddSourceColumn(leftLineage.transformColumns[i]);
            transforColumn.AddSourceColumn(rightLineage.transformColumns[i]);
            lineage.transformColumns.Add(transforColumn);
        }
        return lineage;
    }

    public static UpstreamReference GetColumnUpstreamReference(List<UpstreamReference> upstreamList, Column column)
    {
        if (column.sourceColumns.Count > 0)
        {
            return null; // Column originates from subcolumns
        }
        if (column.logic != null)
        {
            // TODO: Implement logic parsing. E.g. RANK() OVER()
            return null; 
        }
        if(upstreamList.Count == 1)
        {
            return upstreamList[0];
        }
        else
        {
            if(column.upstreamReferenceAlias != null && column.upstreamReferenceSchema != null)
            {
                return upstreamList.Find(x => x.table.schemaName == column.upstreamReferenceSchema && x.table.tableName == column.upstreamReferenceAlias);
            }
            else if (column.upstreamReferenceAlias != null)
            {
                return upstreamList.Find(x => x.alias == column.upstreamReferenceAlias);
            }
            else
            {
                foreach(var upstream in upstreamList.FindAll(u => u.type == UpstreamType.QUERY))
                {
                    var colMatch = upstream.query.transformColumns.FirstOrDefault(c => c.alias == column.name);
                    if (colMatch != null) return upstream;
                }
                foreach (var upstream in upstreamList.FindAll(u => u.type == UpstreamType.TABLE))
                {
                    var tableAlias = upstream.table;
                    var tableColumns = SqlUtils.ReadTableColumnsData(tableAlias);
                    var colMatch = tableColumns.FirstOrDefault(c => c.alias == column.name);
                    if (colMatch != null) return upstream;
                }
            }
        }
        return null;
    }
    public static List<Column> GetSourceColumn(TableLineage lineage, Column targetColumn)
    {
        var result = new List<Column>();
        var relevantUpstream = GetColumnUpstreamReference(lineage.upstream, targetColumn);
        foreach (var upstream in lineage.upstream)
        {
            if (relevantUpstream != upstream) continue;
            if(upstream.type == UpstreamType.QUERY)
            {
                foreach(var scol in upstream.query.transformColumns)
                {
                    if (scol.alias == targetColumn.name)
                    {
                        result.Add(scol);
                    }
                }
            }
        }
        return result;
    }
    public static TableLineage GetQuerySpecificationLineage(QuerySpecification querySpecification, TableLineage lineage){
        if(querySpecification.FromClause != null){
            GetFromClauseLineage(querySpecification.FromClause, lineage);
        }
        if(querySpecification.SelectElements != null){
            GetSelectElementsLineage(querySpecification.SelectElements, lineage);
        }
        return lineage;
    }
    public static TableLineage GetSelectElementsLineage(IList<SelectElement> selectElements, TableLineage lineage){
        foreach(var selectElement in selectElements){
            GetSelectElementLineage(selectElement, lineage);
        }
        return lineage;
    }
    public static TableLineage GetSelectElementLineage(SelectElement selectElement, TableLineage lineage){
        switch (selectElement)
        {
            case SelectScalarExpression selectScalarExpression:
                GetSelectScalarExpressionLineage(selectScalarExpression, lineage);
                break;
            case SelectStarExpression selectStarExpression:
                var selectStarTableAliasValue = selectStarExpression.Qualifier?.Identifiers.FirstOrDefault()?.Value;
                if(selectStarTableAliasValue != null)
                {
                    var upstream = GetAnyUpstream(selectStarTableAliasValue, lineage.upstream);
                    if(upstream.type == UpstreamType.TABLE)
                    {
                        GetSelectStarExpressionLineage(selectStarExpression, upstream.table, lineage);
                    }
                    else if (upstream.type == UpstreamType.QUERY)
                    {
                        lineage.transformColumns.AddRange(upstream.query.transformColumns);
                    }
                }
                else
                {
                    foreach(var upstream in lineage.upstream)
                    {
                        if (upstream.type == UpstreamType.TABLE)
                        {
                            GetSelectStarExpressionLineage(selectStarExpression, upstream.table, lineage);
                        }
                        else if (upstream.type == UpstreamType.QUERY)
                        {
                            lineage.transformColumns.AddRange(upstream.query.transformColumns);
                        }
                    }
                }
                break;
            case SelectSetVariable selectSetVariable:
                GetSelectSetVariableLineage(selectSetVariable, lineage);
                break;
            default:
                throw new NotSupportedException($"SelectElement type not supported: {selectElement.GetType().Name}");
        }
        return lineage;
    }
    public static void SetColumnUpstream(List<UpstreamReference> upstreams, Column column)
    {
        var relevantUpstream = GetColumnUpstreamReference(upstreams, column);
        if (relevantUpstream != null)
        {
            if (relevantUpstream.type == UpstreamType.QUERY)
            {
                var sourceCol = relevantUpstream.query.transformColumns.FirstOrDefault(c => c.alias == column.name);
                if(sourceCol != null && column.GetSourceColumns().Count == 0)
                {
                    column.AddSourceColumn(sourceCol);
                }
            }
            else
            {
                column.tableAlias = relevantUpstream.table;
            }
        }
    }
    public static TableLineage GetSelectScalarExpressionLineage(SelectScalarExpression selectScalarExpression, TableLineage lineage){
        var columnAlias = selectScalarExpression.ColumnName?.Value;
        columnAlias = columnAlias != null ? columnAlias.ToLower() : null;
        var columns = GetScalarExpressionColumns(selectScalarExpression.Expression, lineage, columnAlias);
        if (columns.Count() == 1)
        {
            SetColumnUpstream(lineage.upstream, columns[0]);
            lineage.transformColumns.Add(columns[0]);
        }
        else if(columns.Count() > 1)
        {
            var transformColumn = new Column(columns[0].alias, logic: columns[0].logic);
            foreach(var column in columns){
                SetColumnUpstream(lineage.upstream, column);
                transformColumn.AddSourceColumn(column);
            }
            lineage.transformColumns.Add(transformColumn);
        }
        return lineage;
    }
    public static List<Column> GetColumnSearchedCaseExpressionColumns(SearchedCaseExpression searchedCaseExpression, TableLineage lineage){
        List<Column> result = new List<Column>();
        foreach(var whenClause in searchedCaseExpression.WhenClauses){
            var columns = GetScalarExpressionColumns(whenClause.ThenExpression, lineage);
            result.AddRange(columns);
        }
        if(searchedCaseExpression.ElseExpression != null){
            //return result;
            var elseColumns = GetScalarExpressionColumns(searchedCaseExpression.ElseExpression, lineage);
            result.AddRange(elseColumns);
        }
        return result;
    }
    public static List<Column> GetColumnSimpleCaseExpressionColumns(SimpleCaseExpression caseExpression, TableLineage lineage)
    {
        List<Column> result = new List<Column>();
        foreach (var whenClause in caseExpression.WhenClauses)
        {
            var columns = GetScalarExpressionColumns(whenClause.ThenExpression, lineage);
            result.AddRange(columns);
        }
        if (caseExpression.ElseExpression != null)
        {
            //return result;
            var elseColumns = GetScalarExpressionColumns(caseExpression.ElseExpression, lineage);
            result.AddRange(elseColumns);
        }
        return result;
    }
    public static Column GetSetClauseColumn(SetClause setClause, TableLineage lineage)
    {
        Column result = null;
        switch (setClause)
        {
            case AssignmentSetClause assignmentSetClause:
                var sourceColumns = GetScalarExpressionColumns(assignmentSetClause.NewValue, lineage);
                var targetColumn = GetColumnReferenceExpressionColumn(assignmentSetClause.Column, lineage);
                targetColumn.SetSourceColumns(sourceColumns);
                result = targetColumn;
                break;
        }
        return result;
    }
    public static List<Column> GetScalarExpressionColumns(ScalarExpression scalarExpression, TableLineage lineage, string columnAlias = null){
        List<Column> columns = new List<Column>();
        string logic = null;
        switch(scalarExpression){
            case ColumnReferenceExpression columnReferenceExpression:
                var column = GetColumnReferenceExpressionColumn(columnReferenceExpression, lineage);
                if (columnAlias != null) column.alias = columnAlias;
                columns.Add(column);
                break;
            case SearchedCaseExpression caseExpression:
                var thenColumns = GetColumnSearchedCaseExpressionColumns(caseExpression, lineage);
                logic = GetScriptTokenStreamText(caseExpression);
                var newCol = new Column(columnAlias ?? logic, logic: logic);
                newCol.SetSourceColumns(thenColumns);
                columns.Add(newCol);
                break;
            case ParenthesisExpression parenthesisExpression:
                columns = GetScalarExpressionColumns(parenthesisExpression.Expression, lineage);
                logic = GetScriptTokenStreamText(parenthesisExpression);
                foreach(var c in columns){
                    c.logic = logic;
                    c.alias = columnAlias;
                }
                break;
            case UnaryExpression unaryExpression:
                columns = GetScalarExpressionColumns(unaryExpression.Expression, lineage);
                logic = GetScriptTokenStreamText(unaryExpression);
                foreach(var c in columns){
                    c.logic = logic;
                    c.alias = columnAlias;
                }
                break;
            case FunctionCall functionCall:
                var functionColumns = GetFunctionCallColumns(functionCall, lineage);
                columns = new List<Column>();
                if(functionColumns != null){
                    logic = GetScriptTokenStreamText(functionCall);
                    var newColumn = new Column(columnAlias ?? logic, logic: logic);
                    newColumn.SetSourceColumns(functionColumns);
                    columns.Add(newColumn);
                }
                break;
            case ConvertCall convertCall:
                columns = GetScalarExpressionColumns(convertCall.Parameter, lineage, columnAlias);
                logic = GetScriptTokenStreamText(convertCall);
                foreach(var c in columns){
                    c.logic = logic;
                    c.alias = columnAlias;
                }
                break;
            case BinaryExpression binaryExpression:
                var leftColumns = GetScalarExpressionColumns(binaryExpression.FirstExpression, lineage);
                var rightColumns = GetScalarExpressionColumns(binaryExpression.SecondExpression, lineage);
                columns.AddRange(leftColumns);
                columns.AddRange(rightColumns);
                logic = GetScriptTokenStreamText(binaryExpression);
                foreach(var c in columns){
                    c.logic = logic;
                    c.alias = columnAlias;
                }
                break;
            case StringLiteral stringLiteral:
                columns.Add(new Column(columnAlias));
                break;
            case NumericLiteral numericLiteral:
                columns.Add(new Column(columnAlias));
                break;
            case NullLiteral nullLiteral:
                columns.Add(new Column(columnAlias));
                break;
            case IntegerLiteral integerLiteral:
                columns.Add(new Column(columnAlias));
                break;
            case VariableReference variableReference:
                columns.Add(new Column(columnAlias));
                break;
            case GlobalVariableExpression globalVariableExpression:
                columns.Add(new Column(columnAlias));
                break;
            case CoalesceExpression ce:
                foreach(var c in ce.Expressions)
                {
                    var subCols = GetScalarExpressionColumns(c, lineage);
                    foreach(var col in subCols)
                    {
                        if(col.isBlank() == false)
                        {
                            columns.Add(col);
                        }
                    }
                }
                break;
            case IdentityFunctionCall ifc:
                columns.Add(new Column(columnAlias));
                break;
            case CastCall castCall:
                columns = GetScalarExpressionColumns(castCall.Parameter, lineage);
                break;
            case NullIfExpression nie:
                var nieColumns = GetScalarExpressionColumns(nie.FirstExpression, lineage);
                nieColumns.AddRange(GetScalarExpressionColumns(nie.SecondExpression, lineage));
                columns = nieColumns;
                break;
            case IIfCall iif:
                var iifColumns = GetScalarExpressionColumns(iif.ThenExpression, lineage);
                iifColumns.AddRange(GetScalarExpressionColumns(iif.ElseExpression, lineage));
                columns = iifColumns;
                break;
            case TryCastCall tcc:
                columns = GetScalarExpressionColumns(tcc.Parameter, lineage);
                break;
            case TryConvertCall tcc:
                columns = GetScalarExpressionColumns(tcc.Parameter, lineage);
                break;
            case BinaryLiteral bl:
                columns.Add(new Column(columnAlias));
                break;
            case ParameterlessCall parameterlessCall:
                columns.Add(new Column(columnAlias));
                break;
            case LeftFunctionCall fc:
                columns = GetScalarExpressionColumns(fc.Parameters[0], lineage);
                break;
            case RightFunctionCall fc:
                columns = GetScalarExpressionColumns(fc.Parameters[0], lineage);
                break;
            case SimpleCaseExpression caseExpression:
                columns = GetColumnSimpleCaseExpressionColumns(caseExpression, lineage);
                logic = GetScriptTokenStreamText(caseExpression);
                foreach (var c in columns)
                {
                    c.logic = logic;
                    c.alias = columnAlias;
                }
                break;
            case ScalarSubquery ssq:
                var queryLineage = new TableLineage();
                queryLineage = GetQueryExpressionLineage(ssq.QueryExpression, queryLineage);
                columns = queryLineage.transformColumns;
                break;
            default:
                throw new NotSupportedException($"ScalarExpression type not supported: {scalarExpression.GetType().Name}");
        }
        return columns;
    }
    public static string GetScriptTokenStreamText(TSqlFragment fragment)
        {
            StringBuilder tokenText = new StringBuilder();
            for (int counter = fragment.FirstTokenIndex; counter <= fragment.LastTokenIndex; counter++)
            {
                tokenText.Append(fragment.ScriptTokenStream[counter].Text);
            }
            return tokenText.ToString();
        }
    public static List<Column> GetFunctionCallColumns(FunctionCall functionCall, TableLineage lineage){
        List<Column> columns = new List<Column>();
        foreach(var parameter in functionCall.Parameters){
            if(parameter is ColumnReferenceExpression columnReferenceExpression){
                var column = GetColumnReferenceExpressionColumn(columnReferenceExpression, lineage);
                columns.Add(column);
            }
            else if(parameter is ScalarExpression scalarExpression){
                columns.AddRange(GetScalarExpressionColumns(scalarExpression, lineage));
            }
            else{
                throw new NotSupportedException($"FunctionCall parameter type not supported: {parameter.GetType().Name}");
            }
        }
        return columns;
    }
    public static Column GetColumnFromIdentifier(IList<Identifier> columnParts){
        var columnName = columnParts.Last().Value;

        var column = new Column(
            columnName
        );
        if(columnParts.Count > 2)
        {
            column.upstreamReferenceSchema = columnParts[columnParts.Count-3].Value.ToLower();
            column.upstreamReferenceAlias = columnParts[columnParts.Count-2].Value.ToLower();
        }
        else if (columnParts.Count > 1)
        {
            column.upstreamReferenceAlias = columnParts[columnParts.Count - 2].Value.ToLower();
        }
        return column;
    }
    public static Column GetColumnReferenceExpressionColumn(ColumnReferenceExpression columnReferenceExpression, TableLineage lineage){
        if(columnReferenceExpression.ColumnType == ColumnType.Wildcard){
            return null;
        }
        var columnParts = columnReferenceExpression.MultiPartIdentifier.Identifiers;
        var column = GetColumnFromIdentifier(columnParts);
        SetColumnUpstream(lineage.upstream, column);
        return column;
    }
    public static TableLineage GetSelectStarExpressionLineage(SelectStarExpression selectStarExpression, TableAlias targetTable, TableLineage lineage){
        var starColumns = SqlUtils.ReadTableColumnsData(targetTable);
        foreach( var column in starColumns)
        {
            column.tableAlias = targetTable;
        }
        lineage.transformColumns.AddRange(starColumns);
        return lineage;
    }
    public static TableLineage GetSelectSetVariableLineage(SelectSetVariable selectSetVariable, TableLineage lineage){
        return lineage;
    }
    public static TableLineage GetFromClauseLineage(FromClause fromClause, TableLineage lineage){
        if(fromClause.TableReferences == null) return lineage;
        foreach(var tableReference in fromClause.TableReferences){
            GetTableReferenceLineage(tableReference, lineage);
        }
        return lineage;
    }
    public static TableLineage GetNamedTableReferenceLineage(NamedTableReference namedTableReference, TableLineage lineage){
        var schemaObject = namedTableReference.SchemaObject;
        var alias = namedTableReference.Alias?.Value;
        var table = schemaObject.BaseIdentifier.Value;
        var schema = schemaObject.SchemaIdentifier?.Value;
        var database = schemaObject.DatabaseIdentifier?.Value;
        UpstreamType referenceType;
        if(database == null && schema == null && table != null)
        {
            //scan upstream, if match on table then return the CTE
            var foundCTE = GetAnyUpstream(alias: table, lineage.ctes);
            if(foundCTE != null && foundCTE.type == UpstreamType.QUERY)
            {
                foundCTE.alias = alias;
                lineage.upstream.Add(foundCTE);
                return lineage;
            }
            else
            {
                var foundUpstream = GetAnyUpstream(alias: table, lineage.upstream);
                if(foundUpstream != null && foundUpstream.type == UpstreamType.QUERY)
                {
                    //upstream is a CTE.
                    lineage.upstream.Add(foundUpstream);
                    return lineage;
                }
            }
        }
        var tableAlias = GetSchemaObjectTable(namedTableReference.SchemaObject, lineage, alias);
        AddUpstreamReferenceTableIfNotExists(tableAlias, lineage);
        return lineage;
    }

    public static TableLineage GetJoinParenthesisTableReferenceLineage(JoinParenthesisTableReference joinParenthesisTableReference, TableLineage lineage){
        var tableReferenceLineage = joinParenthesisTableReference.Join;
        return lineage;
    }

    public static TableLineage GetTableReferenceLineage(TableReference tableReference, TableLineage lineage){
        switch (tableReference)
        {
            case NamedTableReference namedTableReference:
                GetNamedTableReferenceLineage(namedTableReference, lineage);
                break;
            case JoinParenthesisTableReference joinParenthesisTableReference:
                GetJoinParenthesisTableReferenceLineage(joinParenthesisTableReference, lineage);
                break;
            case QualifiedJoin qualifiedJoin:
                GetQualifiedJoinLineage(qualifiedJoin, lineage);
                break;
            case QueryDerivedTable queryDerivedTable: //SubQuery
                var derivedLineage = new TableLineage();
                GetQueryDerivedTableLineage(queryDerivedTable, derivedLineage);
                var derivedQueryAlias = queryDerivedTable.Alias.Value;
                var upstreamRef = new UpstreamReference(UpstreamType.QUERY, alias: derivedQueryAlias, query: derivedLineage);
                lineage.upstream.Add(upstreamRef);
                break;
            case TableReferenceWithAlias tableReferenceWithAlias:
                GetTableReferenceWithAliasLineage(tableReferenceWithAlias, lineage);
                break;
            case UnqualifiedJoin unqualifiedJoin:
                GetUnqualifiedJoinLineage(unqualifiedJoin, lineage);
                break;
            default:
                throw new NotSupportedException($"TableReference type not supported: {tableReference.GetType().Name}");
        }
        return lineage;
    }
    public static TableLineage GetUnqualifiedJoinLineage(UnqualifiedJoin unqualifiedJoin, TableLineage lineage){
        var firstTableReferenceLineage = GetTableReferenceLineage(unqualifiedJoin.FirstTableReference, lineage);
        var secondTableReferenceLineage = GetTableReferenceLineage(unqualifiedJoin.SecondTableReference, lineage);
        return lineage;
    }
    public static TableLineage GetQualifiedJoinLineage(QualifiedJoin qualifiedJoin, TableLineage lineage){
        var firstTableReferenceLineage = GetTableReferenceLineage(qualifiedJoin.FirstTableReference, lineage);
        var secondTableReferenceLineage = GetTableReferenceLineage(qualifiedJoin.SecondTableReference, lineage);
        return lineage;
    }
    public static TableLineage GetQueryDerivedTableLineage(QueryDerivedTable queryDerivedTable, TableLineage lineage){
        var queryExpressionLineage = GetQueryExpressionLineage(queryDerivedTable.QueryExpression, lineage);
        var originalUpstreamCount = queryExpressionLineage.upstream.Count;
        for(int i = 0; i < originalUpstreamCount; i++){
            if (queryExpressionLineage.upstream[i].type == UpstreamType.TABLE)
            {
                AddUpstreamReferenceTableIfNotExists(queryExpressionLineage.upstream[i].table, lineage);
            }
        }
        return lineage;
    }
    public static UpstreamReference GetTableUpstream(string alias, TableLineage lineage)
    {
        if (alias != null) alias = alias.ToLower();
        UpstreamReference foundUpstream = lineage.upstream.FirstOrDefault(t => t.alias == alias && t.type == UpstreamType.TABLE);
        return foundUpstream;
    }
    public static UpstreamReference GetQueryUpstream(string alias, TableLineage lineage)
    {
        UpstreamReference foundUpstream = lineage.upstream.FirstOrDefault(t => t.alias == alias && t.type == UpstreamType.QUERY);
        return foundUpstream;
    }
    public static UpstreamReference GetAnyUpstream(string alias, List<UpstreamReference> upstreamList)
    {
        alias = alias.ToLower();
        // Look for alias
        foreach (var upstream in upstreamList)
        {
            if(upstream.alias != null && upstream.alias == alias)
            {
                return upstream;
            }
            if (upstream.CTEAlias != null && upstream.CTEAlias == alias)
            {
                return upstream;
            }
        }
        // Look for actual table names
        foreach (var upstream in upstreamList.FindAll(x => x.type == UpstreamType.TABLE))
        {
            if(upstream.table.tableName == alias)
            {
                return upstream;
            }
        }
        return null;
    }
    public static void AddUpstreamReferenceTableIfNotExists(TableAlias tableAlias, TableLineage lineage){
        UpstreamReference foundUpstream;
        if(tableAlias.alias == null)
        {
            foundUpstream = SearchUpstreamTableWithoutAlias(lineage, tableAlias.tableName, tableAlias.schemaName, tableAlias.databaseName);
            if (foundUpstream == null)
            {
                var alias = tableAlias.alias?.ToLower();
                var tableName = tableAlias.tableName.ToLower();
                var schemaName = tableAlias.schemaName?.ToLower();
                var databaseName = tableAlias.databaseName?.ToLower();
                var newTable = new TableAlias(tableName, alias: alias, schemaName: schemaName, databaseName: databaseName);
                var newUpstream = new UpstreamReference(UpstreamType.TABLE, alias: alias, table: newTable);
                lineage.upstream.Add(newUpstream);
            }
        }
        else
        {
            foundUpstream = GetTableUpstream(tableAlias.alias, lineage);
            if(foundUpstream == null){
                var alias = tableAlias.alias?.ToLower();
                var tableName = tableAlias.tableName.ToLower();
                var schemaName = tableAlias.schemaName?.ToLower();
                var databaseName = tableAlias.databaseName?.ToLower();
                var newTable = new TableAlias(tableName, alias: alias, schemaName: schemaName, databaseName: databaseName);
                var newUpstream = new UpstreamReference(UpstreamType.TABLE, alias: alias, table: newTable);
                lineage.upstream.Add(newUpstream);
            }
        }
    }

    public static TableLineage GetTableReferenceWithAliasLineage(TableReferenceWithAlias tableReferenceWithAlias, TableLineage lineage){
        var alias = tableReferenceWithAlias.Alias?.Value;
        switch (tableReferenceWithAlias)
        {
            case SchemaObjectFunctionTableReference softr:
                break;
            case OpenQueryTableReference oqtr:
                //TODO: LinkedServer queries

                //var subquery = oqtr.Query.Value;
                //subquery = subquery.Replace("\r", " ").Replace("\n", " ").Replace("\t", " ");
                //var result = StoredProcedureParserProgram.GetStatementTargets(subquery, defaultSchema, defaultDatabase, true);
                //var relevantSelect = result.ProcedureEvents.First(e => e.Type == ProcedureStatementType.SELECT);
                //AddUpstreamReferenceTableIfNotExists
                break;
            case VariableTableReference vtr:
                break;
            case InlineDerivedTable idt:
                break;
            case OpenJsonTableReference ojtr:
                //TODO:
                //Column turns into upstream table
                var schemaItems = ojtr.SchemaDeclarationItems;
                foreach(var col in schemaItems)
                {
                    var colMapping = col.Mapping;
                    var colAlias = col.ColumnDefinition.ColumnIdentifier.Value;
                    if(colMapping is StringLiteral)
                    {
                        var colPath = (colMapping as StringLiteral).Value;
                    }
                }
                break;
            case OpenXmlTableReference oxtr:
                //TODO:
                break;
            default:
                throw new NotImplementedException($"TableReferenceWithAlias type not supported: {tableReferenceWithAlias.GetType().Name}");
        }
        //var tableAlias = GetSchemaObjectTable(tableReferenceWithAlias.SchemaObject, lineage, alias);
        //AddUpstreamReferenceTableIfNotExists(tableAlias, lineage);
        return lineage;
    }

    public static TableLineage GetInsertSource(InsertSource source, TableLineage lineage)
    {
        switch (source)
        {
            case SelectInsertSource selectInsertSource:
                return ProcParserUtils.GetQueryExpressionLineage(selectInsertSource.Select, lineage);
            case ValuesInsertSource valuesInsertSource:
                // Value rows unhandled
                return lineage;
            //return ProcParserUtils.GetQueryExpressionLineage(valuesInsertSource.Select, lineage);
            case ExecuteInsertSource eis:
                return lineage;
            default:
                throw new NotSupportedException($"InsertSource type not supported: {source.GetType().Name}");
        }
    }
    public static List<Column> GetColumnOrigins(List<Column> sourceColumns)
    {
        var result = new List<Column>();
        foreach (var col in sourceColumns)
        {
            if(col.name != null)
            {
                if (col.GetSourceColumns().Any())
                {
                    result.AddRange(GetColumnOrigins(col.GetSourceColumns()));
                    col.SetSourceColumns(new List<Column>());
                }
                if(col.tableAlias != null)
                {
                    result.Add(col);
                }
            }
        }
        return result;
    }
    public static List<ProcedureStatement> CompressLineage(List<ProcedureStatement> events)
    {
        foreach (var statement in events)
        {
            switch (statement.Type)
            {
                case ProcedureStatementType.SELECT:
                case ProcedureStatementType.SELECT_INTO:
                case ProcedureStatementType.INSERT:
                case ProcedureStatementType.UPDATE:
                    foreach (var col in statement.GetColumns())
                    {
                        var origins = GetColumnOrigins(col.GetSourceColumns());
                        col.SetSourceColumns(origins);
                    }
                    break;
                default:
                    break;
            }
        }
        return events;
    }
}