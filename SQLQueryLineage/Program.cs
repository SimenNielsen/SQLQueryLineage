using SQLQueryLineage.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SQLQueryLineage;
public static class SQLQueryLineageProgram
{
    public static SQLQueryLineageVisitor GetStatementTargets(string storedProcedureDefinition, 
        string defaultSchema = "dbo", 
        string defaultDatabase = "master", 
        bool isLinkedServer = false)
    {
        ProcParserUtils.defaultDatabase = defaultDatabase;
        ProcParserUtils.defaultSchema = defaultSchema;
        if (isLinkedServer == true)
        {
            storedProcedureDefinition = storedProcedureDefinition.ToLower().Replace("$system", "system");
        }
        StringReader reader = new StringReader(storedProcedureDefinition);

        //specify parser for appropriate SQL version
        var parser = new TSql130Parser(true, SqlEngineType.Standalone);

        IList<ParseError> errors;
        TSqlFragment sqlFragment = parser.Parse(reader, out errors);

        if (errors.Count > 0)
        {
            throw new Exception("Error parsing stored procedure definition");
        }

        SQLQueryLineageVisitor sqlVisitor = new SQLQueryLineageVisitor();
        sqlFragment.Accept(sqlVisitor);

        return sqlVisitor;
    }
}