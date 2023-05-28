using SQLQueryLineage.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Runtime.CompilerServices;

namespace SQLQueryLineage;
public static class SQLQueryLineageProgram
{
    public static ParserProperties properties = null;
    internal static List<ProcedureStatement> remoteVisitEvents = new List<ProcedureStatement>();
    public static SQLQueryLineageVisitor GetStatementTargets(string storedProcedureDefinition,
        ParserProperties properties = null)
    {
        if(properties == null) properties = new ParserProperties(); //default properties
        SQLQueryLineageProgram.properties = properties;
        ProcParserUtils.defaultDatabase = properties.defaultDatabase;
        ProcParserUtils.defaultSchema = properties.defaultSchema;
        if (properties.isLinkedServer == true)
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
        sqlVisitor.ProcedureEvents.AddRange(remoteVisitEvents);
        return sqlVisitor;
    }
}