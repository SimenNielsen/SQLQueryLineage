using SQLQueryLineage.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;

namespace SQLQueryLineage;
public static class SQLQueryLineageProgram
{
    static void Main(string[] args)
    {
        //file path is first argument
        string path = args[0];
        // default schema is second argument
        string defaultSchema = args[1];
        // default database is third argument
        string defaultDatabase = args[2];
        string outputFilePath = args[3];
        //read file contents
        string query = File.ReadAllText(path);
        var parseResult = GetStatementTargets(query, defaultSchema, defaultDatabase);
        File.WriteAllText(outputFilePath, JsonConvert.SerializeObject(parseResult));
    }

    public static SQLQueryLineageVisitor GetStatementTargets(string storedProcedureDefinition, string defaultSchema = "dbo", string defaultDatabase = "master", bool isLinkedServer = false)
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