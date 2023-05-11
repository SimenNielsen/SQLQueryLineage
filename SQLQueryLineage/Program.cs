using SQLQueryLineage.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog.Extensions.Logging;
using SQLQueryLineage.cli;

namespace SQLQueryLineage;
public static class SQLQueryLineageProgram
{
    private static async Task<int> Main(string[] args)
    {
        var Configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(AppDomain.CurrentDomain.BaseDirectory + "\\appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        Log.Logger = new LoggerConfiguration()
               .ReadFrom.Configuration(Configuration)
               .Enrich.FromLogContext()
               .CreateLogger();

        var builder = new HostBuilder()
            .ConfigureServices((hostContext, services) =>
            {
                services.AddLogging(config =>
                {
                    config.ClearProviders();
                    config.AddProvider(new SerilogLoggerProvider(Log.Logger));
                });
            });

        try
        {
            return await builder.RunCommandLineApplicationAsync<SqlLineageCmd>(args);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return 1;
        }
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