using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog.Extensions.Logging;
using SQLQueryLineageCLI.Commands;

public static class SQLQueryLineageCLIProgram
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
}