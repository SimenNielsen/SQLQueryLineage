using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace SQLQueryLineageCLI.Commands
{
    [Command(Name = "sqllineage", OptionsComparison = System.StringComparison.InvariantCultureIgnoreCase)]
    [VersionOptionFromMember("--version", MemberName = nameof(GetVersion))]
    [Subcommand(
        typeof(ParseCmd)
    )]
    class SqlLineageCmd : BaseCmd
    {
        public SqlLineageCmd(ILogger<ParseCmd> logger, IConsole console)
        {
            _logger = logger;
            _console = console;
        }

        protected override Task<int> OnExecute(CommandLineApplication app)
        {
            app.ShowHelp();
            return Task.FromResult(0);
        }
        private static string GetVersion()
            => typeof(SqlLineageCmd).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

    }
}
