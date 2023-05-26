using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SQLQueryLineage;
using SQLQueryLineage.Common;

namespace SQLQueryLineageCLI.Commands
{
    [Command(Name = "parse", Description = "Parse sql file and output to json file")]
    class ParseCmd : BaseCmd
    {
        [Option(CommandOptionType.SingleValue, ShortName = "f", LongName = "filepath", Description = "path to sql file to parse", ValueName = "sql file path", ShowInHelpText = true)]
        public string FilePath { get; set; }
        [Option(CommandOptionType.SingleValue, ShortName = "o", LongName = "output-filepath", Description = "path to save output json content", ValueName = "output filepath", ShowInHelpText = true)]
        public string OutputFilePath { get; set; }
        [Option(CommandOptionType.SingleValue, ShortName = "d", LongName = "database", Description = "default database", ValueName = "default database", ShowInHelpText = true)]
        public string Database { get; set; } = "master";
        [Option(CommandOptionType.SingleValue, ShortName = "s", LongName = "schema", Description = "default schema", ValueName = "default schema", ShowInHelpText = true)]
        public string Schema { get; set; } = "dbo";
        [Option(CommandOptionType.NoValue, ShortName = "c", LongName = "compress", Description = "compress the lineage", ValueName = "compress lineage", ShowInHelpText = true)]
        public bool Compress { get; set; } = false;
        [Option(CommandOptionType.NoValue, ShortName = "l", LongName = "linked-server", Description = "specify if query is towards linked server", ValueName = "is linked server", ShowInHelpText = true)]
        public bool isLinkedServer { get; set; } = false;

        public ParseCmd(ILogger<ParseCmd> logger, IConsole console)
        {
            _logger = logger;
            _console = console;
        }

        private void ValidateOptions()
        {
            if (string.IsNullOrEmpty(FilePath))
            {
                throw new Exception("filepath is required");
            }
        }
        protected override Task<int> OnExecute(CommandLineApplication app)
        {
            try
            {
                ValidateOptions();
                ParserProperties properties = new ParserProperties()
                {
                    defaultDatabase = Database,
                    defaultSchema = Schema,
                    compress = Compress,
                    isLinkedServer = isLinkedServer
                };
                string query = File.ReadAllText(FilePath);
                var parseResult = SQLQueryLineageProgram.GetStatementTargets(query, properties);
                if(Compress == true)
                {
                    ProcParserUtils.CompressLineage(parseResult.ProcedureEvents);
                }
                File.WriteAllText(OutputFilePath, JsonConvert.SerializeObject(parseResult));
                return Task.FromResult(0);
            }
            catch (Exception ex)
            {
                OnException(ex);
                return Task.FromResult(1);
            }
        }
    }
}
