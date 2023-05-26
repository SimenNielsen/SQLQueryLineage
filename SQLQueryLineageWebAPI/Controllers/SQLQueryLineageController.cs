using Microsoft.AspNetCore.Mvc;
using SQLQueryLineage;
using SQLQueryLineage.Common;
using Newtonsoft.Json;

namespace SQLQueryLineageWebAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class SQLQueryLineageController : ControllerBase
    {

        private readonly ILogger<SQLQueryLineageController> _logger;

        public SQLQueryLineageController(ILogger<SQLQueryLineageController> logger)
        {
            _logger = logger;
        }

        [HttpPost(Name = "SQLQueryLineage")]
        public IEnumerable<ProcedureStatement> Post([FromBody] SQLQueryLineageParseRequestBody body)
        {
            var query = body.query;
            var properties = body.properties;
            SQLQueryLineageVisitor parseResult = SQLQueryLineageProgram.GetStatementTargets(query, properties);
            _logger.LogInformation(JsonConvert.SerializeObject(parseResult));
            return parseResult.ProcedureEvents.ToArray().AsEnumerable();
        }
    }
}