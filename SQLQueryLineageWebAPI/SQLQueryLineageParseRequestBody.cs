using SQLQueryLineage.Common;
namespace SQLQueryLineageWebAPI
{
    public class SQLQueryLineageParseRequestBody
    {
        public string query { get; set; }
        public ParserProperties properties { get; set; }
    }
}
