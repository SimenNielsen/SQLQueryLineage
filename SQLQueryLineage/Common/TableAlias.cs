namespace SQLQueryLineage.Common;

public class TableAlias
{
    public string alias { get; set; }
    public string tableName { get; set; }
    public string schemaName { get; set; }
    public string databaseName { get; set; }
    public TableAlias(string tableName, string alias = null, string schemaName = null, string databaseName = null)
    {
        schemaName = schemaName ?? ProcParserUtils.defaultSchema;
        schemaName = schemaName == string.Empty ? ProcParserUtils.defaultSchema : schemaName;

        databaseName = databaseName ?? ProcParserUtils.defaultDatabase;
        databaseName = databaseName == string.Empty ? ProcParserUtils.defaultDatabase : databaseName;

        alias = alias == null ? null : alias.ToLower();

        this.tableName = tableName.ToLower();
        this.schemaName = schemaName.ToLower();
        this.databaseName = databaseName.ToLower();
        this.alias = alias;
    }
}
