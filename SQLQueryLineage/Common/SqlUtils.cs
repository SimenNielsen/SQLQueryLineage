﻿using System.Data.SqlClient;
using SQLQueryLineage;
namespace SQLQueryLineage.Common
{
    public static class SqlUtils
    {
        private static Dictionary<string, List<Column>> cache = new Dictionary<string, List<Column>>();
        public static List<Column> ReadTableColumnsData(TableAlias targetTable)
        {
            if(cache.ContainsKey(targetTable.tableName))
            {
                return cache[targetTable.tableName];
            }
            if(
                string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SOURCE_HOST")) ||
                string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SOURCE_PORT")) || 
                string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SOURCE_USER")) ||
                string.IsNullOrEmpty(Environment.GetEnvironmentVariable("SOURCE_PASS"))
            )
            {
                if (SQLQueryLineageProgram.properties.failSilent == true)
                {
                    return new List<Column>();
                }
                else
                {
                    throw new Exception("Environment variables not set. For accurate lineage please provide SOURCE_HOST, SOURCE_PORT, SOURCE_USER and SOURCE_PASS.");
                }
            }
            var result = new List<Column>();
            string queryString = $"select col.name as column_name \r\nfrom sys.tables as tab \r\nleft join sys.columns as col on tab.object_id = col.object_id\r\nwhere tab.name = '{targetTable.tableName}'\r\nunion all\r\nselect col.name as column_name \r\nfrom sys.views as v \r\nleft join sys.columns as col on v.object_id = col.object_id \r\nwhere v.name = '{targetTable.tableName}'";
            using (SqlConnection connection = new SqlConnection(
                       $"Server={Environment.GetEnvironmentVariable("SOURCE_HOST")},{Environment.GetEnvironmentVariable("SOURCE_PORT")};Database={targetTable.databaseName};User Id={Environment.GetEnvironmentVariable("SOURCE_USER")};Password={Environment.GetEnvironmentVariable("SOURCE_PASS")};"))
            {
                SqlCommand command = new SqlCommand(
                    queryString, connection);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var colName = reader.GetString(0);
                            result.Add(new Column(colName));
                        }
                    }
                }
            }
            cache.Add(targetTable.tableName, result);
            return result;
        }
    }
}
