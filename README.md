# SQL Query Lineage
![.NET Workflow](https://github.com/SimenNielsen/SQLQueryLineage/actions/workflows/dotnet.yml/badge.svg)

This project is a .NET console application that parses MS SQL stored procedures and generates column-level lineage in JSON format. The tool extracts information from the stored procedures and outputs a JSON file that contains information about the columns and their dependencies.

## Table of Contents
- [Installation](https://github.com/SimenNielsen/SQLQueryLineage#installation)
- [Usage](https://github.com/SimenNielsen/SQLQueryLineage#usage)
- [Sample Output](https://github.com/SimenNielsen/SQLQueryLineage#sample-output)
- [Contributing](https://github.com/SimenNielsen/SQLQueryLineage#contributing)
- [License](https://github.com/SimenNielsen/SQLQueryLineage#license)

## Installation
Download that latest release from [Releases](https://github.com/SimenNielsen/SQL_Procedure_Parser/releases).

## Usage
Sample usage:
```
sqllineage parse -file="D:\dev\sp_sample2.sql" -schema=testschema -database=testdb -output-file="C:\Users\Simen\Documents\test.json"
```

Will parse the file specified with the -file parameter and output the result to the file specified with the -output-file parameter. The -schema and -database parameters are optional and will default to dbo and master respectively.

The json contains a list of events found in a specified file. Event types are:
* SELECT = 1
* SELECT INTO = 2
* INSERT = 3
* UPDATE = 4
* EXECUTE = 5

Some queries require a connection string to the query database, e.g. when the upstream is unclear when column alias is not mentioned. To allow connection to a specific database we need to set these 4 environment variables:
- SOURCE_HOST
- SOURCE_PORT
- SOURCE_USER
- SOURCE_PASS
<br>
To set environment variables in Windows Powershell: ```$env:SOURCE_HOST="localhost"```.

Example:
```
SQL_Procedure_Parser.exe "./udp_please_parse_me.sql" "dbo" "TestDB" "C:\Temp\Testing\out.json"
```

## Sample Output
The output file generated by the tool will contain a JSON array with information about each column and its dependencies. Given following query, here is the output json content.
### Input
Using the following parameters: ```"C:\Users\User\Documents\SQL\testing\sample_query.sql" "dbo" "TestDB" "C:\Users\User\Documents\SQL\testing\output\out.json"```
sample_query.sql:
```
CREATE PROCEDURE get_order_info @order_id INT
AS
BEGIN
    SELECT o.id AS order_id,
           o.customer_id,
           c.name AS customer_name,
           c.email AS customer_email,
           o.date AS order_date
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.id
    WHERE o.id = @order_id
END
```
### Output
```
{
  "ProcedureEvents": [
    {
      "Type": 1,
      "Columns": [
        {
          "name": "id",
          "alias": "order_id",
          "tableAlias": {
            "alias": "o",
            "tableName": "orders",
            "schemaName": "dbo",
            "databaseName": "testdb"
          },
          "logic": null,
          "sourceColumns": [],
          "upstreamReferenceAlias": "o"
        },
        {
          "name": "customer_id",
          "alias": "customer_id",
          "tableAlias": {
            "alias": "o",
            "tableName": "orders",
            "schemaName": "dbo",
            "databaseName": "testdb"
          },
          "logic": null,
          "sourceColumns": [],
          "upstreamReferenceAlias": "o"
        },
        {
          "name": "name",
          "alias": "customer_name",
          "tableAlias": {
            "alias": "c",
            "tableName": "customers",
            "schemaName": "dbo",
            "databaseName": "testdb"
          },
          "logic": null,
          "sourceColumns": [],
          "upstreamReferenceAlias": "c"
        },
        {
          "name": "email",
          "alias": "customer_email",
          "tableAlias": {
            "alias": "c",
            "tableName": "customers",
            "schemaName": "dbo",
            "databaseName": "testdb"
          },
          "logic": null,
          "sourceColumns": [],
          "upstreamReferenceAlias": "c"
        },
        {
          "name": "date",
          "alias": "order_date",
          "tableAlias": {
            "alias": "o",
            "tableName": "orders",
            "schemaName": "dbo",
            "databaseName": "testdb"
          },
          "logic": null,
          "sourceColumns": [],
          "upstreamReferenceAlias": "o"
        }
      ],
      "Target": {
        "alias": null,
        "tableName": "#order_info",
        "schemaName": "dbo",
        "databaseName": "testdb"
      }
    }
  ]
}
```

## Contributing
Contributions to this project are welcome. If you have any suggestions or would like to report a bug, please open an issue on the GitHub repository.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
