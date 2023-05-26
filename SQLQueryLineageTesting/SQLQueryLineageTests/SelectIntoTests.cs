using SQLQueryLineage;
using SQLQueryLineage.Common;

namespace SQLQueryLineageTesting.SQLQueryLineageTests
{
    [TestClass]
    public class SelectIntoTests
    {
        public SelectIntoTests() { }
        [TestMethod]
        public void AssertNonEmptyOnSelectInto()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.IsTrue(result.ProcedureEvents.Count == 1);
        }
        [TestMethod]
        public void AssertColumnName()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "col1";
            string compare = result.ProcedureEvents[0].GetColumns()[0].name;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnNameWithAlias()
        {
            string query = "SELECT " +
                "col1 as c1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "c1";
            string compare = result.ProcedureEvents[0].GetColumns()[0].alias;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnNameFromSubquery()
        {
            string query = "SELECT " +
                "sq.col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "(SELECT col1 FROM Students) as sq";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "col1";
            string compare = result.ProcedureEvents[0].GetColumns()[0].name;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnsLength()
        {
            string query = "SELECT " +
                "col1 " +
                ",col2 " +
                ",col3 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            int expected = 3;
            int compare = result.ProcedureEvents[0].GetColumns().Count();
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertSourceTable()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "students";
            string compare = result.ProcedureEvents[0].GetColumns()[0].tableAlias.tableName;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnSourceDefaultSchema()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            ParserProperties prop = new ParserProperties() { defaultSchema = "dbo" };
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query, prop);
            string expected = "dbo";
            string compare = result.ProcedureEvents[0].GetColumns()[0].tableAlias.schemaName;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnSourceSchema()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "testschema.Students";
            ParserProperties prop = new ParserProperties() { defaultSchema = "dbo" };
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query, prop);
            string expected = "testschema";
            string compare = result.ProcedureEvents[0].GetColumns()[0].tableAlias.schemaName;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnSourceDefaultDatebase()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "master";
            string compare = result.ProcedureEvents[0].GetColumns()[0].tableAlias.databaseName;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnSourceDatebase()
        {
            string query = "SELECT " +
                "col1 " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "testdb..Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "testdb";
            string compare = result.ProcedureEvents[0].GetColumns()[0].tableAlias.databaseName;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnAggregateLogicColumnNameWithoutAlias()
        {
            string query = "SELECT " +
                "MAX(col1) " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "max(col1)";
            string compare = result.ProcedureEvents[0].GetColumns()[0].name;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertColumnAggregateLogicColumnNameWithAlias()
        {
            string query = "SELECT " +
                "MAX(col1) as col1Max " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "col1max";
            string compare = result.ProcedureEvents[0].GetColumns()[0].alias;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertSubQueryColumnSource()
        {
            string query = "SELECT " +
                "a.testcol as tc " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "(" +
                "SELECT source_col as testcol " +
                "FROM SourceTable" +
                ") as a";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "source_col";
            string compare = result.ProcedureEvents[0].GetColumns()[0].GetSourceColumns()[0].name;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertSubQueryColumnSourceNested()
        {
            string query = "SELECT " +
                "a.testcol as tc " +
                "INTO " +
                "Table1 " +
                "FROM " +
                "(" +
                "SELECT g.source_col as testcol " +
                "FROM (" +
                "SELECT base_col as source_col " +
                "FROM Students " +
                ") as g " +
                ") as a";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "base_col";
            string compare = result.ProcedureEvents[0].GetColumns()[0].GetSourceColumns()[0].GetSourceColumns()[0].name;
            Assert.AreEqual(expected, compare);
        }
        [TestMethod]
        public void AssertUnionAllSourceColumns()
        {
            string query = "SELECT x.col1 " +
                "INTO [NEW_TABLE] " +
                "FROM (SELECT col1 FROM TABLE1 " +
                "UNION " +
                "SELECT col1 FROM TABLE2) x";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(1, result.ProcedureEvents[0].GetColumns().Count);
            Assert.AreEqual("col1", result.ProcedureEvents[0].GetColumns().First().name);
            Assert.AreEqual("table2", result.ProcedureEvents[0].GetColumns().First().GetSourceColumns().First().GetSourceColumns().Last().tableAlias.tableName);
        }
        [TestMethod]
        public void AssertStoredProcedureSample1()
        {
            string path = @"Scripts/sp_sample1.sql";
            string query = File.ReadAllText(path);
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("humanresources", result.ProcedureEvents[0].GetTarget().schemaName);
            Assert.AreEqual("employee_jointables", result.ProcedureEvents[0].GetTarget().tableName);
            Assert.AreEqual(18, result.ProcedureEvents[0].GetColumns().Count);
        }
        [TestMethod]
        public void AssertStoredProcedureSample2()
        {
            string path = @"Scripts/sp_sample2.sql";
            string query = File.ReadAllText(path);
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("#collectionstartdates", result.ProcedureEvents[0].GetTarget().tableName);
            Assert.AreEqual("collectionstartdate", result.ProcedureEvents[0].GetColumns().Last().alias);
            Assert.AreEqual(2, result.ProcedureEvents[0].GetColumns().Count);
        }
        [TestMethod]
        public void AssertCTETest1()
        {
            string path = @"Scripts/CTE_test_1.sql";
            string query = File.ReadAllText(path);
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("#temp", result.ProcedureEvents[0].GetTarget().tableName);
            Assert.AreEqual("students", result.ProcedureEvents[0].GetColumns().First().GetSourceColumns().First().GetSourceColumns().First().tableAlias.tableName);
            Assert.AreEqual(2, result.ProcedureEvents[0].GetColumns().Count);
        }
    }
}
