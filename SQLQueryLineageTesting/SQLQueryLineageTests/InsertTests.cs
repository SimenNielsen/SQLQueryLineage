using SQLQueryLineage;
using SQLQueryLineage.Common;

namespace SQLQueryLineageTesting.SQLQueryLineageTests
{
    [TestClass]
    public class InsertTests
    {
        public InsertTests() { }
        [TestMethod]
        public void AssertInsertEventCount()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(1, result.ProcedureEvents.Count);
        }
        [TestMethod]
        public void AssertInsertEventType()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(ProcedureStatementType.INSERT, result.ProcedureEvents[0].Type);
        }
        [TestMethod]
        public void AssertInsertColumnsCount()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(3, result.ProcedureEvents[0].GetColumns().Count);
        }
        [TestMethod]
        public void AssertInsertColumnName()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, Col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("col3", result.ProcedureEvents[0].GetColumns()[2].name);
        }
        [TestMethod]
        public void AssertInsertColumnSourceCount()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, Col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(1, result.ProcedureEvents[0].GetColumns()[2].GetSourceColumns().Count);
        }
        [TestMethod]
        public void AssertInsertColumnSourceColumnName()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, Col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("scol2", result.ProcedureEvents[0].GetColumns()[1].GetSourceColumns()[0].name);
        }
        [TestMethod]
        public void AssertInsertColumnSourceColumnTable()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, Col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("source_table", result.ProcedureEvents[0].GetColumns()[1].GetSourceColumns()[0].tableAlias.tableName);
        }
        [TestMethod]
        public void AssertInsertTargetTableName()
        {
            string query = "INSERT INTO insertTable1 (col1, col2, Col3) SELECT scol1, scol2, scol3 FROM source_table";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("inserttable1", result.ProcedureEvents[0].GetTarget().tableName);
        }
        [TestMethod]
        public void AssertInsertTargetTableNameByAlias()
        {
            string query = "INSERT INTO it1 (col1, col2, Col3) SELECT scol1, scol2, scol3 FROM insertTable1 as it1";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("inserttable1", result.ProcedureEvents[0].GetTarget().tableName);
        }
    }
}
