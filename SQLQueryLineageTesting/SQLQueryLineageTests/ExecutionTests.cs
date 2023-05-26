using SQLQueryLineage;

namespace SQLQueryLineageTesting.SQLQueryLineageTests
{
    [TestClass]
    public class ExecutionTests
    {
        public ExecutionTests() { }
        [TestMethod]
        public void AssertUpdateEventCount()
        {
            string query = "EXEC testbase1.dbo.uspGetEmployeeManagers @BusinessEntityID = 50;";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(1, result.ProcedureEvents.Count);
        }
        [TestMethod]
        public void AssertUpdateEventProcedureName()
        {
            string query = "EXEC testbase1.dbo.uspGetEmployeeManagers @BusinessEntityID = 50;";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("uspgetemployeemanagers", result.ProcedureEvents[0].Target.tableName);
        }
        [TestMethod]
        public void AssertUpdateEventProcedureSchemaName()
        {
            string query = "EXEC testbase1.dbo.uspGetEmployeeManagers @BusinessEntityID = 50;";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("dbo", result.ProcedureEvents[0].Target.schemaName);
        }
        [TestMethod]
        public void AssertUpdateEventProcedureDatabaseName()
        {
            string query = "EXEC testbase1.dbo.uspGetEmployeeManagers @BusinessEntityID = 50;";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("testbase1", result.ProcedureEvents[0].Target.databaseName);
        }
    }
}
