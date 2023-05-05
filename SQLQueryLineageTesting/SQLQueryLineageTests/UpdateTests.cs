using SQLQueryLineage;
using SQLQueryLineage.Common;

namespace SQLQueryLineageTesting
{
    [TestClass]
    public class UpdateTests
    {
        public UpdateTests() { }
        [TestMethod]
        public void AssertUpdateEventCount()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(1, result.ProcedureEvents.Count);
        }
        [TestMethod]
        public void AssertUpdateEventType()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(ProcedureStatementType.INSERT, result.ProcedureEvents[0].Type);
        }
        [TestMethod]
        public void AssertUpdateColumnsCount()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(2, result.ProcedureEvents[0].GetColumns().Count);
        }
        [TestMethod]
        public void AssertUpdateColumnName()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("personpostcode", result.ProcedureEvents[0].GetColumns()[1].name);
        }
        [TestMethod]
        public void AssertUpdateColumnSourceCount()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual(1, result.ProcedureEvents[0].GetColumns()[1].GetSourceColumns().Count);
        }
        [TestMethod]
        public void AssertUpdateColumnSourceColumnName()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("postcode", result.ProcedureEvents[0].GetColumns()[1].GetSourceColumns()[0].name);
        }
        [TestMethod]
        public void AssertUpdateColumnSourceColumnTable()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("addresslist", result.ProcedureEvents[0].GetColumns()[1].GetSourceColumns()[0].tableAlias.tableName);
        }
        [TestMethod]
        public void AssertUpdateTargetTableName()
        {
            string query = "UPDATE Persons SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("persons", result.ProcedureEvents[0].GetTarget().tableName);
        }
        [TestMethod]
        public void AssertUpdateTargetTableNameByAlias()
        {
            string query = "UPDATE Per SET Per.PersonCityName=Addr.City, Per.PersonPostCode=Addr.PostCode FROM Persons Per INNER JOIN AddressList Addr ON Per.PersonId = Addr.PersonId";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            Assert.AreEqual("persons", result.ProcedureEvents[0].GetTarget().tableName);
        }
    }
}
