using SQLQueryLineage;

namespace SQLQueryLineageTesting.SQLQueryLineageTests
{
    [TestClass]
    public class SelectTests
    {
        public SelectTests() { }
        [TestMethod]
        public void AssertSimpleSelectColumnName()
        {
            string query = "SELECT " +
                "col1 " +
                "FROM Students";
            SQLQueryLineageVisitor result = SQLQueryLineageProgram.GetStatementTargets(query);
            string expected = "col1";
            string compare = result.ProcedureEvents[0].GetColumns()[0].name;
            Assert.AreEqual(expected, compare);
        }
    }
}
