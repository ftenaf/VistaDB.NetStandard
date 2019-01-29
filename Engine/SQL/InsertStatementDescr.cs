using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class InsertStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new InsertStatement(conn, parent, parser, id);
    }
  }
}
