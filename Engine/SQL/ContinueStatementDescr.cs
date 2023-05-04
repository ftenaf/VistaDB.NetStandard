using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class ContinueStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new ContinueStatement(conn, parent, parser, id);
    }
  }
}
