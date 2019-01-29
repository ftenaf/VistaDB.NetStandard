using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BreakStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new BreakStatement(conn, parent, parser, id);
    }
  }
}
