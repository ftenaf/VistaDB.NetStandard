using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class SelectStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new SelectStatement(conn, parent, parser, id);
    }
  }
}
