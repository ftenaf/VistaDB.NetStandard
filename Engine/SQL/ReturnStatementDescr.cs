using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class ReturnStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new ReturnStatement(conn, parent, parser, id);
    }
  }
}
