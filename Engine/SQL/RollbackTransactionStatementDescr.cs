using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class RollbackTransactionStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new RollbackTransactionStatement(conn, parent, parser, id);
    }
  }
}
