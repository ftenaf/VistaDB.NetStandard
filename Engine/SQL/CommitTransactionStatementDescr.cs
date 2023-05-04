using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CommitTransactionStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new CommitTransactionStatement(conn, parent, parser, id);
    }
  }
}
