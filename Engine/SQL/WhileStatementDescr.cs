using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class WhileStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new WhileStatement(conn, parent, parser, id);
    }
  }
}
