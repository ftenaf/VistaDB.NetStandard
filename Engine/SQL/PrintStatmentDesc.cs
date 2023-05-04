using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class PrintStatmentDesc : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new PrintStatement(conn, parent, parser, id);
    }
  }
}
