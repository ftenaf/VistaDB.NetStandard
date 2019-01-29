using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class PrintStatmentDesc : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new PrintStatement(conn, parent, parser, id);
    }
  }
}
