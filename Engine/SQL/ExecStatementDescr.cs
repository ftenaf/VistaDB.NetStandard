using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class ExecStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new ExecStatement(conn, parent, parser, id);
    }
  }
}
