using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class TruncateDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new TruncateTableStatement(conn, parent, parser, id);
    }
  }
}
