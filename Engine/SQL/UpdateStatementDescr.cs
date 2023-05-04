using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class UpdateStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return new UpdateStatement(conn, parent, parser, id);
    }
  }
}
