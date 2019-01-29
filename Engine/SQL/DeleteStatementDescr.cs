using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DeleteStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new DeleteStatement(conn, parent, parser, id);
    }
  }
}
