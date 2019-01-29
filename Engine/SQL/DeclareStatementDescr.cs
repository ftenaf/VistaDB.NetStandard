using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DeclareStatementDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new DeclareStatement(conn, parent, parser, id);
    }
  }
}
