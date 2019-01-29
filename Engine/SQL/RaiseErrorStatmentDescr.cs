using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class RaiseErrorStatmentDescr : IStatementDescr
  {
    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      return (Statement) new RaiseErrorStatement(conn, parent, parser, id);
    }
  }
}
