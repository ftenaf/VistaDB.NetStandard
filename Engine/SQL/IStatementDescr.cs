using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal interface IStatementDescr
  {
    Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id);
  }
}
