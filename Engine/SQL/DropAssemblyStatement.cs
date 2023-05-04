using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropAssemblyStatement : DropTableStatement
  {
    public DropAssemblyStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      foreach (string tableName in tableNames)
        Database.DropAssembly(tableName, false);
      return null;
    }
  }
}
