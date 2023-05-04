using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropTriggerStatement : DropTableStatement
  {
    internal DropTriggerStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      foreach (string tableName in tableNames)
        Database.UnregisterClrTrigger(tableName);
      return null;
    }
  }
}
