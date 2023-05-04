using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropProcStatement : DropTableStatement
  {
    public DropProcStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      IStoredProcedureCollection storedProcedures = Database.GetStoredProcedures();
      IVistaDBClrProcedureCollection clrProcedures = Database.GetClrProcedures();
      foreach (string tableName in tableNames)
      {
        if (storedProcedures.ContainsKey(tableName))
        {
          Database.DeleteStoredProcedureObject(tableName);
        }
        else
        {
          if (!clrProcedures.ContainsKey(tableName))
            throw new VistaDBSQLException(607, tableName, lineNo, symbolNo);
          Database.UnregisterClrProcedure(tableName);
        }
      }
      return null;
    }
  }
}
