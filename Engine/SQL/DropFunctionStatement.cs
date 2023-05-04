using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropFunctionStatement : DropTableStatement
  {
    public DropFunctionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      IUserDefinedFunctionCollection definedFunctions = Database.GetUserDefinedFunctions();
      IVistaDBClrProcedureCollection clrProcedures = Database.GetClrProcedures();
      foreach (string tableName in tableNames)
      {
        if (definedFunctions.ContainsKey(tableName))
        {
          Database.DeleteUserDefinedFunctionObject(tableName);
        }
        else
        {
          if (!clrProcedures.ContainsKey(tableName))
            throw new VistaDBSQLException(607, tableName, lineNo, symbolNo);
          Database.UnregisterClrProcedure(tableName);
        }
      }
      return (IQueryResult) null;
    }
  }
}
