using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterFunctionStatement : CreateFunctionStatement
  {
    internal AlterFunctionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      bool flag1 = false;
      bool flag2 = Database.NestedTransactionLevel == 0;
      IStoredProcedureInformation sp = (IStoredProcedureInformation) null;
      if (Database.GetUserDefinedFunctions()[functionName] == null)
        throw new VistaDBSQLException(607, functionName, lineNo, symbolNo);
      try
      {
        if (flag2)
          Database.BeginTransaction();
        else
          sp = (IStoredProcedureInformation) Database.GetUserDefinedFunctions()[functionName];
        Database.DeleteUserDefinedFunctionObject(functionName);
        Database.CreateUserDefinedFunctionObject(function);
        flag1 = true;
      }
      finally
      {
        if (flag2)
        {
          if (flag1)
            Database.CommitTransaction();
          else
            Database.RollbackTransaction();
        }
        else if (!flag1)
          Database.CreateStoredProcedureObject(sp);
      }
      return (IQueryResult) null;
    }
  }
}
