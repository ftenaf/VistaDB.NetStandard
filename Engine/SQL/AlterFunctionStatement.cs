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
      bool flag2 = this.Database.NestedTransactionLevel == 0;
      IStoredProcedureInformation sp = (IStoredProcedureInformation) null;
      if (this.Database.GetUserDefinedFunctions()[this.functionName] == null)
        throw new VistaDBSQLException(607, this.functionName, this.lineNo, this.symbolNo);
      try
      {
        if (flag2)
          this.Database.BeginTransaction();
        else
          sp = (IStoredProcedureInformation) this.Database.GetUserDefinedFunctions()[this.functionName];
        this.Database.DeleteUserDefinedFunctionObject(this.functionName);
        this.Database.CreateUserDefinedFunctionObject(this.function);
        flag1 = true;
      }
      finally
      {
        if (flag2)
        {
          if (flag1)
            this.Database.CommitTransaction();
          else
            this.Database.RollbackTransaction();
        }
        else if (!flag1)
          this.Database.CreateStoredProcedureObject(sp);
      }
      return (IQueryResult) null;
    }
  }
}
