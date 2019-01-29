using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterProcedureStatement : CreateProcedureStatement
  {
    internal AlterProcedureStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      bool flag1 = false;
      bool flag2 = this.Database.NestedTransactionLevel == 0;
      IStoredProcedureInformation sp = (IStoredProcedureInformation) null;
      if (this.Database.GetStoredProcedures()[this.name] == null)
        throw new VistaDBSQLException(607, this.name, this.lineNo, this.symbolNo);
      try
      {
        if (flag2)
          this.Database.BeginTransaction();
        else
          sp = this.Database.GetStoredProcedures()[this.name];
        this.Database.DeleteStoredProcedureObject(this.name);
        this.Database.CreateStoredProcedureObject(this.storedProcedure);
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
