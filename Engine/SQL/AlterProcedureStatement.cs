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
      bool flag2 = Database.NestedTransactionLevel == 0;
      IStoredProcedureInformation sp = (IStoredProcedureInformation) null;
      if (Database.GetStoredProcedures()[name] == null)
        throw new VistaDBSQLException(607, name, lineNo, symbolNo);
      try
      {
        if (flag2)
          Database.BeginTransaction();
        else
          sp = Database.GetStoredProcedures()[name];
        Database.DeleteStoredProcedureObject(name);
        Database.CreateStoredProcedureObject(storedProcedure);
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
