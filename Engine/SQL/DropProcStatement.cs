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
      IStoredProcedureCollection storedProcedures = this.Database.GetStoredProcedures();
      IVistaDBClrProcedureCollection clrProcedures = this.Database.GetClrProcedures();
      foreach (string tableName in this.tableNames)
      {
        if (storedProcedures.ContainsKey(tableName))
        {
          this.Database.DeleteStoredProcedureObject(tableName);
        }
        else
        {
          if (!clrProcedures.ContainsKey(tableName))
            throw new VistaDBSQLException(607, tableName, this.lineNo, this.symbolNo);
          this.Database.UnregisterClrProcedure(tableName);
        }
      }
      return (IQueryResult) null;
    }
  }
}
