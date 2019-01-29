﻿using VistaDB.DDA;
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
      IUserDefinedFunctionCollection definedFunctions = this.Database.GetUserDefinedFunctions();
      IVistaDBClrProcedureCollection clrProcedures = this.Database.GetClrProcedures();
      foreach (string tableName in this.tableNames)
      {
        if (definedFunctions.ContainsKey(tableName))
        {
          this.Database.DeleteUserDefinedFunctionObject(tableName);
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
