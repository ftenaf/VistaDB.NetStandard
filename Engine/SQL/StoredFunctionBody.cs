using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class StoredFunctionBody : StoredProcedureBody
  {
    internal StoredFunctionBody(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override IQueryResult OnExecuteQuery()
    {
      foreach (Statement statement in (List<Statement>) this.statements)
      {
        if (statement.ExecuteQuery() != null)
          throw new Exception("Select statements included within a function cannot return data to a client.");
      }
      return (IQueryResult) null;
    }

    public override CreateTableStatement DoGetTemporaryTableName(string paramName)
    {
      if (this.parent != null)
      {
        IParameter returnParameter = this.parent.DoGetReturnParameter();
        if (returnParameter != null && returnParameter.DataType == VistaDBType.Unknown && returnParameter.Direction == ParameterDirection.ReturnValue)
          this.DoRegisterTemporaryTableName(paramName, returnParameter.Value as CreateTableStatement);
        this.parent.DoSetReturnParameter((IParameter) null);
      }
      if (this.tempTables.ContainsKey(paramName))
        return this.tempTables[paramName];
      return (CreateTableStatement) null;
    }

    public override void Dispose()
    {
      base.Dispose();
      GC.SuppressFinalize((object) this);
    }
  }
}
