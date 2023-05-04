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
      foreach (Statement statement in (List<Statement>) statements)
      {
        if (statement.ExecuteQuery() != null)
          throw new Exception("Select statements included within a function cannot return data to a client.");
      }
      return null;
    }

    public override CreateTableStatement DoGetTemporaryTableName(string paramName)
    {
      if (parent != null)
      {
        IParameter returnParameter = parent.DoGetReturnParameter();
        if (returnParameter != null && returnParameter.DataType == VistaDBType.Unknown && returnParameter.Direction == ParameterDirection.ReturnValue)
          DoRegisterTemporaryTableName(paramName, returnParameter.Value as CreateTableStatement);
        parent.DoSetReturnParameter(null);
      }
      if (tempTables.ContainsKey(paramName))
        return tempTables[paramName];
      return null;
    }

    public override void Dispose()
    {
      base.Dispose();
      GC.SuppressFinalize(this);
    }
  }
}
