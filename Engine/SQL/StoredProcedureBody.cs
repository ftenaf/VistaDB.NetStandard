using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class StoredProcedureBody : BatchStatement
  {
    internal override BatchStatement Batch
    {
      get
      {
        return null;
      }
    }

    internal StoredProcedureBody(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
      ReturnParamCascade = false;
    }

    public override IParameter DoGetParam(string paramName)
    {
      if (!prms.ContainsKey(paramName))
        return null;
      return prms[paramName];
    }

    public override WhileStatement DoGetCycleStatement()
    {
      return null;
    }
  }
}
