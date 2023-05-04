using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CoalesceFunction : MaxOfFunction
  {
    public CoalesceFunction(SQLParser parser)
      : base(parser)
    {
      skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      base.ParseParameters(parser);
      if (ParamCount < 2)
        throw new VistaDBSQLException(501, text, LineNo, SymbolNo);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      isAllowNull = parameters[ParamCount - 1].IsAllowNull;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      int index = 0;
      for (int paramCount = ParamCount; index < paramCount; ++index)
      {
        if (!paramValues[index].IsNull)
          return ((IValue) paramValues[index]).Value;
      }
      return (object) null;
    }
  }
}
