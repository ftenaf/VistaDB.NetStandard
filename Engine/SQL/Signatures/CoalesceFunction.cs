using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CoalesceFunction : MaxOfFunction
  {
    public CoalesceFunction(SQLParser parser)
      : base(parser)
    {
      this.skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      base.ParseParameters(parser);
      if (this.ParamCount < 2)
        throw new VistaDBSQLException(501, this.text, this.LineNo, this.SymbolNo);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.isAllowNull = this.parameters[this.ParamCount - 1].IsAllowNull;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      int index = 0;
      for (int paramCount = this.ParamCount; index < paramCount; ++index)
      {
        if (!this.paramValues[index].IsNull)
          return ((IValue) this.paramValues[index]).Value;
      }
      return (object) null;
    }
  }
}
