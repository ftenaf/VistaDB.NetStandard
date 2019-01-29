using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class MaxOfFunction : Function
  {
    public MaxOfFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      if (this.ParamCount < 1)
        throw new VistaDBSQLException(501, "MAXOF", this.lineNo, this.symbolNo);
      int index = 0;
      for (int paramCount = this.ParamCount; index < paramCount; ++index)
        this.parameterTypes[index] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.dataType = this[0].DataType;
      int index1 = 1;
      for (int paramCount = this.ParamCount; index1 < paramCount; ++index1)
      {
        VistaDBType dataType = this[index1].DataType;
        if (!Utils.CompatibleTypes(dataType, this.dataType))
          throw new VistaDBSQLException(550, this.text, this.lineNo, this.symbolNo);
        if (Utils.CompareRank(this.dataType, dataType) < 0)
          this.dataType = dataType;
      }
      int index2 = 0;
      for (int paramCount = this.ParamCount; index2 < paramCount; ++index2)
        this.paramValues[index2] = this.CreateColumn(this.dataType);
      return signatureType;
    }

    protected virtual bool AcceptValue(int result)
    {
      return result > 0;
    }

    protected override object ExecuteSubProgram()
    {
      ((IValue) this.result).Value = ((IValue) this.paramValues[0]).Value;
      int index = 1;
      for (int paramCount = this.ParamCount; index < paramCount; ++index)
      {
        if (this.AcceptValue(this.paramValues[index].Compare((IVistaDBColumn) this.result)))
          ((IValue) this.result).Value = ((IValue) this.paramValues[index]).Value;
      }
      return ((IValue) this.result).Value;
    }
  }
}
