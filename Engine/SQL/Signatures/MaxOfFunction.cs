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
      if (ParamCount < 1)
        throw new VistaDBSQLException(501, "MAXOF", lineNo, symbolNo);
      int index = 0;
      for (int paramCount = ParamCount; index < paramCount; ++index)
        parameterTypes[index] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = this[0].DataType;
      int index1 = 1;
      for (int paramCount = ParamCount; index1 < paramCount; ++index1)
      {
        VistaDBType dataType = this[index1].DataType;
        if (!Utils.CompatibleTypes(dataType, this.dataType))
          throw new VistaDBSQLException(550, text, lineNo, symbolNo);
        if (Utils.CompareRank(this.dataType, dataType) < 0)
          this.dataType = dataType;
      }
      int index2 = 0;
      for (int paramCount = ParamCount; index2 < paramCount; ++index2)
        paramValues[index2] = CreateColumn(dataType);
      return signatureType;
    }

    protected virtual bool AcceptValue(int result)
    {
      return result > 0;
    }

    protected override object ExecuteSubProgram()
    {
      ((IValue) result).Value = ((IValue) paramValues[0]).Value;
      int index = 1;
      for (int paramCount = ParamCount; index < paramCount; ++index)
      {
        if (AcceptValue(paramValues[index].Compare((IVistaDBColumn) result)))
          ((IValue) result).Value = ((IValue) paramValues[index]).Value;
      }
      return ((IValue) result).Value;
    }
  }
}
