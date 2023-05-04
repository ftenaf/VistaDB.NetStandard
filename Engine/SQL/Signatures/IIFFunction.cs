using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IIFFunction : Function
  {
    public IIFFunction(SQLParser parser)
      : base(parser, 3, true)
    {
      skipNull = false;
      parameterTypes[0] = VistaDBType.Bit;
      parameterTypes[1] = VistaDBType.Unknown;
      parameterTypes[2] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = this[1].DataType;
      paramValues[1] = CreateColumn(dataType);
      paramValues[2] = CreateColumn(dataType);
      isAllowNull = parameters[1].IsAllowNull || parameters[2].IsAllowNull;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      if (paramValues[0].IsNull)
        return null;
      if (!(bool)paramValues[0].Value)
        return paramValues[2].Value;
      return paramValues[1].Value;
    }
  }
}
