using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNullFunction : Function
  {
    public IsNullFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      skipNull = false;
      parameterTypes[0] = VistaDBType.Unknown;
      parameterTypes[1] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = this[0].DataType;
      if (signatureType != SignatureType.Constant && dataType != this[1].DataType && this[1].SignatureType == SignatureType.Constant)
        this[1] = ConstantSignature.PrepareAndCheckConstant(this[1], dataType);
      paramValues[0] = CreateColumn(dataType);
      paramValues[1] = CreateColumn(this[1].DataType);
      ConstantSignature parameter = parameters[0] as ConstantSignature;
      if ((Signature) parameter != (Signature) null && parameter.AlwaysNull)
        dataType = paramValues[1].Type;
      isAllowNull = parameters[1].IsAllowNull;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      if (!paramValues[0].IsNull)
        return ((IValue) paramValues[0]).Value;
      return ((IValue) paramValues[1]).Value;
    }
  }
}
