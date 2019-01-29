using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNullFunction : Function
  {
    public IsNullFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      this.skipNull = false;
      this.parameterTypes[0] = VistaDBType.Unknown;
      this.parameterTypes[1] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.dataType = this[0].DataType;
      if (signatureType != SignatureType.Constant && this.dataType != this[1].DataType && this[1].SignatureType == SignatureType.Constant)
        this[1] = ConstantSignature.PrepareAndCheckConstant(this[1], this.dataType);
      this.paramValues[0] = this.CreateColumn(this.dataType);
      this.paramValues[1] = this.CreateColumn(this[1].DataType);
      ConstantSignature parameter = this.parameters[0] as ConstantSignature;
      if ((Signature) parameter != (Signature) null && parameter.AlwaysNull)
        this.dataType = this.paramValues[1].Type;
      this.isAllowNull = this.parameters[1].IsAllowNull;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      if (!this.paramValues[0].IsNull)
        return ((IValue) this.paramValues[0]).Value;
      return ((IValue) this.paramValues[1]).Value;
    }
  }
}
