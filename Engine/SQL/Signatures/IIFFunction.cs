using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IIFFunction : Function
  {
    public IIFFunction(SQLParser parser)
      : base(parser, 3, true)
    {
      this.skipNull = false;
      this.parameterTypes[0] = VistaDBType.Bit;
      this.parameterTypes[1] = VistaDBType.Unknown;
      this.parameterTypes[2] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.dataType = this[1].DataType;
      this.paramValues[1] = this.CreateColumn(this.dataType);
      this.paramValues[2] = this.CreateColumn(this.dataType);
      this.isAllowNull = this.parameters[1].IsAllowNull || this.parameters[2].IsAllowNull;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      if (this.paramValues[0].IsNull)
        return (object) null;
      if (!(bool) ((IValue) this.paramValues[0]).Value)
        return ((IValue) this.paramValues[2]).Value;
      return ((IValue) this.paramValues[1]).Value;
    }
  }
}
