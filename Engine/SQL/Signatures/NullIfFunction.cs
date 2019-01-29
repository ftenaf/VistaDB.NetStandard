using System;
using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NullIfFunction : Function
  {
    public NullIfFunction(SQLParser parser)
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
      Signature signature = this[1];
      if (signatureType != SignatureType.Constant && signature.DataType != this.dataType && signature.SignatureType == SignatureType.Constant)
      {
        signature = ConstantSignature.PrepareAndCheckConstant(signature, this.dataType);
        this[1] = signature;
      }
      this.paramValues[0] = this.CreateColumn(this.dataType);
      this.paramValues[1] = this.CreateColumn(signature.DataType);
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      IColumn paramValue1 = this.paramValues[0];
      IColumn paramValue2 = this.paramValues[1];
      if (paramValue1.IsNull || paramValue2.IsNull)
        return ((IValue) paramValue1).Value;
      if (paramValue1.InternalType == paramValue2.InternalType)
      {
        if (paramValue1.Compare((IVistaDBColumn) paramValue2) != 0)
          return ((IValue) paramValue1).Value;
        return (object) null;
      }
      if (!this.ExistConvertion(paramValue1.InternalType, paramValue2.InternalType))
        return ((IValue) paramValue1).Value;
      IColumn column = this.CreateColumn(this.dataType);
      try
      {
        this.Convert((IValue) paramValue2, (IValue) column);
      }
      catch (Exception ex)
      {
        return ((IValue) paramValue1).Value;
      }
      if (paramValue1.Compare((IVistaDBColumn) column) != 0)
        return ((IValue) paramValue1).Value;
      return (object) null;
    }
  }
}
