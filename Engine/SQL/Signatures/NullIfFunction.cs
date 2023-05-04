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
      skipNull = false;
      parameterTypes[0] = VistaDBType.Unknown;
      parameterTypes[1] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = this[0].DataType;
      Signature signature = this[1];
      if (signatureType != SignatureType.Constant && signature.DataType != dataType && signature.SignatureType == SignatureType.Constant)
      {
        signature = ConstantSignature.PrepareAndCheckConstant(signature, dataType);
        this[1] = signature;
      }
      paramValues[0] = CreateColumn(dataType);
      paramValues[1] = CreateColumn(signature.DataType);
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      IColumn paramValue1 = paramValues[0];
      IColumn paramValue2 = paramValues[1];
      if (paramValue1.IsNull || paramValue2.IsNull)
        return ((IValue) paramValue1).Value;
      if (paramValue1.InternalType == paramValue2.InternalType)
      {
        if (paramValue1.Compare((IVistaDBColumn) paramValue2) != 0)
          return ((IValue) paramValue1).Value;
        return (object) null;
      }
      if (!ExistConvertion(paramValue1.InternalType, paramValue2.InternalType))
        return ((IValue) paramValue1).Value;
      IColumn column = CreateColumn(dataType);
      try
      {
        Convert((IValue) paramValue2, (IValue) column);
      }
      catch (Exception)
            {
        return ((IValue) paramValue1).Value;
      }
      if (paramValue1.Compare((IVistaDBColumn) column) != 0)
        return ((IValue) paramValue1).Value;
      return (object) null;
    }
  }
}
