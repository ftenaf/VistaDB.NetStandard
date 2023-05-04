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
        return paramValue1.Value;
      if (paramValue1.InternalType == paramValue2.InternalType)
      {
        if (paramValue1.Compare(paramValue2) != 0)
          return paramValue1.Value;
        return null;
      }
      if (!ExistConvertion(paramValue1.InternalType, paramValue2.InternalType))
        return paramValue1.Value;
      IColumn column = CreateColumn(dataType);
      try
      {
        Convert(paramValue2, column);
      }
      catch (Exception)
            {
        return paramValue1.Value;
      }
      if (paramValue1.Compare(column) != 0)
        return paramValue1.Value;
      return null;
    }
  }
}
