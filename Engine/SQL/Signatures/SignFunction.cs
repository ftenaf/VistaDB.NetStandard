using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SignFunction : Function
  {
    public SignFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Unknown;
      parameterTypes[0] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (Utils.IsCharacterDataType(this[0].DataType))
      {
        dataType = VistaDBType.Float;
      }
      else
      {
        if (!Utils.IsNumericDataType(this[0].DataType))
          throw new VistaDBSQLException(550, "SIGN", lineNo, symbolNo);
        dataType = this[0].DataType;
      }
      paramValues[0] = CreateColumn(dataType);
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      object obj = paramValues[0].Value;
      switch (dataType)
      {
        case VistaDBType.TinyInt:
          return (byte)Math.Sign((byte)obj);
        case VistaDBType.SmallInt:
          return (short)Math.Sign((short)obj);
        case VistaDBType.Int:
          return Math.Sign((int)obj);
        case VistaDBType.BigInt:
          return (long)Math.Sign((long)obj);
        case VistaDBType.Real:
          return (float)Math.Sign((float)obj);
        case VistaDBType.Float:
          return (double)Math.Sign((double)obj);
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return (Decimal)Math.Sign((Decimal)obj);
        default:
          throw new VistaDBSQLException(556, "Unknown data type", lineNo, symbolNo);
      }
    }
  }
}
