using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class AbsFunction : Function
  {
    public AbsFunction(SQLParser parser)
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
          throw new VistaDBSQLException(550, "ABS", lineNo, symbolNo);
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
          byte num1 = (byte) obj;
          return num1 < 0 ? -num1 : num1;
        case VistaDBType.SmallInt:
          short num2 = (short) obj;
          return num2 < 0 ? -num2 : num2;
        case VistaDBType.Int:
          int num3 = (int) obj;
          return num3 < 0 ? -num3 : num3;
        case VistaDBType.BigInt:
          long num4 = (long) obj;
          return num4 < 0L ? -num4 : num4;
        case VistaDBType.Real:
          float num5 = (float) obj;
          return (float)((double)num5 < 0.0 ? -(double)num5 : (double)num5);
        case VistaDBType.Float:
          double num6 = (double) obj;
          return num6 < 0.0 ? -num6 : num6;
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          Decimal num7 = (Decimal) obj;
          return num7 < new Decimal(0) ? -num7 : num7;
        default:
          throw new VistaDBSQLException(556, "Unknown data type", lineNo, symbolNo);
      }
    }
  }
}
