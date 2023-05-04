using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RoundFunction : Function
  {
    public RoundFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      dataType = VistaDBType.Unknown;
      parameterTypes[0] = VistaDBType.Unknown;
      parameterTypes[1] = VistaDBType.Int;
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
          throw new VistaDBSQLException(661, "ROUND", lineNo, symbolNo);
        dataType = this[0].DataType;
      }
      paramValues[0] = CreateColumn(dataType);
      return signatureType;
    }

        protected override object ExecuteSubProgram()
    {
      object obj = paramValues[0].Value;
      int num1 = (int)paramValues[1].Value;
      int num2 = Math.Abs(num1);
      switch (dataType)
      {
        case VistaDBType.TinyInt:
          if (num1 >= 0)
            return obj;
          return (byte)Math.Truncate(Math.Round((byte)obj * Math.Pow(0.1, num2)) * Math.Pow(10.0, num2));
        case VistaDBType.SmallInt:
          if (num1 >= 0)
            return obj;
          return (short)Math.Truncate(Math.Round((short)obj * Math.Pow(0.1, num2)) * Math.Pow(10.0, num2));
        case VistaDBType.Int:
          if (num1 >= 0)
            return obj;
          return (int)Math.Truncate(Math.Round((int)obj * Math.Pow(0.1, num2)) * Math.Pow(10.0, num2));
        case VistaDBType.BigInt:
          if (num1 >= 0)
            return obj;
          return (long)Math.Truncate(Math.Round((long)obj * Math.Pow(0.1, num2)) * Math.Pow(10.0, num2));
        case VistaDBType.Real:
          if (num1 > -1 && num1 < 29)
            return (float)Math.Round((double)(float)obj, num1);
          return Math.Round((double)(float)obj * Math.Pow(0.1, num2)) * Math.Pow(10.0, num2);
        case VistaDBType.Float:
          if (num1 > -1 && num1 < 29)
            return Math.Round((double)obj, num1);
          return Math.Round((double)obj * Math.Pow(0.1, num2)) * Math.Pow(10.0, num2);
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          if (num1 > -1 && num1 < 29)
            return Math.Round((Decimal)obj, num1);
          return Math.Round((Decimal)obj * (Decimal)Math.Pow(0.1, num2)) * (Decimal)Math.Pow(10.0, num2);
        default:
          throw new VistaDBSQLException(556, "Unknown data type", lineNo, symbolNo);
      }
    }
  }
}
