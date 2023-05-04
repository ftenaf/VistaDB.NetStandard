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
      object obj = ((IValue) paramValues[0]).Value;
      int num1 = (int) ((IValue) paramValues[1]).Value;
      int num2 = Math.Abs(num1);
      switch (dataType)
      {
        case VistaDBType.TinyInt:
          if (num1 >= 0)
            return obj;
          return (object) (byte) Math.Truncate(Math.Round((double) (byte) obj * Math.Pow(0.1, (double) num2)) * Math.Pow(10.0, (double) num2));
        case VistaDBType.SmallInt:
          if (num1 >= 0)
            return obj;
          return (object) (short) Math.Truncate(Math.Round((double) (short) obj * Math.Pow(0.1, (double) num2)) * Math.Pow(10.0, (double) num2));
        case VistaDBType.Int:
          if (num1 >= 0)
            return obj;
          return (object) (int) Math.Truncate(Math.Round((double) (int) obj * Math.Pow(0.1, (double) num2)) * Math.Pow(10.0, (double) num2));
        case VistaDBType.BigInt:
          if (num1 >= 0)
            return obj;
          return (object) (long) Math.Truncate(Math.Round((double) (long) obj * Math.Pow(0.1, (double) num2)) * Math.Pow(10.0, (double) num2));
        case VistaDBType.Real:
          if (num1 > -1 && num1 < 29)
            return (object) (float) Math.Round((double) (float) obj, num1);
          return (object) (Math.Round((double) (float) obj * Math.Pow(0.1, (double) num2)) * Math.Pow(10.0, (double) num2));
        case VistaDBType.Float:
          if (num1 > -1 && num1 < 29)
            return (object) Math.Round((double) obj, num1);
          return (object) (Math.Round((double) obj * Math.Pow(0.1, (double) num2)) * Math.Pow(10.0, (double) num2));
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          if (num1 > -1 && num1 < 29)
            return (object) Math.Round((Decimal) obj, num1);
          return (object) (Math.Round((Decimal) obj * (Decimal) Math.Pow(0.1, (double) num2)) * (Decimal) Math.Pow(10.0, (double) num2));
        default:
          throw new VistaDBSQLException(556, "Unknown data type", lineNo, symbolNo);
      }
    }
  }
}
