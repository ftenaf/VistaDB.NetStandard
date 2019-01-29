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
      this.dataType = VistaDBType.Unknown;
      this.parameterTypes[0] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (Utils.IsCharacterDataType(this[0].DataType))
      {
        this.dataType = VistaDBType.Float;
      }
      else
      {
        if (!Utils.IsNumericDataType(this[0].DataType))
          throw new VistaDBSQLException(550, "ABS", this.lineNo, this.symbolNo);
        this.dataType = this[0].DataType;
      }
      this.paramValues[0] = this.CreateColumn(this.dataType);
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      object obj = ((IValue) this.paramValues[0]).Value;
      switch (this.dataType)
      {
        case VistaDBType.TinyInt:
          byte num1 = (byte) obj;
          return (object) (num1 < (byte) 0 ? (int) -num1 : (int) num1);
        case VistaDBType.SmallInt:
          short num2 = (short) obj;
          return (object) (num2 < (short) 0 ? (int) -num2 : (int) num2);
        case VistaDBType.Int:
          int num3 = (int) obj;
          return (object) (num3 < 0 ? -num3 : num3);
        case VistaDBType.BigInt:
          long num4 = (long) obj;
          return (object) (num4 < 0L ? -num4 : num4);
        case VistaDBType.Real:
          float num5 = (float) obj;
          return (object) (float) ((double) num5 < 0.0 ? -(double) num5 : (double) num5);
        case VistaDBType.Float:
          double num6 = (double) obj;
          return (object) (num6 < 0.0 ? -num6 : num6);
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          Decimal num7 = (Decimal) obj;
          return (object) (num7 < new Decimal(0) ? -num7 : num7);
        default:
          throw new VistaDBSQLException(556, "Unknown data type", this.lineNo, this.symbolNo);
      }
    }
  }
}
