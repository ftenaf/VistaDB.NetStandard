using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class FracFunction : Function
  {
    public FracFunction(SQLParser parser)
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
          throw new VistaDBSQLException(550, "FRAC", this.lineNo, this.symbolNo);
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
          return (object) (byte) 0;
        case VistaDBType.SmallInt:
          return (object) (short) 0;
        case VistaDBType.Int:
          return (object) 0;
        case VistaDBType.BigInt:
          return (object) 0L;
        case VistaDBType.Real:
          float num = (float) obj;
          return num - Math.Truncate(num);
        case VistaDBType.Float:
          double d1 = (double) obj;
          return (decimal) d1 - (decimal) Math.Truncate(d1);
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          Decimal d2 = (Decimal) obj;
          return (object) (d2 - Math.Truncate(d2));
        default:
          throw new VistaDBSQLException(556, "Unknown data type", this.lineNo, this.symbolNo);
      }
    }
  }
}
