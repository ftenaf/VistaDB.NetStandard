using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IntFunction : Function
  {
    public IntFunction(SQLParser parser)
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
          throw new VistaDBSQLException(550, "INT", lineNo, symbolNo);
        dataType = this[0].DataType;
      }
      paramValues[0] = CreateColumn(dataType);
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      object obj = ((IValue) paramValues[0]).Value;
      switch (dataType)
      {
        case VistaDBType.TinyInt:
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
          return obj;
        case VistaDBType.Real:
          return (object) (float) (long) (float) obj;
        case VistaDBType.Float:
          return (object) (double) (long) (double) obj;
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return (object) (Decimal) (long) ((Decimal) obj);
        default:
          throw new VistaDBSQLException(556, "Unknown data type", lineNo, symbolNo);
      }
    }
  }
}
