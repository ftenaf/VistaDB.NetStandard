﻿using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CeilingFunction : Function
  {
    public CeilingFunction(SQLParser parser)
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
          throw new VistaDBSQLException(550, "CEILING", lineNo, symbolNo);
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
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
          return obj;
        case VistaDBType.Real:
          return (float)Math.Ceiling((double)(float)obj);
        case VistaDBType.Float:
          return Math.Ceiling((double)obj);
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return (Decimal)Math.Ceiling((double)((Decimal)obj));
        default:
          throw new VistaDBSQLException(556, "Unknown data type", lineNo, symbolNo);
      }
    }
  }
}
