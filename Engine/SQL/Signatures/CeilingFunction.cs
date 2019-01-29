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
          throw new VistaDBSQLException(550, "CEILING", this.lineNo, this.symbolNo);
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
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
          return obj;
        case VistaDBType.Real:
          return (object) (float) Math.Ceiling((double) (float) obj);
        case VistaDBType.Float:
          return (object) Math.Ceiling((double) obj);
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return (object) (Decimal) Math.Ceiling((double) ((Decimal) obj));
        default:
          throw new VistaDBSQLException(556, "Unknown data type", this.lineNo, this.symbolNo);
      }
    }
  }
}
