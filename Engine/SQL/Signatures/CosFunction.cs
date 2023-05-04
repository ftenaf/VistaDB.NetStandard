﻿using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CosFunction : Function
  {
    public CosFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Float;
      parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Cos((double) ((IValue) paramValues[0]).Value);
    }
  }
}
