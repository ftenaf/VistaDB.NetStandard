﻿using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class TanFunction : Function
  {
    public TanFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Float;
      parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return Math.Tan((double)paramValues[0].Value);
    }
  }
}
