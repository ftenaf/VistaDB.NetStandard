﻿using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ATanFunction : Function
  {
    public ATanFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Float;
      parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return Math.Atan((double)paramValues[0].Value);
    }
  }
}
