﻿using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SquareFunction : Function
  {
    public SquareFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Float;
      parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Pow((double) ((IValue) paramValues[0]).Value, 2.0);
    }
  }
}
