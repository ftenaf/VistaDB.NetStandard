﻿using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class MonthFunction : Function
  {
    public MonthFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Int;
      parameterTypes[0] = VistaDBType.DateTime;
    }

    protected override object ExecuteSubProgram()
    {
      return ((DateTime)paramValues[0].Value).Month;
    }
  }
}
