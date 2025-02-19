﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReplicateFunction : Function
  {
    public ReplicateFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      int num = (int)paramValues[1].Value - 1;
      if (num < 0)
        return null;
      string str1 = (string)paramValues[0].Value;
      string str2 = str1;
      for (int index = 0; index < num; ++index)
        str2 += str1;
      return str2;
    }

    public override int GetWidth()
    {
      return 8192;
    }
  }
}
