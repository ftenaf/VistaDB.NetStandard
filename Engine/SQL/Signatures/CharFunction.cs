﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CharFunction : Function
  {
    public CharFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Char;
      this.parameterTypes[0] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      int num = (int) ((IValue) this.paramValues[0]).Value;
      if (num < 0 || num > (int) byte.MaxValue)
        return (object) null;
      return (object) ((char) num).ToString();
    }

    public override int GetWidth()
    {
      return 1;
    }
  }
}
