﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class AvgFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new AvgFunction(parser);
    }
  }
}
