﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class LookupFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new LookupFunction(parser);
    }
  }
}
