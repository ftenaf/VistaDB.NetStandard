﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetUtcDateFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new GetUtcDateFunction(parser);
    }
  }
}
