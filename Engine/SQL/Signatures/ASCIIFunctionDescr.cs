﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class ASCIIFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ASCIIFunction(parser);
    }
  }
}
