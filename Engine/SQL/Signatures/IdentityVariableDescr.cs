﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class IdentityVariableDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new IdentityVariable(parser);
    }
  }
}
