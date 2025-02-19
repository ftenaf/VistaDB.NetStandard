﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNullOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new IsNullOperator(leftSignature, parser);
    }
  }
}
