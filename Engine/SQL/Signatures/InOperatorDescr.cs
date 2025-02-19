﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class InOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new InOperator(leftSignature, parser);
    }
  }
}
