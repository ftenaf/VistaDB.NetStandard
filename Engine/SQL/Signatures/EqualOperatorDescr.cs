﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class EqualOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new EqualOperator(leftSignature, parser);
    }
  }
}
