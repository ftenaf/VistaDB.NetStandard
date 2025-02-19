﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class ModOperatorDescr : Priority1Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new ModOperator(leftSignature, parser);
    }
  }
}
