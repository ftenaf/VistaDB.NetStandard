﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class OrOperatorDescr : IOperatorDescr
  {
    public Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new OrOperator(leftSignature, parser);
    }

    public int Priority
    {
      get
      {
        return 6;
      }
    }
  }
}
