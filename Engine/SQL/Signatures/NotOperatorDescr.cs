﻿namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotOperatorDescr : IOperatorDescr
  {
    public Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new NotOperator(parser);
    }

    public int Priority
    {
      get
      {
        return 4;
      }
    }
  }
}
