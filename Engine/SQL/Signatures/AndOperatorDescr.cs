namespace VistaDB.Engine.SQL.Signatures
{
  internal class AndOperatorDescr : IOperatorDescr
  {
    public Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new AndOperator(leftSignature, parser);
    }

    public int Priority
    {
      get
      {
        return 5;
      }
    }
  }
}
