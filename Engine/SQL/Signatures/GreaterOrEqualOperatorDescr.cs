namespace VistaDB.Engine.SQL.Signatures
{
  internal class GreaterOrEqualOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new GreaterOrEqualOperator(leftSignature, parser);
    }
  }
}
