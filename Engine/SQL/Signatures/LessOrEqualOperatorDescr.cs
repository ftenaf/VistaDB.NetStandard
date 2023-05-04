namespace VistaDB.Engine.SQL.Signatures
{
  internal class LessOrEqualOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new LessOrEqualOperator(leftSignature, parser);
    }
  }
}
