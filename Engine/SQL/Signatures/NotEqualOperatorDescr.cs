namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotEqualOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new NotEqualOperator(leftSignature, parser);
    }
  }
}
