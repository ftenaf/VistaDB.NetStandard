namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotEqualOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new NotEqualOperator(leftSignature, parser);
    }
  }
}
