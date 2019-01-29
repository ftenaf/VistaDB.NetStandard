namespace VistaDB.Engine.SQL.Signatures
{
  internal class LessOrEqualOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new LessOrEqualOperator(leftSignature, parser);
    }
  }
}
