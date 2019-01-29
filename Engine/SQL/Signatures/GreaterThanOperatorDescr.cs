namespace VistaDB.Engine.SQL.Signatures
{
  internal class GreaterThanOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new GreaterThanOperator(leftSignature, parser);
    }
  }
}
