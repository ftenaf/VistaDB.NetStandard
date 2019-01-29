namespace VistaDB.Engine.SQL.Signatures
{
  internal class BetweenOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new BetweenOperator(leftSignature, parser);
    }
  }
}
