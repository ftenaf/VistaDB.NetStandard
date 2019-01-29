namespace VistaDB.Engine.SQL.Signatures
{
  internal class LessThanOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new LessThanOperator(leftSignature, parser);
    }
  }
}
