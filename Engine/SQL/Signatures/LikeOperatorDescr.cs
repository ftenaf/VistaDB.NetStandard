namespace VistaDB.Engine.SQL.Signatures
{
  internal class LikeOperatorDescr : Priority3Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new LikeOperator(leftSignature, parser);
    }
  }
}
