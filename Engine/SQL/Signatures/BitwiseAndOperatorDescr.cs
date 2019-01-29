namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseAndOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new BitwiseAndOperator(leftSignature, parser);
    }
  }
}
