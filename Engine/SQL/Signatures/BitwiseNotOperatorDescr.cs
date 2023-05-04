namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseNotOperatorDescr : Priority1Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new BitwiseNotOperator(parser);
    }
  }
}
