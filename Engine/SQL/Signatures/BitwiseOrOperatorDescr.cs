namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseOrOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new BitwiseOrOperator(leftSignature, parser);
    }
  }
}
