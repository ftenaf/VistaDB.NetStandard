namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseXorOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new BitwiseXorOperator(leftSignature, parser);
    }
  }
}
