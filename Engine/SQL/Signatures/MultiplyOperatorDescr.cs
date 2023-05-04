namespace VistaDB.Engine.SQL.Signatures
{
  internal class MultiplyOperatorDescr : Priority1Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new MultiplyOperator(leftSignature, parser);
    }
  }
}
