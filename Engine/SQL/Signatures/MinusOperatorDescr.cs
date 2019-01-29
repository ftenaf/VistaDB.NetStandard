namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinusOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new MinusOperator(leftSignature, parser);
    }
  }
}
