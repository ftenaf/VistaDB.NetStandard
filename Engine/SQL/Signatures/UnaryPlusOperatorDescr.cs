namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnaryPlusOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new UnaryPlusOperator(parser);
    }
  }
}
