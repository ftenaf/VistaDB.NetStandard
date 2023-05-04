namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnaryMinusOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new UnaryMinusOperator(parser);
    }
  }
}
