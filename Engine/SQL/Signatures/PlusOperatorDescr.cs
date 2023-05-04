namespace VistaDB.Engine.SQL.Signatures
{
  internal class PlusOperatorDescr : Priority2Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new PlusOperator(leftSignature, parser);
    }
  }
}
