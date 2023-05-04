namespace VistaDB.Engine.SQL.Signatures
{
  internal class ExistsOperatorDescr : Priority1Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return new ExistsOperator(parser);
    }
  }
}
