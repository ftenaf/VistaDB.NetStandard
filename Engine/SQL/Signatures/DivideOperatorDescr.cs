namespace VistaDB.Engine.SQL.Signatures
{
  internal class DivideOperatorDescr : Priority1Descr
  {
    public override Signature CreateSignature(Signature leftSignature, SQLParser parser)
    {
      return (Signature) new DivideOperator(leftSignature, parser);
    }
  }
}
