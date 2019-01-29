namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReverseFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new ReverseFunction(parser);
    }
  }
}
