namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReverseFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ReverseFunction(parser);
    }
  }
}
