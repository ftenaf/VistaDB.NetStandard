namespace VistaDB.Engine.SQL.Signatures
{
  internal class FracFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new FracFunction(parser);
    }
  }
}
