namespace VistaDB.Engine.SQL.Signatures
{
  internal class FracFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new FracFunction(parser);
    }
  }
}
