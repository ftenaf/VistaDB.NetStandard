namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinOfFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new MinOfFunction(parser);
    }
  }
}
