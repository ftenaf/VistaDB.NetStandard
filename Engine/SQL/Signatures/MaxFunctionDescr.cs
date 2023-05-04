namespace VistaDB.Engine.SQL.Signatures
{
  internal class MaxFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new MaxFunction(parser);
    }
  }
}
