namespace VistaDB.Engine.SQL.Signatures
{
  internal class MaxOfFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new MaxOfFunction(parser);
    }
  }
}
