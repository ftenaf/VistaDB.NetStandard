namespace VistaDB.Engine.SQL.Signatures
{
  internal class MaxOfFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new MaxOfFunction(parser);
    }
  }
}
