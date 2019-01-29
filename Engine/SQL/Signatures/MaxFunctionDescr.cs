namespace VistaDB.Engine.SQL.Signatures
{
  internal class MaxFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new MaxFunction(parser);
    }
  }
}
