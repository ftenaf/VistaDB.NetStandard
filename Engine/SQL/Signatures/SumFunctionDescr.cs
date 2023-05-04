namespace VistaDB.Engine.SQL.Signatures
{
  internal class SumFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new SumFunction(parser);
    }
  }
}
