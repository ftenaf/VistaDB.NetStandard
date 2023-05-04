namespace VistaDB.Engine.SQL.Signatures
{
  internal class StDevFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new StDevFunction(parser);
    }
  }
}
