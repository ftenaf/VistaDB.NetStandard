namespace VistaDB.Engine.SQL.Signatures
{
  internal class ExpFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ExpFunction(parser);
    }
  }
}
