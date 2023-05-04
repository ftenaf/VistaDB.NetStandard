namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new MinFunction(parser);
    }
  }
}
