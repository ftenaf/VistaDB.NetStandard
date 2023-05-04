namespace VistaDB.Engine.SQL.Signatures
{
  internal class LTrimFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new LTrimFunction(parser);
    }
  }
}
