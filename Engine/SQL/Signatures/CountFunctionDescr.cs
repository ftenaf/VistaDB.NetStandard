namespace VistaDB.Engine.SQL.Signatures
{
  internal class CountFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CountFunction(parser);
    }
  }
}
