namespace VistaDB.Engine.SQL.Signatures
{
  internal class CountBigFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CountBigFunction(parser);
    }
  }
}
