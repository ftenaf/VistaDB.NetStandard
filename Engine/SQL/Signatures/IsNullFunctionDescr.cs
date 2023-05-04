namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNullFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new IsNullFunction(parser);
    }
  }
}
