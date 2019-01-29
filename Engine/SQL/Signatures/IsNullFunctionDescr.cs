namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNullFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new IsNullFunction(parser);
    }
  }
}
