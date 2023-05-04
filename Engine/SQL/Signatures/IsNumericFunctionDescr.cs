namespace VistaDB.Engine.SQL.Signatures
{
  internal class IsNumericFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new IsNumericFunction(parser);
    }
  }
}
