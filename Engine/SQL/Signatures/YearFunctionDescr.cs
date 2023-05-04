namespace VistaDB.Engine.SQL.Signatures
{
  internal class YearFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new YearFunction(parser);
    }
  }
}
