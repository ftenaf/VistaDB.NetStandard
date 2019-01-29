namespace VistaDB.Engine.SQL.Signatures
{
  internal class YearFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new YearFunction(parser);
    }
  }
}
