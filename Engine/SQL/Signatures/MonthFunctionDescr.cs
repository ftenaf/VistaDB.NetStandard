namespace VistaDB.Engine.SQL.Signatures
{
  internal class MonthFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new MonthFunction(parser);
    }
  }
}
