namespace VistaDB.Engine.SQL.Signatures
{
  internal class MonthFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new MonthFunction(parser);
    }
  }
}
