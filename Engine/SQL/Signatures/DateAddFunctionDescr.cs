namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateAddFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DateAddFunction(parser);
    }
  }
}
