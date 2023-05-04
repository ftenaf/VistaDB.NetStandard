namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateDiffFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DateDiffFunction(parser);
    }
  }
}
