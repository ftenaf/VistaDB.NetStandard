namespace VistaDB.Engine.SQL.Signatures
{
  internal class DatePartFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DatePartFunction(parser);
    }
  }
}
