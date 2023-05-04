namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateNameFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DateNameFunction(parser);
    }
  }
}
