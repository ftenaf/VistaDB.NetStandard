namespace VistaDB.Engine.SQL.Signatures
{
  internal class ConvertFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ConvertFunction(parser);
    }
  }
}
