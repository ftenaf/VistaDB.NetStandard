namespace VistaDB.Engine.SQL.Signatures
{
  internal class StrFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new StrFunction(parser);
    }
  }
}
