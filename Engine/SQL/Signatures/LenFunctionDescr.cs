namespace VistaDB.Engine.SQL.Signatures
{
  internal class LenFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new LenFunction(parser);
    }
  }
}
