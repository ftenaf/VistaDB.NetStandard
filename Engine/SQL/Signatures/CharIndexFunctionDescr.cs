namespace VistaDB.Engine.SQL.Signatures
{
  internal class CharIndexFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CharIndexFunction(parser);
    }
  }
}
