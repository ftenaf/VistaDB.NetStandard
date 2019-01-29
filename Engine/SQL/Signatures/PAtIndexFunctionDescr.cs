namespace VistaDB.Engine.SQL.Signatures
{
  internal class PAtIndexFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new PatternIndexFunction(parser);
    }
  }
}
