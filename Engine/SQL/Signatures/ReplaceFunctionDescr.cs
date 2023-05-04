namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReplaceFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ReplaceFunction(parser);
    }
  }
}
