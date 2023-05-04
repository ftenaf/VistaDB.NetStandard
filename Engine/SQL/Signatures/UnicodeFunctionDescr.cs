namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnicodeFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new UnicodeFunction(parser);
    }
  }
}
