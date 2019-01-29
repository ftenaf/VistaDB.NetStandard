namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnicodeFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new UnicodeFunction(parser);
    }
  }
}
