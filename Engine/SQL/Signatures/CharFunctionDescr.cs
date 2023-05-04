namespace VistaDB.Engine.SQL.Signatures
{
  internal class CharFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CharFunction(parser);
    }
  }
}
