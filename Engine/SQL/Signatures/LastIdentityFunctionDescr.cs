namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastIdentityFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new LastIdentityFunction(parser);
    }
  }
}
