namespace VistaDB.Engine.SQL.Signatures
{
  internal class SignFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SignFunction(parser);
    }
  }
}
