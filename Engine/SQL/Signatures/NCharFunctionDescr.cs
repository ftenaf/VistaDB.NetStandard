namespace VistaDB.Engine.SQL.Signatures
{
  internal class NCharFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new NCharFunction(parser);
    }
  }
}
