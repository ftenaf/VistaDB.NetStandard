namespace VistaDB.Engine.SQL.Signatures
{
  internal class IntFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new IntFunction(parser);
    }
  }
}
