namespace VistaDB.Engine.SQL.Signatures
{
  internal class ContainsFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new ContainsFunction(parser);
    }
  }
}
