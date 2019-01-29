namespace VistaDB.Engine.SQL.Signatures
{
  internal class RoundFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new RoundFunction(parser);
    }
  }
}
