namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpStoredFunctionDesr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SpStoredFunction(parser);
    }
  }
}
