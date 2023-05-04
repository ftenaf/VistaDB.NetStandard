namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpStoredFunctionDesr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new SpStoredFunction(parser);
    }
  }
}
