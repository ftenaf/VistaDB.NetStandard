namespace VistaDB.Engine.SQL.Signatures
{
  internal class StuffFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new StuffFunction(parser);
    }
  }
}
