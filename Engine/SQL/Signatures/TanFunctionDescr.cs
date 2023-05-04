namespace VistaDB.Engine.SQL.Signatures
{
  internal class TanFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new TanFunction(parser);
    }
  }
}
