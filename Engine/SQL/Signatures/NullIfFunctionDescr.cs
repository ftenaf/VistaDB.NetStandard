namespace VistaDB.Engine.SQL.Signatures
{
  internal class NullIfFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new NullIfFunction(parser);
    }
  }
}
