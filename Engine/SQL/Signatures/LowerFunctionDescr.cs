namespace VistaDB.Engine.SQL.Signatures
{
  internal class LowerFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new LowerFunction(parser);
    }
  }
}
