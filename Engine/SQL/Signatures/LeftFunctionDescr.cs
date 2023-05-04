namespace VistaDB.Engine.SQL.Signatures
{
  internal class LeftFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new LeftFunction(parser);
    }
  }
}
