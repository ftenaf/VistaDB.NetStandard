namespace VistaDB.Engine.SQL.Signatures
{
  internal class LeftFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new LeftFunction(parser);
    }
  }
}
