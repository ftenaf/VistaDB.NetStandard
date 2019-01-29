namespace VistaDB.Engine.SQL.Signatures
{
  internal class RightFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new RightFunction(parser);
    }
  }
}
