namespace VistaDB.Engine.SQL.Signatures
{
  internal class LowerFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new LowerFunction(parser);
    }
  }
}
