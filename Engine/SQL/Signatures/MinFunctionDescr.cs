namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new MinFunction(parser);
    }
  }
}
