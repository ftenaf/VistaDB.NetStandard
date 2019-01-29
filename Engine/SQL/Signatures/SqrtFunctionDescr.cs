namespace VistaDB.Engine.SQL.Signatures
{
  internal class SqrtFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SqrtFunction(parser);
    }
  }
}
