namespace VistaDB.Engine.SQL.Signatures
{
  internal class UpperFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new UpperFunction(parser);
    }
  }
}
