namespace VistaDB.Engine.SQL.Signatures
{
  internal class UpperFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new UpperFunction(parser);
    }
  }
}
