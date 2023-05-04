namespace VistaDB.Engine.SQL.Signatures
{
  internal class CeilingFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CeilingFunction(parser);
    }
  }
}
