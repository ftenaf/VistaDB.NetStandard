namespace VistaDB.Engine.SQL.Signatures
{
  internal class CeilingFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new CeilingFunction(parser);
    }
  }
}
