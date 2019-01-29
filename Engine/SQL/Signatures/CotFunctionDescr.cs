namespace VistaDB.Engine.SQL.Signatures
{
  internal class CotFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new CotFunction(parser);
    }
  }
}
