namespace VistaDB.Engine.SQL.Signatures
{
  internal class SinFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SinFunction(parser);
    }
  }
}
