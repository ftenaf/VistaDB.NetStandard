namespace VistaDB.Engine.SQL.Signatures
{
  internal class CosFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new CosFunction(parser);
    }
  }
}
