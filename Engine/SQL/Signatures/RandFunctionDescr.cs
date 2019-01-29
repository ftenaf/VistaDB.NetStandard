namespace VistaDB.Engine.SQL.Signatures
{
  internal class RandFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new RandFunction(parser);
    }
  }
}
