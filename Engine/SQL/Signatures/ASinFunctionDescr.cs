namespace VistaDB.Engine.SQL.Signatures
{
  internal class ASinFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new ASinFunction(parser);
    }
  }
}
