namespace VistaDB.Engine.SQL.Signatures
{
  internal class ASinFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ASinFunction(parser);
    }
  }
}
