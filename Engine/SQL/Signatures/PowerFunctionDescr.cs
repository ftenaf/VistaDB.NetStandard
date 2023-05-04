namespace VistaDB.Engine.SQL.Signatures
{
  internal class PowerFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new PowerFunction(parser);
    }
  }
}
