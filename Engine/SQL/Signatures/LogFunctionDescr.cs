namespace VistaDB.Engine.SQL.Signatures
{
  internal class LogFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new LogFunction(parser);
    }
  }
}
