namespace VistaDB.Engine.SQL.Signatures
{
  internal class Log10FunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new Log10Function(parser);
    }
  }
}
