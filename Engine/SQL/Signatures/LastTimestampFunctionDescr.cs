namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastTimestampFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new LastTimestampFunction(parser);
    }
  }
}
