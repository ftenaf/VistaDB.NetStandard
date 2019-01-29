namespace VistaDB.Engine.SQL.Signatures
{
  internal class NewIDFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new NewIDFunction(parser);
    }
  }
}
