namespace VistaDB.Engine.SQL.Signatures
{
  internal class CaseFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CaseFunction(parser);
    }
  }
}
