namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetDateFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new GetDateFunction(parser);
    }
  }
}
