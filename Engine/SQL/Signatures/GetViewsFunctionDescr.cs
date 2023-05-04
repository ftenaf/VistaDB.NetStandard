namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetViewsFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new GetViewsFunction(parser);
    }
  }
}
