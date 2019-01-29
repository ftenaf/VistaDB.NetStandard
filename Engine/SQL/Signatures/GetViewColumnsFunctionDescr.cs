namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetViewColumnsFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new GetViewColumnsFunction(parser);
    }
  }
}
