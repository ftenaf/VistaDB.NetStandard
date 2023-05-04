namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpColumnsFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new SpColumnsFunction(parser);
    }
  }
}
