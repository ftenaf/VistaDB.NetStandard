namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpStoredProceduresDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new SpStoredProcedures(parser);
    }
  }
}
