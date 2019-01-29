namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBVersionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new VersionVariable(parser);
    }
  }
}
