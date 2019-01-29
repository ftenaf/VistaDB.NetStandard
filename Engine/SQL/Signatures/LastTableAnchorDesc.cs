namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastTableAnchorDesc : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new LastTableAnchor(parser);
    }
  }
}
