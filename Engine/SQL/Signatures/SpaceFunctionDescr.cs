namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpaceFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SpaceFunction(parser);
    }
  }
}
