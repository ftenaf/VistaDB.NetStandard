namespace VistaDB.Engine.SQL.Signatures
{
  internal class RadiansFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new RadiansFunction(parser);
    }
  }
}
