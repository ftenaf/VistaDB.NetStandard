namespace VistaDB.Engine.SQL.Signatures
{
  internal class CastFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new CastFunction(parser);
    }
  }
}
