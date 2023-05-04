namespace VistaDB.Engine.SQL.Signatures
{
  internal class FloorFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new FloorFunction(parser);
    }
  }
}
