namespace VistaDB.Engine.SQL.Signatures
{
  internal class PIFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new PIFunction(parser);
    }
  }
}
