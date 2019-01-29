namespace VistaDB.Engine.SQL.Signatures
{
  internal class SubStringFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SubStringFunction(parser);
    }
  }
}
