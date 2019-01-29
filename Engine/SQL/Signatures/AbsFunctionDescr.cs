namespace VistaDB.Engine.SQL.Signatures
{
  internal class AbsFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new AbsFunction(parser);
    }
  }
}
