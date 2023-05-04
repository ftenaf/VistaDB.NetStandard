namespace VistaDB.Engine.SQL.Signatures
{
  internal class AbsFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new AbsFunction(parser);
    }
  }
}
