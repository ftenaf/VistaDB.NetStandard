namespace VistaDB.Engine.SQL.Signatures
{
  internal class ATN2FunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ATN2Function(parser);
    }
  }
}
