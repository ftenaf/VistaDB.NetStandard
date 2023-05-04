namespace VistaDB.Engine.SQL.Signatures
{
  internal class ATanFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ATanFunction(parser);
    }
  }
}
