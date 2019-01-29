namespace VistaDB.Engine.SQL.Signatures
{
  internal class SquareFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SquareFunction(parser);
    }
  }
}
