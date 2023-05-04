namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReplicateFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ReplicateFunction(parser);
    }
  }
}
