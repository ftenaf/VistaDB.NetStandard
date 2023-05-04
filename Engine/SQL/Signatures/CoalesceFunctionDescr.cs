namespace VistaDB.Engine.SQL.Signatures
{
  internal class CoalesceFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new CoalesceFunction(parser);
    }
  }
}
