namespace VistaDB.Engine.SQL.Signatures
{
  internal class CoalesceFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new CoalesceFunction(parser);
    }
  }
}
