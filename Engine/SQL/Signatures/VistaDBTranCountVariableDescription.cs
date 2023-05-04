namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBTranCountVariableDescription : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new TranCountVariable(parser);
    }
  }
}
