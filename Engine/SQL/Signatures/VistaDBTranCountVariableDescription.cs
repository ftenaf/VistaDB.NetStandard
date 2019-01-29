namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBTranCountVariableDescription : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new TranCountVariable(parser);
    }
  }
}
