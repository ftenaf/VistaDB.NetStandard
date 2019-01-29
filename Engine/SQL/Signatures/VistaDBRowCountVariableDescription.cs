namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBRowCountVariableDescription : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new RowCountVariable(parser);
    }
  }
}
