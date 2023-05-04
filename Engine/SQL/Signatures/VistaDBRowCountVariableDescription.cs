namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBRowCountVariableDescription : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new RowCountVariable(parser);
    }
  }
}
