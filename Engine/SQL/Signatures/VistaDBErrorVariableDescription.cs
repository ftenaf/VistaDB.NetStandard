namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBErrorVariableDescription : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new ErrorVariable(parser);
    }
  }
}
