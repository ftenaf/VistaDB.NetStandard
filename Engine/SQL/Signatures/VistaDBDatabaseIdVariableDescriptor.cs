namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBDatabaseIdVariableDescriptor : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new DatabaseIdVariable(parser);
    }
  }
}
