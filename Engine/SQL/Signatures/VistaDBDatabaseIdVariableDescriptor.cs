namespace VistaDB.Engine.SQL.Signatures
{
  internal class VistaDBDatabaseIdVariableDescriptor : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DatabaseIdVariable(parser);
    }
  }
}
