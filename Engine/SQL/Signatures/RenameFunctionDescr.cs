namespace VistaDB.Engine.SQL.Signatures
{
  internal class RenameFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new RenameFunction(parser);
    }
  }
}
