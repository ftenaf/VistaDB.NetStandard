namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpForeignKeysFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new SpForeignKeyFunction(parser);
    }
  }
}
