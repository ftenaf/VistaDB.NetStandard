namespace VistaDB.Engine.SQL.Signatures
{
  internal class ACosFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new ACosFunction(parser);
    }
  }
}
