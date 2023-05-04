namespace VistaDB.Engine.SQL.Signatures
{
  internal class DegreesFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DegreesFunction(parser);
    }
  }
}
