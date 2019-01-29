namespace VistaDB.Engine.SQL.Signatures
{
  internal class DegreesFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new DegreesFunction(parser);
    }
  }
}
