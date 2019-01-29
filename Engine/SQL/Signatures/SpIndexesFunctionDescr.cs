namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpIndexesFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new SpIndexesFuncion(parser);
    }
  }
}
