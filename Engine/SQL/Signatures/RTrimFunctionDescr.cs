namespace VistaDB.Engine.SQL.Signatures
{
  internal class RTrimFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new RTrimFunction(parser);
    }
  }
}
