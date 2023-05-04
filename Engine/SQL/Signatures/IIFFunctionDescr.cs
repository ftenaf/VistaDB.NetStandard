namespace VistaDB.Engine.SQL.Signatures
{
  internal class IIFFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new IIFFunction(parser);
    }
  }
}
