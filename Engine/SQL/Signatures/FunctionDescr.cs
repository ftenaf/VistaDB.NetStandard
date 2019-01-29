namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class FunctionDescr
  {
    public abstract Signature CreateSignature(SQLParser parser);
  }
}
