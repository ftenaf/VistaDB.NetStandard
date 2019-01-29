namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class Priority0Descr : IOperatorDescr
  {
    public abstract Signature CreateSignature(Signature leftSignature, SQLParser parser);

    public int Priority
    {
      get
      {
        return 0;
      }
    }
  }
}
