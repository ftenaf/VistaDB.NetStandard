namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class Priority2Descr : IOperatorDescr
  {
    public abstract Signature CreateSignature(Signature leftSignature, SQLParser parser);

    public int Priority
    {
      get
      {
        return 2;
      }
    }
  }
}
