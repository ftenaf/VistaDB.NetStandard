namespace VistaDB.VistaDBTypes
{
  public abstract class VistaDBValue : IVistaDBValue
  {
    private object val;

    public virtual object Value
    {
      get
      {
        return val;
      }
      set
      {
        val = value;
      }
    }

    public bool HasValue
    {
      get
      {
        return val != null;
      }
    }

    public bool IsNull
    {
      get
      {
        return val == null;
      }
    }

    public virtual VistaDBType Type
    {
      get
      {
        return VistaDBType.Unknown;
      }
    }

    public abstract System.Type SystemType { get; }
  }
}
