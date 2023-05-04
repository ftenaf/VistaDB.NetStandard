namespace VistaDB.VistaDBTypes
{
  public class VistaDBInt32 : VistaDBValue
  {
    public VistaDBInt32()
    {
    }

    public VistaDBInt32(int val)
    {
      Value = val;
    }

    public override object Value
    {
      get
      {
        return base.Value;
      }
      set
      {
        base.Value = value == null ? value : (int)value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.Int;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (int);
      }
    }

    public int GetValueOrDefault()
    {
      if (HasValue)
        return (int) Value;
      return 0;
    }

    public int GetValueOrDefault(int defaultValue)
    {
      if (HasValue)
        return (int) Value;
      return defaultValue;
    }

    public int GetValueOrDefault(VistaDBInt32 defaultValue)
    {
      if (HasValue)
        return (int) Value;
      return (int) defaultValue.Value;
    }
  }
}
