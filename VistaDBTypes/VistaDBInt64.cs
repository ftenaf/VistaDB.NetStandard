namespace VistaDB.VistaDBTypes
{
  public class VistaDBInt64 : VistaDBValue
  {
    public VistaDBInt64()
    {
    }

    public VistaDBInt64(long val)
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
        base.Value = value == null ? value : (long)value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.BigInt;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (long);
      }
    }

    public long GetValueOrDefault()
    {
      if (HasValue)
        return (long) Value;
      return 0;
    }

    public long GetValueOrDefault(long defaultValue)
    {
      if (HasValue)
        return (long) Value;
      return defaultValue;
    }

    public long GetValueOrDefault(VistaDBInt64 defaultValue)
    {
      if (HasValue)
        return (long) Value;
      return (long) defaultValue.Value;
    }
  }
}
