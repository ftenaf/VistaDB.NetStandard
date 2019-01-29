namespace VistaDB.VistaDBTypes
{
  public class VistaDBInt64 : VistaDBValue
  {
    public VistaDBInt64()
    {
    }

    public VistaDBInt64(long val)
    {
      this.Value = (object) val;
    }

    public override object Value
    {
      get
      {
        return base.Value;
      }
      set
      {
        base.Value = value == null ? value : (object) (long) value;
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
      if (this.HasValue)
        return (long) this.Value;
      return 0;
    }

    public long GetValueOrDefault(long defaultValue)
    {
      if (this.HasValue)
        return (long) this.Value;
      return defaultValue;
    }

    public long GetValueOrDefault(VistaDBInt64 defaultValue)
    {
      if (this.HasValue)
        return (long) this.Value;
      return (long) defaultValue.Value;
    }
  }
}
