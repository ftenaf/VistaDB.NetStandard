namespace VistaDB.VistaDBTypes
{
  public class VistaDBInt32 : VistaDBValue
  {
    public VistaDBInt32()
    {
    }

    public VistaDBInt32(int val)
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
        base.Value = value == null ? value : (object) (int) value;
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
      if (this.HasValue)
        return (int) this.Value;
      return 0;
    }

    public int GetValueOrDefault(int defaultValue)
    {
      if (this.HasValue)
        return (int) this.Value;
      return defaultValue;
    }

    public int GetValueOrDefault(VistaDBInt32 defaultValue)
    {
      if (this.HasValue)
        return (int) this.Value;
      return (int) defaultValue.Value;
    }
  }
}
