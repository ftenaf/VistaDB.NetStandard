namespace VistaDB.VistaDBTypes
{
  public class VistaDBBoolean : VistaDBValue
  {
    public VistaDBBoolean()
    {
    }

    public VistaDBBoolean(bool val)
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
        base.Value = value == null ? value : (object) (bool) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.Bit;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (bool);
      }
    }

    public bool GetValueOrDefault()
    {
      if (this.HasValue)
        return (bool) this.Value;
      return false;
    }

    public bool GetValueOrDefault(bool defaultValue)
    {
      if (this.HasValue)
        return (bool) this.Value;
      return defaultValue;
    }

    public bool GetValueOrDefault(VistaDBBoolean defaultValue)
    {
      if (this.HasValue)
        return (bool) this.Value;
      return (bool) defaultValue.Value;
    }
  }
}
