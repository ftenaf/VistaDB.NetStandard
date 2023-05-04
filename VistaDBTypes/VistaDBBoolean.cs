namespace VistaDB.VistaDBTypes
{
  public class VistaDBBoolean : VistaDBValue
  {
    public VistaDBBoolean()
    {
    }

    public VistaDBBoolean(bool val)
    {
      Value = (object) val;
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
      if (HasValue)
        return (bool) Value;
      return false;
    }

    public bool GetValueOrDefault(bool defaultValue)
    {
      if (HasValue)
        return (bool) Value;
      return defaultValue;
    }

    public bool GetValueOrDefault(VistaDBBoolean defaultValue)
    {
      if (HasValue)
        return (bool) Value;
      return (bool) defaultValue.Value;
    }
  }
}
