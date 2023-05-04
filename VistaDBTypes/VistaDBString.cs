namespace VistaDB.VistaDBTypes
{
  public class VistaDBString : VistaDBValue
  {
    public VistaDBString()
    {
    }

    public VistaDBString(string val)
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
        base.Value = value == null ? null : (object) value.ToString();
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.NChar;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (string);
      }
    }

    public string GetValueOrDefault()
    {
      if (HasValue)
        return (string) Value;
      return string.Empty;
    }

    public string GetValueOrDefault(string defaultValue)
    {
      if (HasValue)
        return (string) Value;
      return defaultValue;
    }

    public string GetValueOrDefault(VistaDBString defaultValue)
    {
      if (HasValue)
        return (string) Value;
      return (string) defaultValue.Value;
    }
  }
}
