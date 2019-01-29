namespace VistaDB.VistaDBTypes
{
  public class VistaDBString : VistaDBValue
  {
    public VistaDBString()
    {
    }

    public VistaDBString(string val)
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
        base.Value = value == null ? (object) (string) null : (object) value.ToString();
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
      if (this.HasValue)
        return (string) this.Value;
      return string.Empty;
    }

    public string GetValueOrDefault(string defaultValue)
    {
      if (this.HasValue)
        return (string) this.Value;
      return defaultValue;
    }

    public string GetValueOrDefault(VistaDBString defaultValue)
    {
      if (this.HasValue)
        return (string) this.Value;
      return (string) defaultValue.Value;
    }
  }
}
