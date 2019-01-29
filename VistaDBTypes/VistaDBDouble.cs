namespace VistaDB.VistaDBTypes
{
  public class VistaDBDouble : VistaDBValue
  {
    public VistaDBDouble()
    {
    }

    public VistaDBDouble(double val)
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
        base.Value = value == null ? value : (object) (double) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.Float;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (double);
      }
    }

    public double GetValueOrDefault()
    {
      if (this.HasValue)
        return (double) this.Value;
      return 0.0;
    }

    public double GetValueOrDefault(double defaultValue)
    {
      if (this.HasValue)
        return (double) this.Value;
      return defaultValue;
    }

    public double GetValueOrDefault(VistaDBDouble defaultValue)
    {
      if (this.HasValue)
        return (double) this.Value;
      return (double) defaultValue.Value;
    }
  }
}
