namespace VistaDB.VistaDBTypes
{
  public class VistaDBDouble : VistaDBValue
  {
    public VistaDBDouble()
    {
    }

    public VistaDBDouble(double val)
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
      if (HasValue)
        return (double) Value;
      return 0.0;
    }

    public double GetValueOrDefault(double defaultValue)
    {
      if (HasValue)
        return (double) Value;
      return defaultValue;
    }

    public double GetValueOrDefault(VistaDBDouble defaultValue)
    {
      if (HasValue)
        return (double) Value;
      return (double) defaultValue.Value;
    }
  }
}
