namespace VistaDB.VistaDBTypes
{
  public class VistaDBSingle : VistaDBValue
  {
    public VistaDBSingle()
    {
    }

    public VistaDBSingle(float val)
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
        base.Value = value == null ? value : (object) (float) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.Real;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (float);
      }
    }

    public float GetValueOrDefault()
    {
      if (HasValue)
        return (float) Value;
      return 0.0f;
    }

    public float GetValueOrDefault(float defaultValue)
    {
      if (HasValue)
        return (float) Value;
      return defaultValue;
    }

    public float GetValueOrDefault(VistaDBSingle defaultValue)
    {
      if (HasValue)
        return (float) Value;
      return (float) defaultValue.Value;
    }
  }
}
