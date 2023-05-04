namespace VistaDB.VistaDBTypes
{
  public class VistaDBInt16 : VistaDBValue
  {
    public VistaDBInt16()
    {
    }

    public VistaDBInt16(short val)
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
        base.Value = value == null ? value : (object) (short) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.SmallInt;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (short);
      }
    }

    public short GetValueOrDefault()
    {
      if (HasValue)
        return (short) Value;
      return short.MinValue;
    }

    public short GetValueOrDefault(short defaultValue)
    {
      if (HasValue)
        return (short) Value;
      return defaultValue;
    }

    public short GetValueOrDefault(VistaDBInt16 defaultValue)
    {
      if (HasValue)
        return (short) Value;
      return (short) defaultValue.Value;
    }
  }
}
