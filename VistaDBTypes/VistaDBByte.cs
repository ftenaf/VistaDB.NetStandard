namespace VistaDB.VistaDBTypes
{
  public class VistaDBByte : VistaDBValue
  {
    public VistaDBByte()
    {
    }

    public VistaDBByte(byte val)
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
        base.Value = value == null ? value : (byte)value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.TinyInt;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (byte);
      }
    }

    public byte GetValueOrDefault()
    {
      if (HasValue)
        return (byte) Value;
      return 0;
    }

    public byte GetValueOrDefault(byte defaultValue)
    {
      if (HasValue)
        return (byte) Value;
      return defaultValue;
    }

    public byte GetValueOrDefault(VistaDBByte defaultValue)
    {
      if (HasValue)
        return (byte) Value;
      return (byte) defaultValue.Value;
    }
  }
}
