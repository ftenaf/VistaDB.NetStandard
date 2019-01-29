namespace VistaDB.VistaDBTypes
{
  public class VistaDBByte : VistaDBValue
  {
    public VistaDBByte()
    {
    }

    public VistaDBByte(byte val)
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
        base.Value = value == null ? value : (object) (byte) value;
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
      if (this.HasValue)
        return (byte) this.Value;
      return 0;
    }

    public byte GetValueOrDefault(byte defaultValue)
    {
      if (this.HasValue)
        return (byte) this.Value;
      return defaultValue;
    }

    public byte GetValueOrDefault(VistaDBByte defaultValue)
    {
      if (this.HasValue)
        return (byte) this.Value;
      return (byte) defaultValue.Value;
    }
  }
}
