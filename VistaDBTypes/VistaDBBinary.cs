namespace VistaDB.VistaDBTypes
{
  public class VistaDBBinary : VistaDBValue
  {
    public VistaDBBinary()
    {
    }

    public VistaDBBinary(byte[] val)
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
        base.Value = value == null ? value : (object) (byte[]) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.VarBinary;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (byte[]);
      }
    }

    public byte[] GetValueOrDefault()
    {
      if (HasValue)
        return (byte[]) Value;
      return new byte[0];
    }

    public byte[] GetValueOrDefault(byte[] defaultValue)
    {
      if (HasValue)
        return (byte[]) Value;
      return defaultValue;
    }

    public byte[] GetValueOrDefault(VistaDBBinary defaultValue)
    {
      if (HasValue)
        return (byte[]) Value;
      return (byte[]) defaultValue.Value;
    }
  }
}
