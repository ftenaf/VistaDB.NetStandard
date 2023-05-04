using System;

namespace VistaDB.VistaDBTypes
{
  public class VistaDBGuid : VistaDBValue
  {
    public VistaDBGuid()
    {
    }

    public VistaDBGuid(Guid val)
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
        base.Value = value == null ? value : (object) (Guid) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.UniqueIdentifier;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (Guid);
      }
    }

    public Guid GetValueOrDefault()
    {
      if (HasValue)
        return (Guid) Value;
      return new Guid();
    }

    public Guid GetValueOrDefault(Guid defaultValue)
    {
      if (HasValue)
        return (Guid) Value;
      return defaultValue;
    }

    public Guid GetValueOrDefault(VistaDBGuid defaultValue)
    {
      if (HasValue)
        return (Guid) Value;
      return (Guid) defaultValue.Value;
    }
  }
}
