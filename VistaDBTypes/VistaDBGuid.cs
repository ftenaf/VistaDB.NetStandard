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

    public override System.Type SystemType
    {
      get
      {
        return typeof (Guid);
      }
    }

    public Guid GetValueOrDefault()
    {
      if (this.HasValue)
        return (Guid) this.Value;
      return new Guid();
    }

    public Guid GetValueOrDefault(Guid defaultValue)
    {
      if (this.HasValue)
        return (Guid) this.Value;
      return defaultValue;
    }

    public Guid GetValueOrDefault(VistaDBGuid defaultValue)
    {
      if (this.HasValue)
        return (Guid) this.Value;
      return (Guid) defaultValue.Value;
    }
  }
}
