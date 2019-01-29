using System;

namespace VistaDB.VistaDBTypes
{
  public class VistaDBDateTime : VistaDBValue
  {
    public VistaDBDateTime()
    {
    }

    public VistaDBDateTime(DateTime val)
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
        base.Value = value == null ? value : (object) (DateTime) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.DateTime;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (DateTime);
      }
    }

    public DateTime GetValueOrDefault()
    {
      if (this.HasValue)
        return (DateTime) this.Value;
      return DateTime.MinValue;
    }

    public DateTime GetValueOrDefault(DateTime defaultValue)
    {
      if (this.HasValue)
        return (DateTime) this.Value;
      return defaultValue;
    }

    public DateTime GetValueOrDefault(VistaDBDateTime defaultValue)
    {
      if (this.HasValue)
        return (DateTime) this.Value;
      return (DateTime) defaultValue.Value;
    }
  }
}
