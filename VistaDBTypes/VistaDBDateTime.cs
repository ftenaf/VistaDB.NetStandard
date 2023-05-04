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

    public override Type SystemType
    {
      get
      {
        return typeof (DateTime);
      }
    }

    public DateTime GetValueOrDefault()
    {
      if (HasValue)
        return (DateTime) Value;
      return DateTime.MinValue;
    }

    public DateTime GetValueOrDefault(DateTime defaultValue)
    {
      if (HasValue)
        return (DateTime) Value;
      return defaultValue;
    }

    public DateTime GetValueOrDefault(VistaDBDateTime defaultValue)
    {
      if (HasValue)
        return (DateTime) Value;
      return (DateTime) defaultValue.Value;
    }
  }
}
