using System;

namespace VistaDB.VistaDBTypes
{
  public class VistaDBDecimal : VistaDBValue
  {
    public VistaDBDecimal()
    {
    }

    public VistaDBDecimal(Decimal val)
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
        base.Value = value == null ? value : (Decimal)value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.Decimal;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (Decimal);
      }
    }

    public Decimal GetValueOrDefault()
    {
      if (HasValue)
        return (Decimal) Value;
      return new Decimal(0);
    }

    public Decimal GetValueOrDefault(Decimal defaultValue)
    {
      if (HasValue)
        return (Decimal) Value;
      return defaultValue;
    }

    public Decimal GetValueOrDefault(VistaDBDecimal defaultValue)
    {
      if (HasValue)
        return (Decimal) Value;
      return (Decimal) defaultValue.Value;
    }
  }
}
