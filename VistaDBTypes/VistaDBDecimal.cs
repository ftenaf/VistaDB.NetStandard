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
        base.Value = value == null ? value : (object) (Decimal) value;
      }
    }

    public override VistaDBType Type
    {
      get
      {
        return VistaDBType.Decimal;
      }
    }

    public override System.Type SystemType
    {
      get
      {
        return typeof (Decimal);
      }
    }

    public Decimal GetValueOrDefault()
    {
      if (this.HasValue)
        return (Decimal) this.Value;
      return new Decimal(0);
    }

    public Decimal GetValueOrDefault(Decimal defaultValue)
    {
      if (this.HasValue)
        return (Decimal) this.Value;
      return defaultValue;
    }

    public Decimal GetValueOrDefault(VistaDBDecimal defaultValue)
    {
      if (this.HasValue)
        return (Decimal) this.Value;
      return (Decimal) defaultValue.Value;
    }
  }
}
