using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class MoneyColumn : DecimalColumn
  {
    public static readonly int ScaleFactor = 10000;
    private static readonly Decimal MaxCurrency = new Decimal(long.MaxValue) / (Decimal)ScaleFactor;
    private static readonly Decimal MinCurrency = new Decimal(long.MinValue) / (Decimal)ScaleFactor;
    private static readonly int MoneySize = 8;

    internal MoneyColumn()
      : base(VistaDBType.Money, MoneySize)
    {
    }

    internal MoneyColumn(Decimal val)
      : this()
    {
      Value = (object) val;
    }

    internal MoneyColumn(MoneyColumn column)
      : base((DecimalColumn) column)
    {
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) TestDynamicRange(Truncate((Decimal) value));
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object)MaxCurrency;
      }
    }

    internal override object DummyNull
    {
      get
      {
        return (object)MinCurrency;
      }
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((ulong) Decimal.ToInt64((Decimal) Value * (Decimal)ScaleFactor), buffer, offset, MoneySize);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      long int64 = BitConverter.ToInt64(buffer, offset);
      offset += MoneySize;
      val = (object) (new Decimal(int64) / (Decimal)ScaleFactor);
      return offset;
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new MoneyColumn(this);
    }

    private Decimal Truncate(Decimal currency)
    {
      return new Decimal(Decimal.ToInt64(currency * (Decimal)ScaleFactor)) / (Decimal)ScaleFactor;
    }

    private Decimal TestDynamicRange(Decimal currency)
    {
      if (currency.CompareTo(MaxCurrency) > 0 || currency.CompareTo(MinCurrency) < 0)
        throw new VistaDBException(300, "Money = " + currency.ToString());
      return currency;
    }
  }
}
