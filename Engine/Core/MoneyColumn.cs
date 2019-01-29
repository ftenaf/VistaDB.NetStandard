using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class MoneyColumn : DecimalColumn
  {
    public static readonly int ScaleFactor = 10000;
    private static readonly Decimal MaxCurrency = new Decimal(long.MaxValue) / (Decimal) MoneyColumn.ScaleFactor;
    private static readonly Decimal MinCurrency = new Decimal(long.MinValue) / (Decimal) MoneyColumn.ScaleFactor;
    private static readonly int MoneySize = 8;

    internal MoneyColumn()
      : base(VistaDBType.Money, MoneyColumn.MoneySize)
    {
    }

    internal MoneyColumn(Decimal val)
      : this()
    {
      this.Value = (object) val;
    }

    internal MoneyColumn(MoneyColumn column)
      : base((DecimalColumn) column)
    {
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) this.TestDynamicRange(this.Truncate((Decimal) value));
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) MoneyColumn.MaxCurrency;
      }
    }

    internal override object DummyNull
    {
      get
      {
        return (object) MoneyColumn.MinCurrency;
      }
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((ulong) Decimal.ToInt64((Decimal) this.Value * (Decimal) MoneyColumn.ScaleFactor), buffer, offset, MoneyColumn.MoneySize);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      long int64 = BitConverter.ToInt64(buffer, offset);
      offset += MoneyColumn.MoneySize;
      this.val = (object) (new Decimal(int64) / (Decimal) MoneyColumn.ScaleFactor);
      return offset;
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new MoneyColumn(this);
    }

    private Decimal Truncate(Decimal currency)
    {
      return new Decimal(Decimal.ToInt64(currency * (Decimal) MoneyColumn.ScaleFactor)) / (Decimal) MoneyColumn.ScaleFactor;
    }

    private Decimal TestDynamicRange(Decimal currency)
    {
      if (currency.CompareTo(MoneyColumn.MaxCurrency) > 0 || currency.CompareTo(MoneyColumn.MinCurrency) < 0)
        throw new VistaDBException(300, "Money = " + currency.ToString());
      return currency;
    }
  }
}
