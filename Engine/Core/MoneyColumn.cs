using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class MoneyColumn : DecimalColumn
  {
    public static readonly int ScaleFactor = 10000;
    private static readonly Decimal MaxCurrency = new Decimal(long.MaxValue) / ScaleFactor;
    private static readonly Decimal MinCurrency = new Decimal(long.MinValue) / ScaleFactor;
    private static readonly int MoneySize = 8;

    internal MoneyColumn()
      : base(VistaDBType.Money, MoneySize)
    {
    }

    internal MoneyColumn(Decimal val)
      : this()
    {
      Value = val;
    }

    internal MoneyColumn(MoneyColumn column)
      : base(column)
    {
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : TestDynamicRange(Truncate((Decimal)value));
      }
    }

    public override object MaxValue
    {
      get
      {
        return MaxCurrency;
      }
    }

    internal override object DummyNull
    {
      get
      {
        return MinCurrency;
      }
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((ulong) Decimal.ToInt64((Decimal) Value * ScaleFactor), buffer, offset, MoneySize);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      long int64 = BitConverter.ToInt64(buffer, offset);
      offset += MoneySize;
      val = new Decimal(int64) / ScaleFactor;
      return offset;
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new MoneyColumn(this);
    }

    private Decimal Truncate(Decimal currency)
    {
      return new Decimal(Decimal.ToInt64(currency * ScaleFactor)) / ScaleFactor;
    }

    private Decimal TestDynamicRange(Decimal currency)
    {
      if (currency.CompareTo(MaxCurrency) > 0 || currency.CompareTo(MinCurrency) < 0)
        throw new VistaDBException(300, "Money = " + currency.ToString());
      return currency;
    }
  }
}
