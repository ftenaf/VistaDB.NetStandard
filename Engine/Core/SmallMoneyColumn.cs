using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class SmallMoneyColumn : DecimalColumn
  {
    public static readonly int ScaleFactor = 10000;
    private static readonly Decimal MaxCurrency = new Decimal(int.MaxValue) / ScaleFactor;
    private static readonly Decimal MinCurrency = new Decimal(int.MinValue) / ScaleFactor;
    private static readonly int SmallMoneySize = 4;

    internal SmallMoneyColumn()
      : base(VistaDBType.SmallMoney, SmallMoneySize)
    {
    }

    internal SmallMoneyColumn(Decimal val)
      : this()
    {
      Value = val;
    }

    internal SmallMoneyColumn(SmallMoneyColumn column)
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
      return VdbBitConverter.GetBytes((uint) Decimal.ToInt32((Decimal) Value * ScaleFactor), buffer, offset, SmallMoneySize);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int int32 = BitConverter.ToInt32(buffer, offset);
      offset += SmallMoneySize;
      val = new Decimal(int32) / ScaleFactor;
      return offset;
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new SmallMoneyColumn(this);
    }

    private Decimal Truncate(Decimal currency)
    {
      return new Decimal(Decimal.ToInt32(currency * ScaleFactor)) / ScaleFactor;
    }

    private Decimal TestDynamicRange(Decimal currency)
    {
      if (currency.CompareTo(MaxCurrency) > 0 || currency.CompareTo(MinCurrency) < 0)
        throw new VistaDBException(300, "SmallMoney = " + currency.ToString());
      return currency;
    }
  }
}
