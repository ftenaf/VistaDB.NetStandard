using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class SmallMoneyColumn : DecimalColumn
  {
    public static readonly int ScaleFactor = 10000;
    private static readonly Decimal MaxCurrency = new Decimal(int.MaxValue) / (Decimal) SmallMoneyColumn.ScaleFactor;
    private static readonly Decimal MinCurrency = new Decimal(int.MinValue) / (Decimal) SmallMoneyColumn.ScaleFactor;
    private static readonly int SmallMoneySize = 4;

    internal SmallMoneyColumn()
      : base(VistaDBType.SmallMoney, SmallMoneyColumn.SmallMoneySize)
    {
    }

    internal SmallMoneyColumn(Decimal val)
      : this()
    {
      this.Value = (object) val;
    }

    internal SmallMoneyColumn(SmallMoneyColumn column)
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
        return (object) SmallMoneyColumn.MaxCurrency;
      }
    }

    internal override object DummyNull
    {
      get
      {
        return (object) SmallMoneyColumn.MinCurrency;
      }
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((uint) Decimal.ToInt32((Decimal) this.Value * (Decimal) SmallMoneyColumn.ScaleFactor), buffer, offset, SmallMoneyColumn.SmallMoneySize);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int int32 = BitConverter.ToInt32(buffer, offset);
      offset += SmallMoneyColumn.SmallMoneySize;
      this.val = (object) (new Decimal(int32) / (Decimal) SmallMoneyColumn.ScaleFactor);
      return offset;
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new SmallMoneyColumn(this);
    }

    private Decimal Truncate(Decimal currency)
    {
      return new Decimal(Decimal.ToInt32(currency * (Decimal) SmallMoneyColumn.ScaleFactor)) / (Decimal) SmallMoneyColumn.ScaleFactor;
    }

    private Decimal TestDynamicRange(Decimal currency)
    {
      if (currency.CompareTo(SmallMoneyColumn.MaxCurrency) > 0 || currency.CompareTo(SmallMoneyColumn.MinCurrency) < 0)
        throw new VistaDBException(300, "SmallMoney = " + currency.ToString());
      return currency;
    }
  }
}
