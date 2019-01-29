using System;

namespace VistaDB.Engine.Core
{
  internal class TinyIntColumn : Row.Column
  {
    private static readonly int ByteSize = 1;

    internal static byte CustValue(Row.Column col)
    {
      if (col.InternalType == VistaDBType.NChar)
        return byte.Parse((string) col.Value);
      return (byte) col.Value;
    }

    internal TinyIntColumn()
      : base((object) null, VistaDBType.TinyInt, TinyIntColumn.ByteSize)
    {
    }

    internal TinyIntColumn(byte val)
      : base((object) val, VistaDBType.TinyInt, TinyIntColumn.ByteSize)
    {
    }

    internal TinyIntColumn(TinyIntColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) (byte) 0;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) byte.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (byte);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) (byte) value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new TinyIntColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      buffer[offset] = (byte) this.val;
      return offset + TinyIntColumn.ByteSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      this.val = (object) buffer[offset];
      return offset + TinyIntColumn.ByteSize;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) (byte) this.Value - (long) (byte) col.Value;
    }

    protected override Row.Column DoUnaryMinus()
    {
      this.Value = (object) -(byte) this.Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value - (uint) TinyIntColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value + (uint) TinyIntColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value * (uint) TinyIntColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value / (uint) TinyIntColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      this.Value = (object) (byte) ((uint) TinyIntColumn.CustValue(numerator) / (uint) (byte) this.Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value % (uint) TinyIntColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      this.Value = (object) (byte) ((uint) TinyIntColumn.CustValue(numerator) % (uint) (byte) this.Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      this.Value = (object) ~(byte) this.Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value & (uint) TinyIntColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value | (uint) TinyIntColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      this.Value = (object) (byte) ((uint) (byte) this.Value ^ (uint) TinyIntColumn.CustValue(denominator));
      return (Row.Column) this;
    }
  }
}
