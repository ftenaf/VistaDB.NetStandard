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
      : base((object) null, VistaDBType.TinyInt, ByteSize)
    {
    }

    internal TinyIntColumn(byte val)
      : base((object) val, VistaDBType.TinyInt, ByteSize)
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
      buffer[offset] = (byte) val;
      return offset + ByteSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = (object) buffer[offset];
      return offset + ByteSize;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) (byte) Value - (long) (byte) col.Value;
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = (object) -(byte) Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (object) (byte) ((uint) (byte) Value - (uint)CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (object) (byte) ((uint) (byte) Value + (uint)CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (object) (byte) ((uint) (byte) Value * (uint)CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (object) (byte) ((uint) (byte) Value / (uint)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = (object) (byte) ((uint)CustValue(numerator) / (uint) (byte) Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (object) (byte) ((uint) (byte) Value % (uint)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = (object) (byte) ((uint)CustValue(numerator) % (uint) (byte) Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      Value = (object) ~(byte) Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      Value = (object) (byte) ((uint) (byte) Value & (uint)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      Value = (object) (byte) ((uint) (byte) Value | (uint)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      Value = (object) (byte) ((uint) (byte) Value ^ (uint)CustValue(denominator));
      return (Row.Column) this;
    }
  }
}
