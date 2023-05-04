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
      : base(null, VistaDBType.TinyInt, ByteSize)
    {
    }

    internal TinyIntColumn(byte val)
      : base(val, VistaDBType.TinyInt, ByteSize)
    {
    }

    internal TinyIntColumn(TinyIntColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (byte)0;
      }
    }

    public override object MaxValue
    {
      get
      {
        return byte.MaxValue;
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
        base.Value = value == null ? value : (byte)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new TinyIntColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      buffer[offset] = (byte) val;
      return offset + ByteSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = buffer[offset];
      return offset + ByteSize;
    }

    protected override long Collate(Row.Column col)
    {
      return (byte)Value - (long) (byte) col.Value;
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = -(byte)Value;
      return this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (byte)((byte)Value - (uint)CustValue(col));
      return this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (byte)((byte)Value + (uint)CustValue(col));
      return this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (byte)((byte)Value * (uint)CustValue(col));
      return this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (byte)((byte)Value / (uint)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = (byte)(CustValue(numerator) / (uint)(byte)Value);
      return this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (byte)((byte)Value % (uint)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = (byte)(CustValue(numerator) % (uint)(byte)Value);
      return this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      Value = ~(byte)Value;
      return this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      Value = (byte)((byte)Value & (uint)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      Value = (byte)((byte)Value | (uint)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      Value = (byte)((byte)Value ^ (uint)CustValue(denominator));
      return this;
    }
  }
}
