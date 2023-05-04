using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class BigIntColumn : Row.Column
  {
    private static readonly int LongSize = 8;
    private byte[] compressingArray;

    internal static long CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return long.Parse((string) col.Value);
        case VistaDBType.TinyInt:
          return (byte)col.Value;
        case VistaDBType.SmallInt:
          return (short)col.Value;
        case VistaDBType.Int:
          return (int)col.Value;
        default:
          return (long) col.Value;
      }
    }

    internal BigIntColumn()
      : base(null, VistaDBType.BigInt, LongSize)
    {
    }

    internal BigIntColumn(long val)
      : base(val, VistaDBType.BigInt, LongSize)
    {
    }

    internal BigIntColumn(BigIntColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return long.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return long.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (long);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (long)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new BigIntColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      ulong difference;
      int len;
      if (precedenceColumn != null)
      {
        bool inverted;
        len = CalcPackedLength((long) val - (long) precedenceColumn.Value, out inverted, out difference);
        if (len > LongSize)
        {
          --len;
          buffer[offset++] = (byte) len;
        }
        else
        {
          ulong num = difference << 5;
          if (inverted)
            num |= 16UL;
          difference = num | (uint)(len - 1);
        }
      }
      else
      {
        difference = (ulong) (long) Value;
        len = LongSize;
      }
      return VdbBitConverter.GetBytes(difference, buffer, offset, len);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      if (precedenceColumn == null)
      {
        val = BitConverter.ToInt64(buffer, offset);
        return offset + LongSize;
      }
      int length = (buffer[offset] & 15) + 1;
      if (length > LongSize)
      {
        val = (long)precedenceColumn.Value + BitConverter.ToInt64(buffer, ++offset);
        return offset + LongSize;
      }
      if (this.compressingArray == null)
        this.compressingArray = new byte[LongSize];
      byte[] compressingArray = this.compressingArray;
      Array.Clear(compressingArray, 0, LongSize);
      Array.Copy(buffer, offset, compressingArray, 0, length);
      ulong num = BitConverter.ToUInt64(compressingArray, 0) >> 5;
      if ((buffer[offset] & 16) == 16)
        num = ~num;
      val = (long)precedenceColumn.Value + (long)num;
      return offset + length;
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      if (precedenceColumn == null || precedenceColumn.IsNull)
        return base.GetBufferLength(precedenceColumn);
      if (!IsNull)
      {
        bool inverted;
        ulong difference;
        return CalcPackedLength((long) val - (long) precedenceColumn.Value, out inverted, out difference);
      }
      return 0;
    }

    internal override int GetLengthCounterWidth(Row.Column precedenceColumn)
    {
      return !(precedenceColumn == null) && !precedenceColumn.IsNull ? 1 : 0;
    }

    internal override int ReadVarLength(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      if (!(precedenceColumn == null) && !precedenceColumn.IsNull)
        return buffer[offset] & 15;
      return 0;
    }

    protected override long Collate(Row.Column col)
    {
      long num1 = (long) Value;
      long num2 = (long) col.Value;
      return num1 > num2 ? 1L : (num1 < num2 ? -1L : 0L);
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = -(long)Value;
      return this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (long)Value - CustValue(col);
      return this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (long)Value + CustValue(col);
      return this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (long)Value * CustValue(col);
      return this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (long)Value / CustValue(denominator);
      return this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = CustValue(numerator) / (long)Value;
      return this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (long)Value % CustValue(denominator);
      return this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = CustValue(numerator) % (long)Value;
      return this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      Value = ~(long)Value;
      return this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      Value = (long)Value & CustValue(denominator);
      return this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      Value = (long)Value | CustValue(denominator);
      return this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      Value = (long)Value ^ CustValue(denominator);
      return this;
    }

    private int CalcPackedLength(long diff, out bool inverted, out ulong difference)
    {
      difference = (ulong) diff;
      inverted = ((long) difference & -576460752303423488L) == -576460752303423488L;
      if (inverted)
        difference = ~difference;
      if (difference <= 7UL)
        return 1;
      if (difference <= 2047UL)
        return 2;
      if (difference <= 524287UL)
        return 3;
      if (difference <= 134217727UL)
        return 4;
      if (difference <= 34359738367UL)
        return 5;
      if (difference <= 8796093022207UL)
        return 6;
      if (difference <= 2251799813685247UL)
        return 7;
      return difference <= 576460752303423487UL ? 8 : 9;
    }
  }
}
