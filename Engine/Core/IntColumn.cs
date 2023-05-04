using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class IntColumn : Row.Column
  {
    private static readonly int Int32Size = 4;
    private byte[] compressingArray;

    internal static int CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return int.Parse((string) col.Value);
        case VistaDBType.TinyInt:
          return (int) (byte) col.Value;
        case VistaDBType.SmallInt:
          return (int) (short) col.Value;
        default:
          return (int) col.Value;
      }
    }

    internal IntColumn()
      : base((object) null, VistaDBType.Int, Int32Size)
    {
    }

    internal IntColumn(int val)
      : base((object) val, VistaDBType.Int, Int32Size)
    {
    }

    internal IntColumn(IntColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) int.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) int.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (int);
      }
    }

    public override object Value
    {
      set
      {
        object obj = value;
        if (obj != null && !(obj is int))
          obj = Convert.ChangeType(obj, typeof (int), (IFormatProvider) null);
        base.Value = obj == null ? obj : (object) (int) obj;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new IntColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      uint difference;
      int len;
      if (precedenceColumn != (Row.Column) null)
      {
        bool inverted;
        len = CalcPackedLength((int) val - (int) precedenceColumn.Value, out inverted, out difference);
        if (len > Int32Size)
        {
          --len;
          buffer[offset++] = (byte) len;
        }
        else
        {
          uint num = difference << 4;
          if (inverted)
            num |= 8U;
          difference = num | (uint) (len - 1);
        }
      }
      else
      {
        difference = (uint) (int) Value;
        len = Int32Size;
      }
      return VdbBitConverter.GetBytes(difference, buffer, offset, len);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      if (precedenceColumn == (Row.Column) null)
      {
        val = (object) BitConverter.ToInt32(buffer, offset);
        return offset + Int32Size;
      }
      int length = ((int) buffer[offset] & 7) + 1;
      if (length > Int32Size)
      {
        val = (object) ((int) precedenceColumn.Value + BitConverter.ToInt32(buffer, ++offset));
        return offset + Int32Size;
      }
      if (this.compressingArray == null)
        this.compressingArray = new byte[Int32Size];
      byte[] compressingArray = this.compressingArray;
      Array.Clear((Array) compressingArray, 0, length);
      Array.Copy((Array) buffer, offset, (Array) compressingArray, 0, length);
      uint num = BitConverter.ToUInt32(compressingArray, 0) >> 4;
      if (((int) buffer[offset] & 8) == 8)
        num = ~num;
      val = (object) ((int) precedenceColumn.Value + (int) num);
      return offset + length;
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      if (precedenceColumn == (Row.Column) null || precedenceColumn.IsNull)
        return base.GetBufferLength(precedenceColumn);
      if (!IsNull)
      {
        bool inverted;
        uint difference;
        return CalcPackedLength((int) val - (int) precedenceColumn.Value, out inverted, out difference);
      }
      return 0;
    }

    internal override int GetLengthCounterWidth(Row.Column precedenceColumn)
    {
      return !(precedenceColumn == (Row.Column) null) && !precedenceColumn.IsNull ? 1 : 0;
    }

    internal override int ReadVarLength(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      if (!(precedenceColumn == (Row.Column) null) && !precedenceColumn.IsNull)
        return (int) buffer[offset] & 7;
      return 0;
    }

    protected override long Collate(Row.Column col)
    {
      int num1 = (int) Value;
      int num2 = (int) col.Value;
      return num1 > num2 ? 1L : (num1 < num2 ? -1L : 0L);
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = (object) -(int) Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (object) ((int) Value - CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (object) ((int) Value + CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (object) ((int) Value * CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (object) ((int) Value / CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = (object) (CustValue(numerator) / (int) Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (object) ((int) Value % CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = (object) (CustValue(numerator) % (int) Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      Value = (object) ~(int) Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      Value = (object) ((int) Value & CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      Value = (object) ((int) Value | CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      Value = (object) ((int) Value ^ CustValue(denominator));
      return (Row.Column) this;
    }

    private int CalcPackedLength(int diff, out bool inverted, out uint difference)
    {
      difference = (uint) diff;
      inverted = ((int) difference & -268435456) == -268435456;
      if (inverted)
        difference = ~difference;
      if (difference <= 15U)
        return 1;
      if (difference <= 4095U)
        return 2;
      if (difference <= 1048575U)
        return 3;
      return difference <= 268435455U ? 4 : 5;
    }
  }
}
