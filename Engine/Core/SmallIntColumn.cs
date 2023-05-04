using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class SmallIntColumn : Row.Column
  {
    private static readonly int Int16Size = 2;

    internal static short CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return short.Parse((string) col.Value);
        case VistaDBType.TinyInt:
          return (byte)col.Value;
        default:
          return (short) col.Value;
      }
    }

    internal SmallIntColumn()
      : base(null, VistaDBType.SmallInt, Int16Size)
    {
    }

    internal SmallIntColumn(short val)
      : base(val, VistaDBType.SmallInt, Int16Size)
    {
    }

    internal SmallIntColumn(SmallIntColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return short.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return short.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (short);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (short)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new SmallIntColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((ushort) (short) Value, buffer, offset, Int16Size);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = BitConverter.ToInt16(buffer, offset);
      return offset + Int16Size;
    }

    protected override long Collate(Row.Column col)
    {
      return (short)Value - (short)col.Value;
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = -(short)Value;
      return this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (short)((short)Value - CustValue(col));
      return this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (short)((short)Value + CustValue(col));
      return this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (short)((short)Value * CustValue(col));
      return this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (short)((short)Value / CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = (short)(CustValue(numerator) / (short)Value);
      return this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (short)((short)Value % CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = (short)(CustValue(numerator) % (short)Value);
      return this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      Value = ~(short)Value;
      return this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      Value = (short)((short)Value & CustValue(denominator));
      return this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      Value = (short)((ushort)(short)Value | (ushort)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      Value = (short)((short)Value ^ CustValue(denominator));
      return this;
    }
  }
}
