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
          return (short) (byte) col.Value;
        default:
          return (short) col.Value;
      }
    }

    internal SmallIntColumn()
      : base((object) null, VistaDBType.SmallInt, Int16Size)
    {
    }

    internal SmallIntColumn(short val)
      : base((object) val, VistaDBType.SmallInt, Int16Size)
    {
    }

    internal SmallIntColumn(SmallIntColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) short.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) short.MaxValue;
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
        base.Value = value == null ? value : (object) (short) value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new SmallIntColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((ushort) (short) Value, buffer, offset, Int16Size);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = (object) BitConverter.ToInt16(buffer, offset);
      return offset + Int16Size;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) ((int) (short) Value - (int) (short) col.Value);
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = (object) -(short) Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (object) (short) ((int) (short) Value - (int)CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (object) (short) ((int) (short) Value + (int)CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (object) (short) ((int) (short) Value * (int)CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (object) (short) ((int) (short) Value / (int)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = (object) (short) ((int)CustValue(numerator) / (int) (short) Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (object) (short) ((int) (short) Value % (int)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = (object) (short) ((int)CustValue(numerator) % (int) (short) Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseNot()
    {
      Value = (object) (int) ~(short) Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseAnd(Row.Column denominator)
    {
      Value = (object) (short) ((int) (short) Value & (int)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseOr(Row.Column denominator)
    {
      Value = (object) (short) ((int) (ushort) (short) Value | (int) (ushort)CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoBitwiseXor(Row.Column denominator)
    {
      Value = (object) (short) ((int) (short) Value ^ (int)CustValue(denominator));
      return (Row.Column) this;
    }
  }
}
