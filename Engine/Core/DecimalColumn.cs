using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class DecimalColumn : Row.Column
  {
    private static readonly int IntSize = 4;
    private static readonly int BoolSize = 1;
    private static readonly int ScaleSize = 1;
    private static readonly int DecSize = IntSize + IntSize + IntSize + BoolSize + ScaleSize;

    internal static Decimal CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return Decimal.Parse((string) col.Value);
        case VistaDBType.TinyInt:
          return (byte)col.Value;
        case VistaDBType.SmallInt:
          return (short)col.Value;
        case VistaDBType.Int:
          return (int)col.Value;
        case VistaDBType.BigInt:
          return (long)col.Value;
        default:
          return (Decimal) col.Value;
      }
    }

    internal DecimalColumn()
      : base(null, VistaDBType.Decimal, DecSize)
    {
    }

    internal DecimalColumn(Decimal val)
      : base(val, VistaDBType.Decimal, DecSize)
    {
    }

    internal DecimalColumn(DecimalColumn col)
      : base(col)
    {
    }

    protected DecimalColumn(VistaDBType type, int size)
      : base(null, type, size)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return new Decimal(-1, -1, -1, true, 0);
      }
    }

    public override object MaxValue
    {
      get
      {
        return new Decimal(-1, -1, -1, false, 0);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (Decimal)value;
      }
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.Decimal;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (Decimal);
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new DecimalColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int[] bits = Decimal.GetBits((Decimal) Value);
      offset = VdbBitConverter.GetBytes((uint) bits[0], buffer, offset, IntSize);
      offset = VdbBitConverter.GetBytes((uint) bits[1], buffer, offset, IntSize);
      offset = VdbBitConverter.GetBytes((uint) bits[2], buffer, offset, IntSize);
      byte[] bytes = BitConverter.GetBytes(bits[3]);
      Array.Copy(bytes, 2, buffer, offset, ScaleSize);
      offset += ScaleSize;
      Array.Copy(bytes, 3, buffer, offset, BoolSize);
      offset += BoolSize;
      return offset;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int int32_1 = BitConverter.ToInt32(buffer, offset);
      offset += IntSize;
      int int32_2 = BitConverter.ToInt32(buffer, offset);
      offset += IntSize;
      int int32_3 = BitConverter.ToInt32(buffer, offset);
      offset += IntSize;
      byte scale = buffer[offset];
      offset += ScaleSize;
      bool boolean = BitConverter.ToBoolean(buffer, offset);
      offset += BoolSize;
      val = new Decimal(int32_1, int32_2, int32_3, boolean, scale);
      return offset;
    }

    protected override long Collate(Row.Column col)
    {
      return Decimal.Compare((Decimal)Value, (Decimal)col.Value);
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = Decimal.Negate((Decimal)Value);
      return this;
    }

    protected override Row.Column DoMinus(Row.Column column)
    {
      Value = (Decimal)Value - CustValue(column);
      return this;
    }

    protected override Row.Column DoPlus(Row.Column column)
    {
      Value = (Decimal)Value + CustValue(column);
      return this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = Decimal.Multiply((Decimal)Value, CustValue(col));
      return this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = Decimal.Divide((Decimal)Value, CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = Decimal.Divide(CustValue(numerator), (Decimal)Value);
      return this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (Decimal)Value % CustValue(denominator);
      return this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = CustValue(numerator) % (Decimal)Value;
      return this;
    }
  }
}
