using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class DecimalColumn : Row.Column
  {
    private static readonly int IntSize = 4;
    private static readonly int BoolSize = 1;
    private static readonly int ScaleSize = 1;
    private static readonly int DecSize = DecimalColumn.IntSize + DecimalColumn.IntSize + DecimalColumn.IntSize + DecimalColumn.BoolSize + DecimalColumn.ScaleSize;

    internal static Decimal CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return Decimal.Parse((string) col.Value);
        case VistaDBType.TinyInt:
          return (Decimal) ((byte) col.Value);
        case VistaDBType.SmallInt:
          return (Decimal) ((short) col.Value);
        case VistaDBType.Int:
          return (Decimal) ((int) col.Value);
        case VistaDBType.BigInt:
          return (Decimal) ((long) col.Value);
        default:
          return (Decimal) col.Value;
      }
    }

    internal DecimalColumn()
      : base((object) null, VistaDBType.Decimal, DecimalColumn.DecSize)
    {
    }

    internal DecimalColumn(Decimal val)
      : base((object) val, VistaDBType.Decimal, DecimalColumn.DecSize)
    {
    }

    internal DecimalColumn(DecimalColumn col)
      : base((Row.Column) col)
    {
    }

    protected DecimalColumn(VistaDBType type, int size)
      : base((object) null, type, size)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) new Decimal(-1, -1, -1, true, (byte) 0);
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) new Decimal(-1, -1, -1, false, (byte) 0);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) (Decimal) value;
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
      return (Row.Column) new DecimalColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int[] bits = Decimal.GetBits((Decimal) this.Value);
      offset = VdbBitConverter.GetBytes((uint) bits[0], buffer, offset, DecimalColumn.IntSize);
      offset = VdbBitConverter.GetBytes((uint) bits[1], buffer, offset, DecimalColumn.IntSize);
      offset = VdbBitConverter.GetBytes((uint) bits[2], buffer, offset, DecimalColumn.IntSize);
      byte[] bytes = BitConverter.GetBytes(bits[3]);
      Array.Copy((Array) bytes, 2, (Array) buffer, offset, DecimalColumn.ScaleSize);
      offset += DecimalColumn.ScaleSize;
      Array.Copy((Array) bytes, 3, (Array) buffer, offset, DecimalColumn.BoolSize);
      offset += DecimalColumn.BoolSize;
      return offset;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int int32_1 = BitConverter.ToInt32(buffer, offset);
      offset += DecimalColumn.IntSize;
      int int32_2 = BitConverter.ToInt32(buffer, offset);
      offset += DecimalColumn.IntSize;
      int int32_3 = BitConverter.ToInt32(buffer, offset);
      offset += DecimalColumn.IntSize;
      byte scale = buffer[offset];
      offset += DecimalColumn.ScaleSize;
      bool boolean = BitConverter.ToBoolean(buffer, offset);
      offset += DecimalColumn.BoolSize;
      this.val = (object) new Decimal(int32_1, int32_2, int32_3, boolean, scale);
      return offset;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) Decimal.Compare((Decimal) this.Value, (Decimal) col.Value);
    }

    protected override Row.Column DoUnaryMinus()
    {
      this.Value = (object) Decimal.Negate((Decimal) this.Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column column)
    {
      this.Value = (object) ((Decimal) this.Value - DecimalColumn.CustValue(column));
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column column)
    {
      this.Value = (object) ((Decimal) this.Value + DecimalColumn.CustValue(column));
      return (Row.Column) this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      this.Value = (object) Decimal.Multiply((Decimal) this.Value, DecimalColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      this.Value = (object) Decimal.Divide((Decimal) this.Value, DecimalColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      this.Value = (object) Decimal.Divide(DecimalColumn.CustValue(numerator), (Decimal) this.Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      this.Value = (object) ((Decimal) this.Value % DecimalColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      this.Value = (object) (DecimalColumn.CustValue(numerator) % (Decimal) this.Value);
      return (Row.Column) this;
    }
  }
}
