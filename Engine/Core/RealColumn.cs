using System;

namespace VistaDB.Engine.Core
{
  internal class RealColumn : Row.Column
  {
    public static int SingleSize = 4;

    internal static float CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return float.Parse((string) col.Value, CrossConversion.NumberFormat);
        case VistaDBType.TinyInt:
          return (float) (byte) col.Value;
        case VistaDBType.SmallInt:
          return (float) (short) col.Value;
        case VistaDBType.Int:
          return (float) (int) col.Value;
        case VistaDBType.BigInt:
          return (float) (long) col.Value;
        case VistaDBType.Decimal:
          return Decimal.ToSingle((Decimal) col.Value);
        default:
          return (float) col.Value;
      }
    }

    internal RealColumn()
      : base((object) null, VistaDBType.Real, RealColumn.SingleSize)
    {
    }

    internal RealColumn(float val)
      : base((object) val, VistaDBType.Real, RealColumn.SingleSize)
    {
    }

    internal RealColumn(RealColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) float.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) float.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (float);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) (float) value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new RealColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      Array.Copy((Array) BitConverter.GetBytes((float) this.Value), 0, (Array) buffer, offset, RealColumn.SingleSize);
      return offset + RealColumn.SingleSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      this.val = (object) BitConverter.ToSingle(buffer, offset);
      return offset + RealColumn.SingleSize;
    }

    protected override long Collate(Row.Column col)
    {
      float num1 = (float) this.Value;
      float num2 = (float) col.Value;
      return (double) num1 > (double) num2 ? 1L : ((double) num1 < (double) num2 ? -1L : 0L);
    }

    protected override Row.Column DoUnaryMinus()
    {
      this.Value = (object) (float) -(double) (float) this.Value;
      return (Row.Column) this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      this.Value = (object) (float) ((double) (float) this.Value * (double) RealColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      this.Value = (object) (float) ((double) (float) this.Value - (double) RealColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      this.Value = (object) (float) ((double) (float) this.Value + (double) RealColumn.CustValue(col));
      return (Row.Column) this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      this.Value = (object) (float) ((double) (float) this.Value / (double) RealColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      this.Value = (object) (float) ((double) RealColumn.CustValue(numerator) / (double) (float) this.Value);
      return (Row.Column) this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      this.Value = (object) (float) ((double) (float) this.Value % (double) RealColumn.CustValue(denominator));
      return (Row.Column) this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      this.Value = (object) (float) ((double) RealColumn.CustValue(numerator) % (double) (float) this.Value);
      return (Row.Column) this;
    }
  }
}
