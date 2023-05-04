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
          return (byte)col.Value;
        case VistaDBType.SmallInt:
          return (short)col.Value;
        case VistaDBType.Int:
          return (int)col.Value;
        case VistaDBType.BigInt:
          return (long)col.Value;
        case VistaDBType.Decimal:
          return Decimal.ToSingle((Decimal) col.Value);
        default:
          return (float) col.Value;
      }
    }

    internal RealColumn()
      : base(null, VistaDBType.Real, SingleSize)
    {
    }

    internal RealColumn(float val)
      : base(val, VistaDBType.Real, SingleSize)
    {
    }

    internal RealColumn(RealColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return float.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return float.MaxValue;
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
        base.Value = value == null ? value : (float)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new RealColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      Array.Copy(BitConverter.GetBytes((float)Value), 0, buffer, offset, SingleSize);
      return offset + SingleSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = BitConverter.ToSingle(buffer, offset);
      return offset + SingleSize;
    }

    protected override long Collate(Row.Column col)
    {
      float num1 = (float) Value;
      float num2 = (float) col.Value;
      return (double) num1 > (double) num2 ? 1L : ((double) num1 < (double) num2 ? -1L : 0L);
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = (float)-(double)(float)Value;
      return this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (float)((double)(float)Value * (double)CustValue(col));
      return this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (float)((double)(float)Value - (double)CustValue(col));
      return this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (float)((double)(float)Value + (double)CustValue(col));
      return this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (float)((double)(float)Value / (double)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = (float)((double)CustValue(numerator) / (double)(float)Value);
      return this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (float)((double)(float)Value % (double)CustValue(denominator));
      return this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = (float)((double)CustValue(numerator) % (double)(float)Value);
      return this;
    }
  }
}
