using System;

namespace VistaDB.Engine.Core
{
  internal class FloatColumn : Row.Column
  {
    private static int DoubleSize = 8;

    internal static double CustValue(Row.Column col)
    {
      switch (col.InternalType)
      {
        case VistaDBType.NChar:
          return double.Parse((string) col.Value, CrossConversion.NumberFormat);
        case VistaDBType.TinyInt:
          return (byte)col.Value;
        case VistaDBType.SmallInt:
          return (short)col.Value;
        case VistaDBType.Int:
          return (int)col.Value;
        case VistaDBType.BigInt:
          return (long)col.Value;
        case VistaDBType.Real:
          return (double) (float) col.Value;
        case VistaDBType.Decimal:
          return Decimal.ToDouble((Decimal) col.Value);
        default:
          return (double) col.Value;
      }
    }

    internal FloatColumn()
      : base(null, VistaDBType.Float, DoubleSize)
    {
    }

    internal FloatColumn(double val)
      : base(val, VistaDBType.Float, DoubleSize)
    {
    }

    internal FloatColumn(FloatColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return double.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return double.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (double);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (double)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new FloatColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      Array.Copy(BitConverter.GetBytes((double)Value), 0, buffer, offset, DoubleSize);
      return offset + DoubleSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = BitConverter.ToDouble(buffer, offset);
      return offset + DoubleSize;
    }

    protected override long Collate(Row.Column col)
    {
      double num1 = (double) Value;
      double num2 = (double) col.Value;
      return num1 > num2 ? 1L : (num1 < num2 ? -1L : 0L);
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = -(double)Value;
      return this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (double)Value - CustValue(col);
      return this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (double)Value + CustValue(col);
      return this;
    }

    protected override Row.Column DoMultiplyBy(Row.Column col)
    {
      Value = (double)Value * CustValue(col);
      return this;
    }

    protected override Row.Column DoDivideBy(Row.Column denominator)
    {
      Value = (double)Value / CustValue(denominator);
      return this;
    }

    protected override Row.Column DoGetDividedBy(Row.Column numerator)
    {
      Value = CustValue(numerator) / (double)Value;
      return this;
    }

    protected override Row.Column DoModBy(Row.Column denominator)
    {
      Value = (double)Value % CustValue(denominator);
      return this;
    }

    protected override Row.Column DoGetModBy(Row.Column numerator)
    {
      Value = CustValue(numerator) % (double)Value;
      return this;
    }
  }
}
