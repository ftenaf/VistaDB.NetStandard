using System;

namespace VistaDB.Engine.Core
{
  internal class DateTimeColumn : Row.Column
  {
    private static readonly int DTSize = 8;

    internal DateTimeColumn()
      : base(null, VistaDBType.DateTime, DTSize)
    {
    }

    internal DateTimeColumn(DateTime val)
      : base(val, VistaDBType.DateTime, DTSize)
    {
    }

    internal DateTimeColumn(DateTimeColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return DateTime.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return DateTime.MaxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (DateTime);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (DateTime)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new DateTimeColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      Array.Copy(BitConverter.GetBytes(((DateTime)Value).Ticks), 0, buffer, offset, DTSize);
      return offset + DTSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = new DateTime(BitConverter.ToInt64(buffer, offset));
      return offset + DTSize;
    }

    protected override long Collate(Row.Column col)
    {
      return DateTime.Compare((DateTime)Value, (DateTime)col.Value);
    }
  }
}
