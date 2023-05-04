using System;

namespace VistaDB.Engine.Core
{
  internal class DateTimeColumn : Row.Column
  {
    private static readonly int DTSize = 8;

    internal DateTimeColumn()
      : base((object) null, VistaDBType.DateTime, DTSize)
    {
    }

    internal DateTimeColumn(DateTime val)
      : base((object) val, VistaDBType.DateTime, DTSize)
    {
    }

    internal DateTimeColumn(DateTimeColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) DateTime.MinValue;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) DateTime.MaxValue;
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
        base.Value = value == null ? value : (object) (DateTime) value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new DateTimeColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      Array.Copy((Array) BitConverter.GetBytes(((DateTime) Value).Ticks), 0, (Array) buffer, offset, DTSize);
      return offset + DTSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = (object) new DateTime(BitConverter.ToInt64(buffer, offset));
      return offset + DTSize;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) DateTime.Compare((DateTime) Value, (DateTime) col.Value);
    }
  }
}
