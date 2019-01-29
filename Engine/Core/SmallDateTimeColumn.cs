using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class SmallDateTimeColumn : Row.Column
  {
    internal static readonly DateTime MinDate = new DateTime(1900, 1, 1);
    private static readonly ulong originDateMinutes = (ulong) SmallDateTimeColumn.MinDate.Ticks / 600000000UL;
    internal static readonly DateTime MaxDate = new DateTime((1342177279L + (long) SmallDateTimeColumn.originDateMinutes) * 600000000L);
    private const int SmallDateTimeSize = 4;
    private const ulong ticksPerMinute = 600000000;

    internal SmallDateTimeColumn()
      : base((object) null, VistaDBType.SmallDateTime, 4)
    {
    }

    internal SmallDateTimeColumn(DateTime val)
      : base((object) val, VistaDBType.SmallDateTime, 4)
    {
    }

    internal SmallDateTimeColumn(SmallDateTimeColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) SmallDateTimeColumn.MinDate;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) SmallDateTimeColumn.MaxDate;
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) this.TestDynamicRange((DateTime) value);
      }
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.DateTime;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (DateTime);
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new SmallDateTimeColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((uint) ((ulong) ((DateTime) this.Value).Ticks / 600000000UL - SmallDateTimeColumn.originDateMinutes), buffer, offset, 4);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      this.val = (object) new DateTime((long) (((ulong) BitConverter.ToUInt32(buffer, offset) + SmallDateTimeColumn.originDateMinutes) * 600000000UL));
      return offset + 4;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) DateTime.Compare((DateTime) this.Value, (DateTime) col.Value);
    }

    private DateTime TestDynamicRange(DateTime date)
    {
      if (date.CompareTo(SmallDateTimeColumn.MaxDate) > 0 || date.CompareTo(SmallDateTimeColumn.MinDate) < 0)
        throw new VistaDBException(300, "SmallDateTimeColumn = " + date.ToString());
      return date;
    }
  }
}
