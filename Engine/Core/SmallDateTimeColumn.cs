using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class SmallDateTimeColumn : Row.Column
  {
    internal static readonly DateTime MinDate = new DateTime(1900, 1, 1);
    private static readonly ulong originDateMinutes = (ulong)MinDate.Ticks / 600000000UL;
    internal static readonly DateTime MaxDate = new DateTime((1342177279L + (long)originDateMinutes) * 600000000L);

        internal SmallDateTimeColumn()
      : base(null, VistaDBType.SmallDateTime, 4)
    {
    }

    internal SmallDateTimeColumn(DateTime val)
      : base(val, VistaDBType.SmallDateTime, 4)
    {
    }

    internal SmallDateTimeColumn(SmallDateTimeColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return MinDate;
      }
    }

    public override object MaxValue
    {
      get
      {
        return MaxDate;
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : TestDynamicRange((DateTime)value);
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
      return new SmallDateTimeColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((uint) ((ulong) ((DateTime) Value).Ticks / 600000000UL - originDateMinutes), buffer, offset, 4);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = new DateTime((long)((BitConverter.ToUInt32(buffer, offset) + originDateMinutes) * 600000000UL));
      return offset + 4;
    }

    protected override long Collate(Row.Column col)
    {
      return DateTime.Compare((DateTime)Value, (DateTime)col.Value);
    }

    private DateTime TestDynamicRange(DateTime date)
    {
      if (date.CompareTo(MaxDate) > 0 || date.CompareTo(MinDate) < 0)
        throw new VistaDBException(300, "SmallDateTimeColumn = " + date.ToString());
      return date;
    }
  }
}
