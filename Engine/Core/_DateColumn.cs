using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class _DateColumn : Row.Column
  {
    private const int DateSize = 4;
    private const long ticksPerDay = 864000000000;

    internal _DateColumn()
      : base((object) null, VistaDBType.NChar | VistaDBType.SmallMoney, 4)
    {
    }

    internal _DateColumn(DateTime val)
      : base((object) val, VistaDBType.NChar | VistaDBType.SmallMoney, 4)
    {
    }

    internal _DateColumn(_DateColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) DateTime.MinValue.Date;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) DateTime.MaxValue.Date;
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) ((DateTime) value).Date;
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
      return (Row.Column) new _DateColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((uint) (((DateTime) this.Value).Date.Ticks / 864000000000L), buffer, offset, 4);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      this.val = (object) new DateTime((long) BitConverter.ToUInt32(buffer, offset) * 864000000000L);
      return offset + 4;
    }

    protected override long Collate(Row.Column col)
    {
      return (long) DateTime.Compare((DateTime) this.Value, (DateTime) col.Value);
    }
  }
}
