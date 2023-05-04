using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class _DateColumn : Row.Column
  {
        internal _DateColumn()
      : base(null, VistaDBType.NChar | VistaDBType.SmallMoney, 4)
    {
    }

    internal _DateColumn(DateTime val)
      : base(val, VistaDBType.NChar | VistaDBType.SmallMoney, 4)
    {
    }

    internal _DateColumn(_DateColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return DateTime.MinValue.Date;
      }
    }

    public override object MaxValue
    {
      get
      {
        return DateTime.MaxValue.Date;
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : ((DateTime)value).Date;
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
      return new _DateColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return VdbBitConverter.GetBytes((uint) (((DateTime) Value).Date.Ticks / 864000000000L), buffer, offset, 4);
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = new DateTime(BitConverter.ToUInt32(buffer, offset) * 864000000000L);
      return offset + 4;
    }

    protected override long Collate(Row.Column col)
    {
      return DateTime.Compare((DateTime)Value, (DateTime)col.Value);
    }
  }
}
