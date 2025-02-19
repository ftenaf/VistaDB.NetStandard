﻿using System;

namespace VistaDB.Engine.Core
{
  internal class BitColumn : Row.Column
  {
    public static int BooleanSize = 1;
    public static readonly BitColumn True = new BitColumn(true);
    public static readonly BitColumn False = new BitColumn(false);

    internal BitColumn()
      : base(null, VistaDBType.Bit, BooleanSize)
    {
    }

    internal BitColumn(bool val)
      : base(val, VistaDBType.Bit, BooleanSize)
    {
    }

    internal BitColumn(BitColumn col)
      : base(col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return false;
      }
    }

    public override object MaxValue
    {
      get
      {
        return true;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (bool);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (bool)value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new BitColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      buffer[offset] = (bool) Value ? (byte) 1 : (byte) 0;
      return offset + BooleanSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      val = BitConverter.ToBoolean(buffer, offset);
      return offset + BooleanSize;
    }

    protected override long Collate(Row.Column col)
    {
      return ((bool)Value ? 1 : 0) - ((bool)col.Value ? 1 : 0);
    }
  }
}
