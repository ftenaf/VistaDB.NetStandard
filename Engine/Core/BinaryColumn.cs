using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class BinaryColumn : Row.Column
  {
    private static readonly int lengthCounterSize = 2;
    private static int MaxArray = (int) ushort.MaxValue;
    private static byte[] dummyNull = new byte[0];

    internal BinaryColumn()
      : this((byte[]) null)
    {
    }

    internal BinaryColumn(byte[] val)
      : base((object) val, VistaDBType.VarBinary, BinaryColumn.MaxArray)
    {
    }

    internal BinaryColumn(BinaryColumn col)
      : base((Row.Column) col)
    {
    }

    protected BinaryColumn(VistaDBType type)
      : base((object) null, type, BinaryColumn.MaxArray)
    {
    }

    protected virtual int MaxArraySize
    {
      get
      {
        return BinaryColumn.MaxArray;
      }
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      if (!this.IsNull)
        return ((byte[]) this.Value).Length + this.GetLengthCounterWidth(precedenceColumn);
      return 0;
    }

    internal override int GetLengthCounterWidth(Row.Column precedenceColumn)
    {
      return BinaryColumn.lengthCounterSize;
    }

    internal override object DummyNull
    {
      get
      {
        return (object) BinaryColumn.dummyNull;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (byte[]);
      }
    }

    public override bool FixedType
    {
      get
      {
        return false;
      }
    }

    public override int MaxLength
    {
      get
      {
        return this.MaxArraySize;
      }
    }

    protected virtual ushort InheritedSize
    {
      get
      {
        return 0;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new BinaryColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      byte[] val = (byte[]) this.val;
      ushort length = (ushort) val.Length;
      int lengthCounterWidth = this.GetLengthCounterWidth(precedenceColumn);
      offset = VdbBitConverter.GetBytes((ushort) ((uint) length + (uint) this.InheritedSize), buffer, offset, lengthCounterWidth);
      Array.Copy((Array) val, 0, (Array) buffer, offset, (int) length);
      return offset + (int) length;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int length = (int) BitConverter.ToUInt16(buffer, offset) - (int) this.InheritedSize;
      offset += this.GetLengthCounterWidth(precedenceColumn);
      this.val = (object) new byte[length];
      Array.Copy((Array) buffer, offset, (Array) this.val, 0, length);
      return offset + length;
    }

    internal override int ReadVarLength(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return (int) BitConverter.ToUInt16(buffer, offset);
    }

    protected override long Collate(Row.Column col)
    {
      byte[] numArray1 = (byte[]) this.Value;
      byte[] numArray2 = (byte[]) col.Value;
      int length = numArray1.Length;
      long num = (long) (length - numArray2.Length);
      for (int index = 0; num == 0L && index < length; ++index)
        num = (long) ((int) numArray1[index] - (int) numArray2[index]);
      return num;
    }

    public override string ToString()
    {
      if (this.IsNull)
        return "<null>";
      byte[] numArray = (byte[]) this.Value;
      string str = string.Empty;
      for (int index = 0; index < numArray.Length; ++index)
        str = numArray[index].ToString() + "@";
      return str;
    }
  }
}
