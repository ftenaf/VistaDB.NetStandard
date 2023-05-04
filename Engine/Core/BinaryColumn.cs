using System;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class BinaryColumn : Row.Column
  {
    private static readonly int lengthCounterSize = 2;
    private static int MaxArray = ushort.MaxValue;
    private static byte[] dummyNull = new byte[0];

    internal BinaryColumn()
      : this((byte[]) null)
    {
    }

    internal BinaryColumn(byte[] val)
      : base(val, VistaDBType.VarBinary, MaxArray)
    {
    }

    internal BinaryColumn(BinaryColumn col)
      : base(col)
    {
    }

    protected BinaryColumn(VistaDBType type)
      : base(null, type, MaxArray)
    {
    }

    protected virtual int MaxArraySize
    {
      get
      {
        return MaxArray;
      }
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      if (!IsNull)
        return ((byte[]) Value).Length + GetLengthCounterWidth(precedenceColumn);
      return 0;
    }

    internal override int GetLengthCounterWidth(Row.Column precedenceColumn)
    {
      return lengthCounterSize;
    }

    internal override object DummyNull
    {
      get
      {
        return dummyNull;
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
        return MaxArraySize;
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
      return new BinaryColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      byte[] val = (byte[]) this.val;
      ushort length = (ushort) val.Length;
      int lengthCounterWidth = GetLengthCounterWidth(precedenceColumn);
      offset = VdbBitConverter.GetBytes((ushort) (length + (uint) InheritedSize), buffer, offset, lengthCounterWidth);
      Array.Copy(val, 0, buffer, offset, length);
      return offset + length;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int length = BitConverter.ToUInt16(buffer, offset) - InheritedSize;
      offset += GetLengthCounterWidth(precedenceColumn);
      val = (new byte[length]);
      Array.Copy(buffer, offset, (Array) val, 0, length);
      return offset + length;
    }

    internal override int ReadVarLength(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return BitConverter.ToUInt16(buffer, offset);
    }

    protected override long Collate(Row.Column col)
    {
      byte[] numArray1 = (byte[]) Value;
      byte[] numArray2 = (byte[]) col.Value;
      int length = numArray1.Length;
      long num = length - numArray2.Length;
      for (int index = 0; num == 0L && index < length; ++index)
        num = numArray1[index] - numArray2[index];
      return num;
    }

    public override string ToString()
    {
      if (IsNull)
        return "<null>";
      byte[] numArray = (byte[]) Value;
      string str = string.Empty;
      for (int index = 0; index < numArray.Length; ++index)
        str = numArray[index].ToString() + "@";
      return str;
    }
  }
}
