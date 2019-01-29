using System;

namespace VistaDB.Engine.Core
{
  internal class UniqueIdentifierColumn : Row.Column
  {
    private static int GuidSize = 16;
    public static readonly Guid maxValue = new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff");

    internal UniqueIdentifierColumn()
      : base((object) null, VistaDBType.UniqueIdentifier, UniqueIdentifierColumn.GuidSize)
    {
    }

    internal UniqueIdentifierColumn(Guid val)
      : base((object) val, VistaDBType.UniqueIdentifier, UniqueIdentifierColumn.GuidSize)
    {
    }

    internal UniqueIdentifierColumn(UniqueIdentifierColumn col)
      : base((Row.Column) col)
    {
    }

    internal override object DummyNull
    {
      get
      {
        return (object) Guid.Empty;
      }
    }

    public override object MaxValue
    {
      get
      {
        return (object) UniqueIdentifierColumn.maxValue;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (Guid);
      }
    }

    public override object Value
    {
      set
      {
        base.Value = value == null ? value : (object) (Guid) value;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new UniqueIdentifierColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      Array.Copy((Array) ((Guid) this.Value).ToByteArray(), 0, (Array) buffer, offset, UniqueIdentifierColumn.GuidSize);
      return offset + UniqueIdentifierColumn.GuidSize;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      byte[] b = new byte[UniqueIdentifierColumn.GuidSize];
      Array.Copy((Array) buffer, offset, (Array) b, 0, UniqueIdentifierColumn.GuidSize);
      this.val = (object) new Guid(b);
      return offset + UniqueIdentifierColumn.GuidSize;
    }

    protected override long Collate(Row.Column col)
    {
      Guid guid1 = (Guid) this.Value;
      Guid guid2 = (Guid) col.Value;
      byte[] byteArray1 = guid1.ToByteArray();
      byte[] byteArray2 = guid2.ToByteArray();
      long num = 0;
      for (int index = 0; index < UniqueIdentifierColumn.GuidSize && num == 0L; ++index)
        num = (long) ((int) byteArray1[index] - (int) byteArray2[index]);
      return num;
    }
  }
}
