namespace VistaDB.Engine.Core
{
  internal class BlobColumn : ExtendedColumn
  {
    private static readonly int typeReferenceSize = 1;
    private BlobColumn.BlobType subType;

    internal BlobColumn()
      : base(VistaDBType.Image)
    {
    }

    internal BlobColumn(BlobColumn column)
      : base((ExtendedColumn) column)
    {
      this.subType = column.subType;
    }

    protected override ushort InheritedSize
    {
      get
      {
        return (ushort) ((uint) base.InheritedSize + (uint) BlobColumn.typeReferenceSize);
      }
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.VarBinary;
      }
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new BlobColumn(this);
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      offset = base.ConvertToByteArray(buffer, offset, precedenceColumn);
      buffer[offset] = (byte) this.subType;
      offset += BlobColumn.typeReferenceSize;
      return offset;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      offset = base.ConvertFromByteArray(buffer, offset, precedenceColumn);
      this.subType = (BlobColumn.BlobType) buffer[offset];
      offset += BlobColumn.typeReferenceSize;
      return offset;
    }

    internal override void CreateFullCopy(Row.Column srcColumn)
    {
      base.CreateFullCopy(srcColumn);
      this.subType = ((BlobColumn) srcColumn).subType;
    }

    protected override long Collate(Row.Column col)
    {
      return base.Collate(col);
    }

    internal enum BlobType : byte
    {
      Unknown,
      Picture,
      File,
    }
  }
}
