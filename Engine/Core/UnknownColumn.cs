namespace VistaDB.Engine.Core
{
  internal class UnknownColumn : Row.Column
  {
    private UnknownColumn()
      : base((object) null, VistaDBType.Unknown, 0)
    {
    }

    internal UnknownColumn(UnknownColumn col)
      : base((Row.Column) col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) null;
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      return 0;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return offset;
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return offset;
    }
  }
}
