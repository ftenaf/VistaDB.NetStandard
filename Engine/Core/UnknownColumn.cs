namespace VistaDB.Engine.Core
{
  internal class UnknownColumn : Row.Column
  {
    private UnknownColumn()
      : base(null, VistaDBType.Unknown, 0)
    {
    }

    internal UnknownColumn(UnknownColumn col)
      : base(col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return null;
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
