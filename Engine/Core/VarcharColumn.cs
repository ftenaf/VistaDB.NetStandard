using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class VarcharColumn : CharColumn
  {
    internal VarcharColumn(string val, int maxSize, int codePage, CultureInfo culture, bool caseInsensitive)
      : base(val, maxSize, codePage, culture, caseInsensitive)
    {
      this.ResetType(VistaDBType.VarChar);
    }

    internal VarcharColumn(VarcharColumn col)
      : base((CharColumn) col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new VarcharColumn(this);
    }

    public override string ToString()
    {
      if (!this.IsNull)
        return this.Value.ToString();
      return "<null>";
    }
  }
}
