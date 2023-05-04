using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class VarcharColumn : CharColumn
  {
    internal VarcharColumn(string val, int maxSize, int codePage, CultureInfo culture, bool caseInsensitive)
      : base(val, maxSize, codePage, culture, caseInsensitive)
    {
      ResetType(VistaDBType.VarChar);
    }

    internal VarcharColumn(VarcharColumn col)
      : base(col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new VarcharColumn(this);
    }

    public override string ToString()
    {
      if (!IsNull)
        return Value.ToString();
      return "<null>";
    }
  }
}
