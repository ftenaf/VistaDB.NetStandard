using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class NVarcharColumn : VarcharColumn
  {
    internal NVarcharColumn(string val, int maxSize, CultureInfo culture, bool caseInsensitive, int codePage)
      : base(val, maxSize, NCharColumn.MakeUpUnicodePage(codePage), culture, caseInsensitive)
    {
      ResetType(VistaDBType.NVarChar);
    }

    internal NVarcharColumn(NVarcharColumn col)
      : base(col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new NVarcharColumn(this);
    }
  }
}
