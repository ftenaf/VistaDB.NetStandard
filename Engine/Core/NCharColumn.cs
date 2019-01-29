using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class NCharColumn : CharColumn
  {
    internal static readonly int Utf8CodePage = 65001;
    internal static readonly int Utf16CodePage = 1200;
    internal static readonly int DefaultUnicode = NCharColumn.Utf8CodePage;

    internal static int MakeUpUnicodePage(int codePage)
    {
      if (codePage == NCharColumn.Utf16CodePage || codePage == NCharColumn.Utf8CodePage)
        return codePage;
      return NCharColumn.DefaultUnicode;
    }

    internal NCharColumn(string val, int maxSize, CultureInfo culture, bool caseInsensitive, int codePage)
      : base(val, maxSize, NCharColumn.MakeUpUnicodePage(codePage), culture, caseInsensitive)
    {
      this.ResetType(VistaDBType.NChar);
    }

    internal NCharColumn(NCharColumn col)
      : base((CharColumn) col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      NCharColumn ncharColumn = new NCharColumn(this);
      if (padRight)
        ncharColumn.PadBySpaces();
      return (Row.Column) ncharColumn;
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.NChar;
      }
    }
  }
}
