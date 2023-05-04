using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class NCharColumn : CharColumn
  {
    internal static readonly int Utf8CodePage = 65001;
    internal static readonly int Utf16CodePage = 1200;
    internal static readonly int DefaultUnicode = Utf8CodePage;

    internal static int MakeUpUnicodePage(int codePage)
    {
      if (codePage == Utf16CodePage || codePage == Utf8CodePage)
        return codePage;
      return DefaultUnicode;
    }

    internal NCharColumn(string val, int maxSize, CultureInfo culture, bool caseInsensitive, int codePage)
      : base(val, maxSize, MakeUpUnicodePage(codePage), culture, caseInsensitive)
    {
      ResetType(VistaDBType.NChar);
    }

    internal NCharColumn(NCharColumn col)
      : base(col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      NCharColumn ncharColumn = new NCharColumn(this);
      if (padRight)
        ncharColumn.PadBySpaces();
      return ncharColumn;
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
