using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class NTextColumn : TextColumn
  {
    internal NTextColumn(string val, CultureInfo culture, bool caseInsensitive, int codePage)
      : base(val, NCharColumn.MakeUpUnicodePage(codePage), culture, caseInsensitive)
    {
      this.ResetType(VistaDBType.NText);
    }

    internal NTextColumn(NTextColumn col)
      : base((TextColumn) col)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new NTextColumn(this);
    }

    public override int MaxLength
    {
      get
      {
        return base.MaxLength / 2;
      }
    }
  }
}
