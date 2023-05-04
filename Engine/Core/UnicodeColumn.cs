using System.Globalization;

namespace VistaDB.Engine.Core
{
  internal class UnicodeColumn : NVarcharColumn
  {
    internal UnicodeColumn(string val, CultureInfo culture, bool caseInsensitive)
      : base(val, int.MaxValue, culture, caseInsensitive, NCharColumn.DefaultUnicode)
    {
    }

    internal UnicodeColumn(UnicodeColumn col)
      : base((NVarcharColumn) col)
    {
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.NChar;
      }
    }

    internal override string PaddedStringValue
    {
      get
      {
        return (string) Value;
      }
      set
      {
        Value = (object) value;
      }
    }

    protected override void TestMaxSize(int maxLength)
    {
    }

    protected override string TrimData(string val, int max)
    {
      return val;
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new UnicodeColumn(this);
    }
  }
}
