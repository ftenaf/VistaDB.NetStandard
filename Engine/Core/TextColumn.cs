using System;
using System.Globalization;
using System.Text;

namespace VistaDB.Engine.Core
{
  internal class TextColumn : ExtendedColumn
  {
    private static readonly string topString = new string(char.MaxValue, 1);
    protected CultureInfo culture;
    protected bool caseInsensitive;
    private Encoding encoding;

    internal TextColumn(string val, int codePage, CultureInfo culture, bool caseInsensitive)
      : base(val, VistaDBType.Text)
    {
      encoding = Encoding.GetEncoding(codePage);
      this.culture = culture;
      this.caseInsensitive = caseInsensitive;
    }

    internal TextColumn(TextColumn col)
      : base(col)
    {
      encoding = col.encoding;
      culture = col.culture;
      caseInsensitive = col.caseInsensitive;
    }

    internal override int CodePage
    {
      get
      {
        return encoding.CodePage;
      }
    }

    internal override object DummyNull
    {
      get
      {
        return string.Empty;
      }
    }

    internal override int CompareLength
    {
      get
      {
        return 0;
      }
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.NChar;
      }
    }

    public override object MaxValue
    {
      get
      {
        return topString;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (string);
      }
    }

    protected override long Collate(Row.Column col)
    {
      string strA = (string) Value;
      string strB = (string) col.Value;
      return strA.Length <= 0 || strA[0] != char.MaxValue ? (strB.Length <= 0 || strB[0] != char.MaxValue ? string.Compare(strA, strB, caseInsensitive, culture) : -1L) : (strB.Length <= 0 || strB[0] != char.MaxValue ? 1L : 0L);
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new TextColumn(this);
    }

    protected override byte[] OnFormatExtendedBuffer()
    {
      if (!IsNull)
        return encoding.GetBytes((string) Value);
      return null;
    }

    protected override object OnUnformatExtendedBuffer(byte[] buffer, int length)
    {
      return encoding.GetString(buffer, 0, length);
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = PaddedStringValue + col.PaddedStringValue.TrimStart();
      return this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = PaddedStringValue + col.PaddedStringValue;
      return this;
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = PaddedStringValue.TrimStart();
      return this;
    }
  }
}
