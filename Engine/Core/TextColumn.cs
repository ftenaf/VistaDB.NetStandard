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
      : base((object) val, VistaDBType.Text)
    {
      encoding = Encoding.GetEncoding(codePage);
      this.culture = culture;
      this.caseInsensitive = caseInsensitive;
    }

    internal TextColumn(TextColumn col)
      : base((ExtendedColumn) col)
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
        return (object) string.Empty;
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
        return (object)topString;
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
      return strA.Length <= 0 || strA[0] != char.MaxValue ? (strB.Length <= 0 || strB[0] != char.MaxValue ? (long) string.Compare(strA, strB, caseInsensitive, culture) : -1L) : (strB.Length <= 0 || strB[0] != char.MaxValue ? 1L : 0L);
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return (Row.Column) new TextColumn(this);
    }

    protected override byte[] OnFormatExtendedBuffer()
    {
      if (!IsNull)
        return encoding.GetBytes((string) Value);
      return (byte[]) null;
    }

    protected override object OnUnformatExtendedBuffer(byte[] buffer, int length)
    {
      return (object) encoding.GetString(buffer, 0, length);
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      Value = (object) (PaddedStringValue + col.PaddedStringValue.TrimStart());
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      Value = (object) (PaddedStringValue + col.PaddedStringValue);
      return (Row.Column) this;
    }

    protected override Row.Column DoUnaryMinus()
    {
      Value = (object) PaddedStringValue.TrimStart();
      return (Row.Column) this;
    }
  }
}
