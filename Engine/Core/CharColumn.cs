using System;
using System.Globalization;
using System.Text;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;

namespace VistaDB.Engine.Core
{
  internal class CharColumn : Row.Column
  {
    private static readonly int LengthCounterSize = 2;
    private static readonly string topString = new string(char.MaxValue, 1);
    private static readonly string topZeroString = string.Empty;
    internal const int MaxSize = 8192;
    private int maxLength;
    protected CultureInfo culture;
    protected bool caseInsensitive;
    private Encoding encoding;
    private int actualLen;

    private static string AlignLeft(string val, int maxLength)
    {
      if (val == null)
        return (string) null;
      if (val.Length > maxLength)
        return val.Remove(maxLength, val.Length - maxLength);
      return val;
    }

    protected virtual string TrimData(string val, int max)
    {
      if (val == null)
        return (string) null;
      if (val.Length > max)
      {
        VistaDBException vistaDbException = new VistaDBException(301, Name + "(" + maxLength.ToString() + ")");
        vistaDbException.Data.Add((object) "Column", (object) Name);
        vistaDbException.Data.Add((object) "Value", (object) val);
        vistaDbException.Data.Add((object) "SqlRow", (object) RowIndex);
        throw vistaDbException;
      }
      return val.TrimEnd(' ');
    }

    internal CharColumn(string val, int maxLength, int codePage, CultureInfo culture, bool caseInsensitive)
      : base((object) val, VistaDBType.Char, maxLength)
    {
      encoding = Encoding.GetEncoding(codePage);
      this.maxLength = maxLength;
      Value = (object) val;
      TestMaxSize(maxLength);
      actualLen = val == null ? 0 : val.Length;
      if (actualLen > maxLength)
        throw new VistaDBException(301, Name + "(" + this.maxLength.ToString() + ")");
      this.culture = culture;
      this.caseInsensitive = caseInsensitive;
    }

    protected virtual void TestMaxSize(int maxLength)
    {
      if (maxLength < 0 || maxLength > 8192)
        throw new VistaDBException(303, 8192.ToString());
    }

    internal CharColumn(CharColumn col)
      : base((Row.Column) col)
    {
      encoding = col.encoding;
      maxLength = col.maxLength;
      culture = col.culture;
      caseInsensitive = col.caseInsensitive;
      actualLen = col.actualLen;
    }

    internal override object DummyNull
    {
      get
      {
        return (object) string.Empty;
      }
    }

    public override object Value
    {
      set
      {
        base.Value = (object)AlignLeft(TrimData((string) value, maxLength), maxLength);
        actualLen = IsNull ? 0 : ((string) value).Length;
      }
    }

    protected override object NonTrimedValue
    {
      set
      {
        base.Value = (object)AlignLeft((string) value, maxLength);
      }
    }

    internal override object DoGetTrimmedValue()
    {
      return Value;
    }

    private string GetTailSubvalue(Row.Column precedenceColumn, ref ushort equalLen)
    {
      string str1 = (string) precedenceColumn.Value;
      ushort length1 = (ushort) str1.Length;
      string str2 = (string) Value;
      ushort length2 = (ushort) str2.Length;
      ushort num = (int) length2 <= (int) length1 ? length2 : length1;
      equalLen = (ushort) 0;
      while ((int) equalLen < (int) num && (int) str2[(int) equalLen] == (int) str1[(int) equalLen])
        ++equalLen;
      return str2.Substring((int) equalLen);
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      if (IsNull)
        return 0;
      int lengthCounterSize = LengthCounterSize;
      if (precedenceColumn == (Row.Column) null || precedenceColumn.IsNull || maxLength <= 2)
        return encoding.GetByteCount((string) Value) + lengthCounterSize;
      ushort equalLen = 0;
      return encoding.GetByteCount(GetTailSubvalue(precedenceColumn, ref equalLen)) + lengthCounterSize + lengthCounterSize;
    }

    internal override int GetLengthCounterWidth(Row.Column precedenceColumn)
    {
      return LengthCounterSize;
    }

    public override object MaxValue
    {
      get
      {
        if (MaxLength <= 0)
          return (object)topZeroString;
        return (object)topString;
      }
    }

    public override int MaxLength
    {
      get
      {
        return maxLength;
      }
    }

    internal override int CodePage
    {
      get
      {
        return encoding.CodePage;
      }
    }

    public override VistaDBType InternalType
    {
      get
      {
        return VistaDBType.NChar;
      }
    }

    public override Type SystemType
    {
      get
      {
        return typeof (string);
      }
    }

    internal override string PaddedStringValue
    {
      get
      {
        return base.PaddedStringValue.PadRight(actualLen);
      }
      set
      {
        Value = (object) value;
      }
    }

    internal override object PaddedValue
    {
      get
      {
        if (!IsNull)
          return (object) PaddedStringValue;
        return (object) null;
      }
    }

    public override bool FixedType
    {
      get
      {
        return false;
      }
    }

    protected override long Collate(Row.Column col)
    {
      string strA = (string) Value;
      string strB = (string) col.Value;
      return strA.Length <= 0 || strA[0] != char.MaxValue ? (strB.Length <= 0 || strB[0] != char.MaxValue ? (long) string.Compare(strA, strB, caseInsensitive, culture) : -1L) : (strB.Length <= 0 || strB[0] != char.MaxValue ? 1L : 0L);
    }

    protected override long CollateTrimmed(Row.Column col)
    {
      string strA = ((string) Value).TrimEnd();
      string strB = ((string) col.Value).TrimEnd();
      return strA.Length <= 0 || strA[0] != char.MaxValue ? (strB.Length <= 0 || strB[0] != char.MaxValue ? (long) string.Compare(strA, strB, caseInsensitive, culture) : -1L) : (strB.Length <= 0 || strB[0] != char.MaxValue ? 1L : 0L);
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      CharColumn charColumn = new CharColumn(this);
      if (padRight)
        charColumn.PadBySpaces();
      return (Row.Column) charColumn;
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      byte[] bytes;
      ushort length;
      if (precedenceColumn != (Row.Column) null && maxLength > 2)
      {
        ushort equalLen = 0;
        bytes = encoding.GetBytes(GetTailSubvalue(precedenceColumn, ref equalLen));
        length = (ushort) bytes.Length;
        offset = VdbBitConverter.GetBytes((ushort) ((uint) length + (uint)LengthCounterSize), buffer, offset, LengthCounterSize);
        offset = VdbBitConverter.GetBytes(equalLen, buffer, offset, LengthCounterSize);
      }
      else
      {
        bytes = encoding.GetBytes((string) Value);
        length = (ushort) bytes.Length;
        offset = VdbBitConverter.GetBytes(length, buffer, offset, LengthCounterSize);
      }
      Array.Copy((Array) bytes, 0, (Array) buffer, offset, (int) length);
      return offset + (int) length;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      int int16_1 = (int) BitConverter.ToInt16(buffer, offset);
      offset += LengthCounterSize;
      if (precedenceColumn != (Row.Column) null && maxLength > 2)
      {
        short int16_2 = BitConverter.ToInt16(buffer, offset);
        offset += LengthCounterSize;
        int16_1 -= LengthCounterSize;
        val = (object) encoding.GetString(buffer, offset, int16_1);
        if (int16_2 > (short) 0)
          val = (object) (((string) precedenceColumn.Value).Substring(0, (int) int16_2) + val);
      }
      else
        val = (object) encoding.GetString(buffer, offset, int16_1);
      return offset + int16_1;
    }

    internal override int ReadVarLength(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      return (int) BitConverter.ToUInt16(buffer, offset);
    }

    public override string ToString()
    {
      if (!IsNull)
        return ((string) val).PadRight(maxLength, ' ');
      return "<null>";
    }

    protected override Row.Column DoUnaryMinus()
    {
      PaddedStringValue = PaddedStringValue.TrimStart();
      return (Row.Column) this;
    }

    protected override Row.Column DoMinus(Row.Column col)
    {
      PaddedStringValue += col.PaddedStringValue.TrimStart();
      return (Row.Column) this;
    }

    protected override Row.Column DoPlus(Row.Column col)
    {
      PaddedStringValue += col.PaddedStringValue;
      return (Row.Column) this;
    }

    protected void PadBySpaces()
    {
      if (val == null)
        return;
      val = (object) ((string) val).PadRight(maxLength, ' ');
    }
  }
}
