using System;
using System.Globalization;
using System.Text;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class CrossConversion : IConversion
  {
    private static readonly IFormatProvider numberFormat = (IFormatProvider) CultureInfo.InvariantCulture.NumberFormat;
    private static readonly ConversionMethod[] methods = new ConversionMethod[CreateEntry(VistaDBType.Unknown, VistaDBType.Unknown) + 1];
    private CultureInfo culture;

    public static IFormatProvider NumberFormat
    {
      get
      {
        return numberFormat;
      }
    }

    static CrossConversion()
    {
            SetMethod(VistaDBType.NChar, VistaDBType.TinyInt, new ConversionMethod(NCharToByte));
            SetMethod(VistaDBType.NChar, VistaDBType.SmallInt, new ConversionMethod(NCharToInt16));
            SetMethod(VistaDBType.NChar, VistaDBType.Int, new ConversionMethod(NCharToInt32));
            SetMethod(VistaDBType.NChar, VistaDBType.BigInt, new ConversionMethod(NCharToInt64));
            SetMethod(VistaDBType.NChar, VistaDBType.Real, new ConversionMethod(NCharToSingle));
            SetMethod(VistaDBType.NChar, VistaDBType.Float, new ConversionMethod(NCharToDouble));
            SetMethod(VistaDBType.NChar, VistaDBType.Decimal, new ConversionMethod(NCharToDecimal));
            SetMethod(VistaDBType.NChar, VistaDBType.DateTime, new ConversionMethod(NCharToDateTime));
            SetMethod(VistaDBType.NChar, VistaDBType.UniqueIdentifier, new ConversionMethod(NCharToGuid));
            SetMethod(VistaDBType.NChar, VistaDBType.VarBinary, new ConversionMethod(NCharToVarBinary));
            SetMethod(VistaDBType.NChar, VistaDBType.Bit, new ConversionMethod(NCharToBit));
            SetMethod(VistaDBType.TinyInt, VistaDBType.NChar, new ConversionMethod(ByteToNChar));
            SetMethod(VistaDBType.TinyInt, VistaDBType.SmallInt, new ConversionMethod(ByteToInt16));
            SetMethod(VistaDBType.TinyInt, VistaDBType.Int, new ConversionMethod(ByteToInt32));
            SetMethod(VistaDBType.TinyInt, VistaDBType.BigInt, new ConversionMethod(ByteToInt64));
            SetMethod(VistaDBType.TinyInt, VistaDBType.Real, new ConversionMethod(ByteToSingle));
            SetMethod(VistaDBType.TinyInt, VistaDBType.Float, new ConversionMethod(ByteToDouble));
            SetMethod(VistaDBType.TinyInt, VistaDBType.Decimal, new ConversionMethod(ByteToDecimal));
            SetMethod(VistaDBType.TinyInt, VistaDBType.Bit, new ConversionMethod(ByteToBoolean));
            SetMethod(VistaDBType.SmallInt, VistaDBType.NChar, new ConversionMethod(Int16ToNChar));
            SetMethod(VistaDBType.SmallInt, VistaDBType.TinyInt, new ConversionMethod(Int16ToByte));
            SetMethod(VistaDBType.SmallInt, VistaDBType.Int, new ConversionMethod(Int16ToInt32));
            SetMethod(VistaDBType.SmallInt, VistaDBType.BigInt, new ConversionMethod(Int16ToInt64));
            SetMethod(VistaDBType.SmallInt, VistaDBType.Real, new ConversionMethod(Int16ToSingle));
            SetMethod(VistaDBType.SmallInt, VistaDBType.Float, new ConversionMethod(Int16ToDouble));
            SetMethod(VistaDBType.SmallInt, VistaDBType.Decimal, new ConversionMethod(Int16ToDecimal));
            SetMethod(VistaDBType.SmallInt, VistaDBType.Bit, new ConversionMethod(Int16ToBoolean));
            SetMethod(VistaDBType.Int, VistaDBType.NChar, new ConversionMethod(Int32ToNChar));
            SetMethod(VistaDBType.Int, VistaDBType.TinyInt, new ConversionMethod(Int32ToByte));
            SetMethod(VistaDBType.Int, VistaDBType.SmallInt, new ConversionMethod(Int32ToInt16));
            SetMethod(VistaDBType.Int, VistaDBType.BigInt, new ConversionMethod(Int32ToInt64));
            SetMethod(VistaDBType.Int, VistaDBType.Real, new ConversionMethod(Int32ToSingle));
            SetMethod(VistaDBType.Int, VistaDBType.Float, new ConversionMethod(Int32ToDouble));
            SetMethod(VistaDBType.Int, VistaDBType.Decimal, new ConversionMethod(Int32ToDecimal));
            SetMethod(VistaDBType.Int, VistaDBType.Bit, new ConversionMethod(Int32ToBoolean));
            SetMethod(VistaDBType.BigInt, VistaDBType.NChar, new ConversionMethod(Int64ToNChar));
            SetMethod(VistaDBType.BigInt, VistaDBType.TinyInt, new ConversionMethod(Int64ToByte));
            SetMethod(VistaDBType.BigInt, VistaDBType.SmallInt, new ConversionMethod(Int64ToInt16));
            SetMethod(VistaDBType.BigInt, VistaDBType.Int, new ConversionMethod(Int64ToInt32));
            SetMethod(VistaDBType.BigInt, VistaDBType.Real, new ConversionMethod(Int64ToSingle));
            SetMethod(VistaDBType.BigInt, VistaDBType.Float, new ConversionMethod(Int64ToDouble));
            SetMethod(VistaDBType.BigInt, VistaDBType.Decimal, new ConversionMethod(Int64ToDecimal));
            SetMethod(VistaDBType.BigInt, VistaDBType.Bit, new ConversionMethod(Int64ToBoolean));
            SetMethod(VistaDBType.BigInt, VistaDBType.VarBinary, new ConversionMethod(BigIntToVarBinary));
            SetMethod(VistaDBType.Real, VistaDBType.NChar, new ConversionMethod(SingleToNChar));
            SetMethod(VistaDBType.Real, VistaDBType.TinyInt, new ConversionMethod(SingleToByte));
            SetMethod(VistaDBType.Real, VistaDBType.SmallInt, new ConversionMethod(SingleToInt16));
            SetMethod(VistaDBType.Real, VistaDBType.Int, new ConversionMethod(SingleToInt32));
            SetMethod(VistaDBType.Real, VistaDBType.BigInt, new ConversionMethod(SingleToInt64));
            SetMethod(VistaDBType.Real, VistaDBType.Float, new ConversionMethod(SingleToDouble));
            SetMethod(VistaDBType.Real, VistaDBType.Decimal, new ConversionMethod(SingleToDecimal));
            SetMethod(VistaDBType.Float, VistaDBType.NChar, new ConversionMethod(DoubleToNChar));
            SetMethod(VistaDBType.Float, VistaDBType.TinyInt, new ConversionMethod(DoubleToByte));
            SetMethod(VistaDBType.Float, VistaDBType.SmallInt, new ConversionMethod(DoubleToInt16));
            SetMethod(VistaDBType.Float, VistaDBType.Int, new ConversionMethod(DoubleToInt32));
            SetMethod(VistaDBType.Float, VistaDBType.BigInt, new ConversionMethod(DoubleToInt64));
            SetMethod(VistaDBType.Float, VistaDBType.Real, new ConversionMethod(DoubleToSingle));
            SetMethod(VistaDBType.Float, VistaDBType.Decimal, new ConversionMethod(DoubleToDecimal));
            SetMethod(VistaDBType.Decimal, VistaDBType.NChar, new ConversionMethod(DecimalToNChar));
            SetMethod(VistaDBType.Decimal, VistaDBType.TinyInt, new ConversionMethod(DecimalToByte));
            SetMethod(VistaDBType.Decimal, VistaDBType.SmallInt, new ConversionMethod(DecimalToInt16));
            SetMethod(VistaDBType.Decimal, VistaDBType.Int, new ConversionMethod(DecimalToInt32));
            SetMethod(VistaDBType.Decimal, VistaDBType.BigInt, new ConversionMethod(DecimalToInt64));
            SetMethod(VistaDBType.Decimal, VistaDBType.Real, new ConversionMethod(DecimalToSingle));
            SetMethod(VistaDBType.Decimal, VistaDBType.Float, new ConversionMethod(DecimalToDouble));
            SetMethod(VistaDBType.Bit, VistaDBType.NChar, new ConversionMethod(BooleanToNChar));
            SetMethod(VistaDBType.Bit, VistaDBType.TinyInt, new ConversionMethod(BooleanToByte));
            SetMethod(VistaDBType.Bit, VistaDBType.SmallInt, new ConversionMethod(BooleanToInt16));
            SetMethod(VistaDBType.Bit, VistaDBType.Int, new ConversionMethod(BooleanToInt32));
            SetMethod(VistaDBType.Bit, VistaDBType.BigInt, new ConversionMethod(BooleanToInt64));
            SetMethod(VistaDBType.DateTime, VistaDBType.NChar, new ConversionMethod(DateTimeToNChar));
            SetMethod(VistaDBType.UniqueIdentifier, VistaDBType.NChar, new ConversionMethod(GuidToNChar));
            SetMethod(VistaDBType.VarBinary, VistaDBType.BigInt, new ConversionMethod(VarBinaryToBigInt));
    }

    internal CrossConversion(CultureInfo culture)
    {
      this.culture = culture;
    }

    private static int CreateEntry(VistaDBType srcType, VistaDBType dstType)
    {
      return (int) ((VistaDBType) ((int) srcType << 8) | dstType);
    }

    private static void SetMethod(VistaDBType srcType, VistaDBType dstType, ConversionMethod method)
    {
            methods[CreateEntry(srcType, dstType)] = method;
    }

    private static void NCharToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) byte.Parse(s);
        else
          dstValue.Value = (object) (byte) 0;
      }
    }

    private static void NCharToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) short.Parse(s, NumberStyles.Integer, numberFormat);
        else
          dstValue.Value = (object) (short) 0;
      }
    }

    private static void NCharToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) int.Parse(s, numberFormat);
        else
          dstValue.Value = (object) 0;
      }
    }

    private static void NCharToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) long.Parse(s, NumberStyles.Integer, numberFormat);
        else
          dstValue.Value = (object) 0L;
      }
    }

    private static void NCharToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) float.Parse(s, numberFormat);
        else
          dstValue.Value = (object) 0.0f;
      }
    }

    private static void NCharToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
        {
          double result;
          if (!double.TryParse(s, NumberStyles.Any, numberFormat, out result))
            throw new VistaDBException(302, srcValue.Type.ToString() + " to " + dstValue.Type.ToString());
          dstValue.Value = (object) result;
        }
        else
          dstValue.Value = (object) 0.0;
      }
    }

    private static void NCharToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length <= 0)
          throw new VistaDBException(302, srcValue.Type.ToString() + " to " + dstValue.Type.ToString());
        Decimal result;
        if (!Decimal.TryParse(s, NumberStyles.Any, numberFormat, out result))
          throw new VistaDBException(302, srcValue.Type.ToString() + " to " + dstValue.Type.ToString());
        dstValue.Value = (object) result;
      }
    }

    private static void NCharToDateTime(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) DateTime.Parse(s, (IFormatProvider) culture.DateTimeFormat);
        else
          dstValue.Value = (object) new DateTime(1900, 1, 1);
      }
    }

    private static void NCharToGuid(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) new Guid((string) obj);
    }

    private static void NCharToVarBinary(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      dstValue.Value = (object) Encoding.GetEncoding(((Row.Column) srcValue).CodePage).GetBytes((string) srcValue.Value);
    }

    private static void NCharToBit(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      if (srcValue.Value == null)
      {
        dstValue.Value = (object) null;
      }
      else
      {
        string s = ((string) srcValue.Value).TrimEnd();
        if (s.Length > 0)
          dstValue.Value = (object) (long.Parse(s, NumberStyles.Integer, numberFormat) > 0L);
        else
          dstValue.Value = (object) false;
      }
    }

    private static void ByteToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((byte) obj).ToString();
    }

    private static void ByteToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) (byte) obj;
    }

    private static void ByteToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (int) (byte) obj;
    }

    private static void ByteToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (long) (byte) obj;
    }

    private static void ByteToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (float) (byte) obj;
    }

    private static void ByteToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (double) (byte) obj;
    }

    private static void ByteToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (Decimal) ((byte) obj);
    }

    private static void ByteToBoolean(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((byte) obj != (byte) 0);
    }

    private static void Int16ToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((short) obj).ToString(numberFormat);
    }

    private static void Int16ToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) (short) obj;
    }

    private static void Int16ToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (int) (short) obj;
    }

    private static void Int16ToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (long) (short) obj;
    }

    private static void Int16ToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (float) (short) obj;
    }

    private static void Int16ToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (double) (short) obj;
    }

    private static void Int16ToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (Decimal) ((short) obj);
    }

    private static void Int16ToBoolean(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((short) obj != (short) 0);
    }

    private static void Int32ToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((int) obj).ToString(numberFormat);
    }

    private static void Int32ToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) (int) obj;
    }

    private static void Int32ToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) (int) obj;
    }

    private static void Int32ToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (long) (int) obj;
    }

    private static void Int32ToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (float) (int) obj;
    }

    private static void Int32ToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (double) (int) obj;
    }

    private static void Int32ToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (Decimal) ((int) obj);
    }

    private static void Int32ToBoolean(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((int) obj != 0);
    }

    private static void Int64ToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((long) obj).ToString(numberFormat);
    }

    private static void Int64ToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) (long) obj;
    }

    private static void Int64ToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) (long) obj;
    }

    private static void Int64ToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (int) (long) obj;
    }

    private static void Int64ToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (float) (long) obj;
    }

    private static void Int64ToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (double) (long) obj;
    }

    private static void Int64ToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (Decimal) ((long) obj);
    }

    private static void Int64ToBoolean(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((long) obj != 0L);
    }

    private static void SingleToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((float) obj).ToString(numberFormat);
    }

    private static void SingleToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) (float) obj;
    }

    private static void SingleToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) (float) obj;
    }

    private static void SingleToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (int) (float) obj;
    }

    private static void SingleToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (long) (float) obj;
    }

    private static void SingleToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (double) (float) obj;
    }

    private static void SingleToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (Decimal) ((float) obj);
    }

    private static void DoubleToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((double) obj).ToString(numberFormat);
    }

    private static void DoubleToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) (double) obj;
    }

    private static void DoubleToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) (double) obj;
    }

    private static void DoubleToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (int) (double) obj;
    }

    private static void DoubleToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (long) (double) obj;
    }

    private static void DoubleToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (float) (double) obj;
    }

    private static void DoubleToDecimal(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (Decimal) ((double) obj);
    }

    private static void DecimalToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((Decimal) obj).ToString(numberFormat);
    }

    private static void DecimalToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) ((Decimal) obj);
    }

    private static void DecimalToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) ((Decimal) obj);
    }

    private static void DecimalToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (int) ((Decimal) obj);
    }

    private static void DecimalToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (long) ((Decimal) obj);
    }

    private static void DecimalToSingle(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (float) ((Decimal) obj);
    }

    private static void DecimalToDouble(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (double) ((Decimal) obj);
    }

    private static void BooleanToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : ((bool) srcValue.Value ? (object) "true" : (object) "false");
    }

    private static void BooleanToByte(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (byte) ((bool) srcValue.Value ? 1 : 0);
    }

    private static void BooleanToInt16(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) (short) ((bool) srcValue.Value ? 1 : 0);
    }

    private static void BooleanToInt32(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((bool) srcValue.Value ? 1 : 0);
    }

    private static void BooleanToInt64(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((bool) srcValue.Value ? 1L : 0L);
    }

    private static void DateTimeToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) ((DateTime) obj).ToString((IFormatProvider) culture.DateTimeFormat);
    }

    private static void GuidToNChar(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      object obj = srcValue.Value;
      dstValue.Value = obj == null ? obj : (object) obj.ToString();
    }

    private static void VarBinaryToBigInt(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      byte[] numArray = (byte[]) srcValue.Value;
      dstValue.Value = numArray == null || numArray.Length != 8 ? (object) null : (object) BitConverter.ToInt64(numArray, 0);
    }

    private static void BigIntToVarBinary(IValue srcValue, IValue dstValue, CultureInfo culture)
    {
      dstValue.Value = (object) BitConverter.GetBytes((long) srcValue.Value);
    }

    internal static ConversionMethod Method(VistaDBType srcType, VistaDBType dstType)
    {
      int entry = CreateEntry(srcType, dstType);
      return methods[entry];
    }

    public void Convert(IValue srcValue, IValue dstValue)
    {
      if (srcValue == null)
        throw new VistaDBException(302, "Unable to cast a NULL source value to a " + dstValue.Type.ToString() + " output value.");
      if (dstValue == null)
        throw new VistaDBException(302, "Unable to cast a source of " + srcValue.Type.ToString() + " to a NULL destination.");
      if (dstValue.InternalType == srcValue.InternalType)
        dstValue.Value = srcValue.Value;
      else if (srcValue.IsNull)
      {
        dstValue.Value = (object) null;
      }
      else
      {
                ConversionMethod conversionMethod = Method(srcValue.InternalType, dstValue.InternalType);
        if (conversionMethod == null)
          throw new VistaDBException(302, srcValue.Type.ToString() + " to " + dstValue.Type.ToString());
        conversionMethod(srcValue, dstValue, culture);
      }
    }

    bool IConversion.ExistConvertion(VistaDBType srcType, VistaDBType dstType)
    {
      return Method(srcType, dstType) != null;
    }

    internal delegate void ConversionMethod(IValue srcValue, IValue dstValue, CultureInfo culture);
  }
}
