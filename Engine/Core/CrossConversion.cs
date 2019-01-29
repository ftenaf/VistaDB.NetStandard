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
    private static readonly CrossConversion.ConversionMethod[] methods = new CrossConversion.ConversionMethod[CrossConversion.CreateEntry(VistaDBType.Unknown, VistaDBType.Unknown) + 1];
    private CultureInfo culture;

    public static IFormatProvider NumberFormat
    {
      get
      {
        return CrossConversion.numberFormat;
      }
    }

    static CrossConversion()
    {
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.NCharToByte));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.NCharToInt16));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.NCharToInt32));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.NCharToInt64));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.NCharToSingle));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.NCharToDouble));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.NCharToDecimal));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.DateTime, new CrossConversion.ConversionMethod(CrossConversion.NCharToDateTime));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.UniqueIdentifier, new CrossConversion.ConversionMethod(CrossConversion.NCharToGuid));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.VarBinary, new CrossConversion.ConversionMethod(CrossConversion.NCharToVarBinary));
      CrossConversion.SetMethod(VistaDBType.NChar, VistaDBType.Bit, new CrossConversion.ConversionMethod(CrossConversion.NCharToBit));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.ByteToNChar));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.ByteToInt16));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.ByteToInt32));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.ByteToInt64));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.ByteToSingle));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.ByteToDouble));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.ByteToDecimal));
      CrossConversion.SetMethod(VistaDBType.TinyInt, VistaDBType.Bit, new CrossConversion.ConversionMethod(CrossConversion.ByteToBoolean));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.Int16ToNChar));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.Int16ToByte));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.Int16ToInt32));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.Int16ToInt64));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.Int16ToSingle));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.Int16ToDouble));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.Int16ToDecimal));
      CrossConversion.SetMethod(VistaDBType.SmallInt, VistaDBType.Bit, new CrossConversion.ConversionMethod(CrossConversion.Int16ToBoolean));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.Int32ToNChar));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.Int32ToByte));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.Int32ToInt16));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.Int32ToInt64));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.Int32ToSingle));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.Int32ToDouble));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.Int32ToDecimal));
      CrossConversion.SetMethod(VistaDBType.Int, VistaDBType.Bit, new CrossConversion.ConversionMethod(CrossConversion.Int32ToBoolean));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.Int64ToNChar));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.Int64ToByte));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.Int64ToInt16));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.Int64ToInt32));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.Int64ToSingle));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.Int64ToDouble));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.Int64ToDecimal));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.Bit, new CrossConversion.ConversionMethod(CrossConversion.Int64ToBoolean));
      CrossConversion.SetMethod(VistaDBType.BigInt, VistaDBType.VarBinary, new CrossConversion.ConversionMethod(CrossConversion.BigIntToVarBinary));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.SingleToNChar));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.SingleToByte));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.SingleToInt16));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.SingleToInt32));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.SingleToInt64));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.SingleToDouble));
      CrossConversion.SetMethod(VistaDBType.Real, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.SingleToDecimal));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.DoubleToNChar));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.DoubleToByte));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.DoubleToInt16));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.DoubleToInt32));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.DoubleToInt64));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.DoubleToSingle));
      CrossConversion.SetMethod(VistaDBType.Float, VistaDBType.Decimal, new CrossConversion.ConversionMethod(CrossConversion.DoubleToDecimal));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.DecimalToNChar));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.DecimalToByte));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.DecimalToInt16));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.DecimalToInt32));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.DecimalToInt64));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.Real, new CrossConversion.ConversionMethod(CrossConversion.DecimalToSingle));
      CrossConversion.SetMethod(VistaDBType.Decimal, VistaDBType.Float, new CrossConversion.ConversionMethod(CrossConversion.DecimalToDouble));
      CrossConversion.SetMethod(VistaDBType.Bit, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.BooleanToNChar));
      CrossConversion.SetMethod(VistaDBType.Bit, VistaDBType.TinyInt, new CrossConversion.ConversionMethod(CrossConversion.BooleanToByte));
      CrossConversion.SetMethod(VistaDBType.Bit, VistaDBType.SmallInt, new CrossConversion.ConversionMethod(CrossConversion.BooleanToInt16));
      CrossConversion.SetMethod(VistaDBType.Bit, VistaDBType.Int, new CrossConversion.ConversionMethod(CrossConversion.BooleanToInt32));
      CrossConversion.SetMethod(VistaDBType.Bit, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.BooleanToInt64));
      CrossConversion.SetMethod(VistaDBType.DateTime, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.DateTimeToNChar));
      CrossConversion.SetMethod(VistaDBType.UniqueIdentifier, VistaDBType.NChar, new CrossConversion.ConversionMethod(CrossConversion.GuidToNChar));
      CrossConversion.SetMethod(VistaDBType.VarBinary, VistaDBType.BigInt, new CrossConversion.ConversionMethod(CrossConversion.VarBinaryToBigInt));
    }

    internal CrossConversion(CultureInfo culture)
    {
      this.culture = culture;
    }

    private static int CreateEntry(VistaDBType srcType, VistaDBType dstType)
    {
      return (int) ((VistaDBType) ((int) srcType << 8) | dstType);
    }

    private static void SetMethod(VistaDBType srcType, VistaDBType dstType, CrossConversion.ConversionMethod method)
    {
      CrossConversion.methods[CrossConversion.CreateEntry(srcType, dstType)] = method;
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
          dstValue.Value = (object) short.Parse(s, NumberStyles.Integer, CrossConversion.numberFormat);
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
          dstValue.Value = (object) int.Parse(s, CrossConversion.numberFormat);
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
          dstValue.Value = (object) long.Parse(s, NumberStyles.Integer, CrossConversion.numberFormat);
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
          dstValue.Value = (object) float.Parse(s, CrossConversion.numberFormat);
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
          if (!double.TryParse(s, NumberStyles.Any, CrossConversion.numberFormat, out result))
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
        if (!Decimal.TryParse(s, NumberStyles.Any, CrossConversion.numberFormat, out result))
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
          dstValue.Value = (object) (long.Parse(s, NumberStyles.Integer, CrossConversion.numberFormat) > 0L);
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
      dstValue.Value = obj == null ? obj : (object) ((short) obj).ToString(CrossConversion.numberFormat);
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
      dstValue.Value = obj == null ? obj : (object) ((int) obj).ToString(CrossConversion.numberFormat);
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
      dstValue.Value = obj == null ? obj : (object) ((long) obj).ToString(CrossConversion.numberFormat);
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
      dstValue.Value = obj == null ? obj : (object) ((float) obj).ToString(CrossConversion.numberFormat);
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
      dstValue.Value = obj == null ? obj : (object) ((double) obj).ToString(CrossConversion.numberFormat);
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
      dstValue.Value = obj == null ? obj : (object) ((Decimal) obj).ToString(CrossConversion.numberFormat);
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

    internal static CrossConversion.ConversionMethod Method(VistaDBType srcType, VistaDBType dstType)
    {
      int entry = CrossConversion.CreateEntry(srcType, dstType);
      return CrossConversion.methods[entry];
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
        CrossConversion.ConversionMethod conversionMethod = CrossConversion.Method(srcValue.InternalType, dstValue.InternalType);
        if (conversionMethod == null)
          throw new VistaDBException(302, srcValue.Type.ToString() + " to " + dstValue.Type.ToString());
        conversionMethod(srcValue, dstValue, this.culture);
      }
    }

    bool IConversion.ExistConvertion(VistaDBType srcType, VistaDBType dstType)
    {
      return CrossConversion.Method(srcType, dstType) != null;
    }

    internal delegate void ConversionMethod(IValue srcValue, IValue dstValue, CultureInfo culture);
  }
}
