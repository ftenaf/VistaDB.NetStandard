using System;
using VistaDB.Engine.Core;

namespace VistaDB.Engine.SQL
{
  internal class Utils
  {
    public static VistaDBType DefaultCharType = VistaDBType.NChar;
    private static Type[] VistaDBToSystem = new Type[25]{ typeof (object), typeof (string), typeof (string), typeof (string), typeof (string), typeof (string), typeof (string), typeof (object), typeof (byte), typeof (short), typeof (int), typeof (long), typeof (float), typeof (double), typeof (Decimal), typeof (Decimal), typeof (Decimal), typeof (bool), typeof (object), typeof (DateTime), typeof (byte[]), typeof (byte[]), typeof (Guid), typeof (DateTime), typeof (long) };

    public static Type GetSystemType(VistaDBType dataType)
    {
      return Utils.VistaDBToSystem[(int) dataType];
    }

    public static VistaDBType GetVistaDBType(Type type)
    {
      switch (Type.GetTypeCode(type))
      {
        case TypeCode.Empty:
        case TypeCode.DBNull:
          return VistaDBType.Unknown;
        case TypeCode.Object:
          if (type == typeof (Guid))
            return VistaDBType.UniqueIdentifier;
          if (type == typeof (byte[]))
            return VistaDBType.Image;
          break;
        case TypeCode.Boolean:
          return VistaDBType.Bit;
        case TypeCode.Char:
          return VistaDBType.NChar;
        case TypeCode.SByte:
          return VistaDBType.TinyInt;
        case TypeCode.Byte:
          return VistaDBType.TinyInt;
        case TypeCode.Int16:
          return VistaDBType.SmallInt;
        case TypeCode.UInt16:
          return VistaDBType.SmallInt;
        case TypeCode.Int32:
          return VistaDBType.Int;
        case TypeCode.UInt32:
          return VistaDBType.Int;
        case TypeCode.Int64:
          return VistaDBType.BigInt;
        case TypeCode.UInt64:
          return VistaDBType.BigInt;
        case TypeCode.Single:
          return VistaDBType.Real;
        case TypeCode.Double:
          return VistaDBType.Float;
        case TypeCode.Decimal:
          return VistaDBType.Decimal;
        case TypeCode.DateTime:
          return VistaDBType.DateTime;
        case TypeCode.String:
          return VistaDBType.NChar;
      }
      return VistaDBType.Unknown;
    }

    public static int CompareRank(VistaDBType dataType1, VistaDBType dataType2)
    {
      return Row.Column.Rank(dataType1) - Row.Column.Rank(dataType2);
    }

    public static VistaDBType GetMaxDataType(VistaDBType dataType1, VistaDBType dataType2)
    {
      if (Utils.CompareRank(dataType1, dataType2) < 0)
        return dataType2;
      return dataType1;
    }

    public static VistaDBType GetMaxNumericDataType(VistaDBType dataType1, VistaDBType dataType2)
    {
      bool flag1 = Utils.IsNumericDataType(dataType1);
      bool flag2 = Utils.IsNumericDataType(dataType2);
      if (flag1 && flag2)
        return Utils.GetMaxDataType(dataType1, dataType2);
      if (flag1)
        return dataType1;
      if (flag2)
        return dataType2;
      return VistaDBType.Float;
    }

    public static bool IsCharacterDataType(VistaDBType dataType)
    {
      switch (dataType)
      {
        case VistaDBType.Char:
        case VistaDBType.NChar:
        case VistaDBType.VarChar:
        case VistaDBType.NVarChar:
        case VistaDBType.Text:
        case VistaDBType.NText:
          return true;
        default:
          return false;
      }
    }

    public static bool IsNumericDataType(VistaDBType dataType)
    {
      switch (dataType)
      {
        case VistaDBType.TinyInt:
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
        case VistaDBType.Real:
        case VistaDBType.Float:
        case VistaDBType.Decimal:
        case VistaDBType.Money:
        case VistaDBType.SmallMoney:
          return true;
        default:
          return false;
      }
    }

    public static bool IsIntegerDataType(VistaDBType dataType)
    {
      switch (dataType)
      {
        case VistaDBType.TinyInt:
        case VistaDBType.SmallInt:
        case VistaDBType.Int:
        case VistaDBType.BigInt:
          return true;
        default:
          return false;
      }
    }

    public static bool IsDateDataType(VistaDBType dataType)
    {
      switch (dataType)
      {
        case VistaDBType.DateTime:
        case VistaDBType.SmallDateTime:
          return true;
        default:
          return false;
      }
    }

    public static bool IsLongDataType(VistaDBType dataType)
    {
      switch (dataType)
      {
        case VistaDBType.Text:
        case VistaDBType.NText:
        case VistaDBType.Image:
        case VistaDBType.VarBinary:
          return true;
        default:
          return false;
      }
    }

    public static bool CompatibleTypes(VistaDBType dataType1, VistaDBType dataType2)
    {
      dataType1 = Row.Column.GetInternalType(dataType1);
      dataType2 = Row.Column.GetInternalType(dataType2);
      if (dataType1 != VistaDBType.Unknown && dataType2 != VistaDBType.Unknown && dataType1 != dataType2)
        return CrossConversion.Method(dataType1, dataType2) != null;
      return true;
    }

    public static int GetByte(char lowHex)
    {
      if (lowHex >= '0' && lowHex <= '9')
        return (int) lowHex - 48;
      if (lowHex >= 'A' && lowHex <= 'F')
        return 10 + (int) lowHex - 65;
      return 10 + (int) lowHex - 97;
    }

    public static byte[] StringToBinary(string hex)
    {
      int length = hex.Length / 2 - 1;
      if (length == 0)
        return new byte[1]{ (byte) 0 };
      if (hex.Length % 2 != 0)
        ++length;
      byte[] numArray = new byte[length];
      int index1 = hex.Length - 1;
      for (int index2 = length - 1; index2 >= 0; --index2)
      {
        numArray[index2] = index1 != 2 ? (byte) (Utils.GetByte(hex[index1]) | Utils.GetByte(hex[index1 - 1]) << 4) : (byte) Utils.GetByte(hex[index1]);
        index1 -= 2;
      }
      return numArray;
    }

    public static bool IsReadOnly(VistaDBDatabaseOpenMode mode)
    {
      switch (mode)
      {
        case VistaDBDatabaseOpenMode.ExclusiveReadOnly:
        case VistaDBDatabaseOpenMode.NonexclusiveReadOnly:
        case VistaDBDatabaseOpenMode.SharedReadOnly:
          return true;
        default:
          return false;
      }
    }
  }
}
