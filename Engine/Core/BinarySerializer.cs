using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;

namespace VistaDB.Engine.Core
{
  public static class BinarySerializer
  {
    private static readonly Encoding s_Encoding = (Encoding) new UTF8Encoding();
    private static readonly bool s_MonoRuntime;

    static BinarySerializer()
    {
      if (Type.GetType("Mono.Runtime") == null)
        BinarySerializer.s_MonoRuntime = false;
      else
        BinarySerializer.s_MonoRuntime = true;
    }

    public static void SerializeValue(Stream stream, bool hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(bool hostValue)
    {
      if (!hostValue)
        return new byte[1];
      return new byte[1]{ (byte) 1 };
    }

    public static void SerializeValue(Stream stream, Guid hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(Guid hostValue)
    {
      return hostValue.ToByteArray();
    }

    public static void SerializeValue(Stream stream, string hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(string hostValue)
    {
      if (hostValue == null)
        return BinarySerializer.SerializeValue(-1);
      if (string.IsNullOrEmpty(hostValue))
        return BinarySerializer.SerializeValue(0);
      byte[] bytes = BinarySerializer.s_Encoding.GetBytes(hostValue);
      byte[] numArray = new byte[4 + bytes.Length];
      Array.Copy((Array) BinarySerializer.SerializeValue(bytes.Length), (Array) numArray, 4);
      Array.Copy((Array) bytes, 0, (Array) numArray, 4, bytes.Length);
      return numArray;
    }

    public static void SerializeValue(Stream stream, DateTime hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(DateTime hostValue)
    {
      return BinarySerializer.SerializeValue(new DateTimeOffset(hostValue));
    }

    public static void SerializeValue(Stream stream, DateTimeOffset hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(DateTimeOffset hostValue)
    {
      return BinarySerializer.SerializeValue(hostValue.ToString("o", (IFormatProvider) CultureInfo.InvariantCulture));
    }

    public static void SerializeValue(Stream stream, long hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(long hostValue)
    {
      return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(hostValue));
    }

    public static void SerializeValue(Stream stream, ulong hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(ulong hostValue)
    {
      return BitConverter.GetBytes(IPAddress.HostToNetworkOrder((long) hostValue));
    }

    public static void SerializeValue(Stream stream, int hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(int hostValue)
    {
      return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(hostValue));
    }

    public static void SerializeValue(Stream stream, uint hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(uint hostValue)
    {
      return BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int) hostValue));
    }

    public static void SerializeValue(Stream stream, short hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(short hostValue)
    {
      return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(hostValue));
    }

    public static void SerializeValue(Stream stream, ushort hostValue)
    {
      byte[] buffer = BinarySerializer.SerializeValue(hostValue);
      stream.Write(buffer, 0, buffer.Length);
    }

    public static byte[] SerializeValue(ushort hostValue)
    {
      return BitConverter.GetBytes(IPAddress.HostToNetworkOrder((short) hostValue));
    }

    public static void DeserializeValue(Stream networkBytes, out long hostValue)
    {
      byte[] buffer = new byte[8];
      networkBytes.Read(buffer, 0, buffer.Length);
      long int64 = BitConverter.ToInt64(buffer, 0);
      hostValue = IPAddress.NetworkToHostOrder(int64);
    }

    public static void DeserializeValue(Stream networkBytes, out ulong hostValue)
    {
      byte[] buffer = new byte[8];
      networkBytes.Read(buffer, 0, buffer.Length);
      long int64 = BitConverter.ToInt64(buffer, 0);
      hostValue = (ulong) IPAddress.NetworkToHostOrder(int64);
    }

    public static void DeserializeValue(Stream networkBytes, out int hostValue)
    {
      byte[] buffer = new byte[4];
      networkBytes.Read(buffer, 0, buffer.Length);
      int int32 = BitConverter.ToInt32(buffer, 0);
      hostValue = IPAddress.NetworkToHostOrder(int32);
    }

    public static void DeserializeValue(Stream networkBytes, out uint hostValue)
    {
      byte[] buffer = new byte[4];
      networkBytes.Read(buffer, 0, buffer.Length);
      int int32 = BitConverter.ToInt32(buffer, 0);
      hostValue = (uint) IPAddress.NetworkToHostOrder(int32);
    }

    public static void DeserializeValue(Stream networkBytes, out short hostValue)
    {
      byte[] buffer = new byte[2];
      networkBytes.Read(buffer, 0, buffer.Length);
      short int16 = BitConverter.ToInt16(buffer, 0);
      hostValue = IPAddress.NetworkToHostOrder(int16);
    }

    public static void DeserializeValue(Stream networkBytes, out ushort hostValue)
    {
      byte[] buffer = new byte[2];
      networkBytes.Read(buffer, 0, buffer.Length);
      short int16 = BitConverter.ToInt16(buffer, 0);
      hostValue = (ushort) IPAddress.NetworkToHostOrder(int16);
    }

    public static void DeserializeValue(Stream networkBytes, out DateTime hostValue)
    {
      DateTimeOffset hostValue1;
      BinarySerializer.DeserializeValue(networkBytes, out hostValue1);
      hostValue = hostValue1.DateTime;
    }

    public static void DeserializeValue(Stream networkBytes, out DateTimeOffset hostValue)
    {
      string hostValue1;
      BinarySerializer.DeserializeValue(networkBytes, out hostValue1);
      if (!BinarySerializer.s_MonoRuntime)
      {
        hostValue = DateTimeOffset.ParseExact(hostValue1, "o", (IFormatProvider) null);
      }
      else
      {
        if (hostValue1.Length != 33 || hostValue1[4] != '-' || (hostValue1[7] != '-' || hostValue1[10] != 'T') || (hostValue1[13] != ':' || hostValue1[16] != ':' || (hostValue1[19] != '.' || hostValue1[30] != ':')))
          throw new FormatException(string.Format("Unrecognized format for DateTimeOffset deserialization: \"{0}\"", (object) hostValue1));
        int year = int.Parse(hostValue1.Substring(0, 4), NumberStyles.None);
        int month = int.Parse(hostValue1.Substring(5, 2), NumberStyles.None);
        int day = int.Parse(hostValue1.Substring(8, 2), NumberStyles.None);
        int hour = int.Parse(hostValue1.Substring(11, 2), NumberStyles.None);
        int minute = int.Parse(hostValue1.Substring(14, 2), NumberStyles.None);
        int second = int.Parse(hostValue1.Substring(17, 2), NumberStyles.None);
        int num = int.Parse(hostValue1.Substring(20, 7), NumberStyles.None);
        TimeSpan offset = new TimeSpan(int.Parse(hostValue1.Substring(28, 2), NumberStyles.None), int.Parse(hostValue1.Substring(31, 2), NumberStyles.None), 0);
        char ch = hostValue1[27];
        switch (ch)
        {
          case '+':
            hostValue = new DateTimeOffset(year, month, day, hour, minute, second, offset).AddTicks((long) num);
            break;
          case '-':
            offset = offset.Negate();
            goto case '+';
          default:
            throw new FormatException(string.Format("Unrecognized character for time zone offset sign: '{0}'", (object) ch));
        }
      }
    }

    public static void DeserializeValue(Stream networkBytes, out Guid hostValue)
    {
      byte[] numArray = new byte[16];
      networkBytes.Read(numArray, 0, numArray.Length);
      hostValue = new Guid(numArray);
    }

    public static void DeserializeValue(Stream networkBytes, out bool hostValue)
    {
      if (networkBytes.ReadByte() == 0)
        hostValue = false;
      else
        hostValue = true;
    }

    public static void DeserializeValue(Stream networkBytes, out string hostValue)
    {
      int hostValue1;
      BinarySerializer.DeserializeValue(networkBytes, out hostValue1);
      if (hostValue1 > 0)
      {
        byte[] numArray = new byte[hostValue1];
        networkBytes.Read(numArray, 0, numArray.Length);
        hostValue = BinarySerializer.s_Encoding.GetString(numArray);
      }
      else if (hostValue1 == 0)
        hostValue = string.Empty;
      else
        hostValue = (string) null;
    }
  }
}
