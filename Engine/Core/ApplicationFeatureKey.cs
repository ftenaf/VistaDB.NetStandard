





using System;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Permissions;
using System.Text;

namespace VistaDB.Engine.Core
{
  public class ApplicationFeatureKey
  {
    private static readonly char[] IllegalCharacters = new char[4]
    {
      '0',
      '1',
      'O',
      'I'
    };
    private const string Base32Digits = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    private const long MaxChunkValue = 1099511627775;
    private const int ProductIndex = 8;
    private const int ApplicationIndex = 9;
    private const int FeatureIndex = 10;
    private const int FlagsIndex = 11;
    private const int ChecksumIndex = 12;
    private const int EndIndex = 15;
    private readonly byte[] m_RawPayload;
    private readonly string m_Key;

    internal ApplicationFeatureKey(byte[] rawPayload)
    {
      if (rawPayload == null)
        throw new ArgumentNullException(nameof (rawPayload), "Payload must be a valid array of 15 bytes.");
      if (rawPayload.Length != 15)
        throw new ArgumentOutOfRangeException(nameof (rawPayload), "Payload must be an array of 15 bytes.");
      this.m_RawPayload = new byte[15];
      for (int index = 0; index < 15; ++index)
        this.m_RawPayload[index] = rawPayload[index];
      if (!this.HasValidChecksum)
        throw new ArgumentException("Payload checksum is not valid.");
      this.m_Key = ApplicationFeatureKey.RawToString(ApplicationFeatureKey.ShaCfbCipher(this.m_RawPayload, true));
    }

    public ApplicationFeatureKey(string key)
    {
      if (key == null)
        throw new ArgumentNullException(nameof (key), "Serial number must be a valid string of length 25.");
      string str = ApplicationFeatureKey.NormalizeKey(key);
      if (str.Length != 25)
        throw new ArgumentOutOfRangeException(nameof (key), "Serial number must be of length 25.");
      this.m_Key = str;
      this.m_RawPayload = ApplicationFeatureKey.ShaCfbCipher(ApplicationFeatureKey.StringToRaw(this.m_Key), false);
    }

    public string Key
    {
      get
      {
        return this.m_Key;
      }
    }

    public byte[] RawPayload
    {
      get
      {
        return (byte[]) this.m_RawPayload.Clone();
      }
    }

    public int ProductCode
    {
      get
      {
        return (int) this.m_RawPayload[8];
      }
    }

    public int ApplicationCode
    {
      get
      {
        return (int) this.m_RawPayload[9];
      }
    }

    public int FeatureCode
    {
      get
      {
        return (int) this.m_RawPayload[10];
      }
    }

    public bool IsProduction
    {
      get
      {
        return (this.KeyFlags & LicenseKeyFlags.Production) != LicenseKeyFlags.None;
      }
    }

    public bool IsTrial
    {
      get
      {
        return (this.KeyFlags & LicenseKeyFlags.Trial) != LicenseKeyFlags.None;
      }
    }

    public bool IsExpiring
    {
      get
      {
        return (this.KeyFlags & LicenseKeyFlags.Expiring) != LicenseKeyFlags.None;
      }
    }

    public byte[] UniqueValue
    {
      get
      {
        byte[] numArray = new byte[8];
        for (int index = 0; index < 8; ++index)
          numArray[index] = this.m_RawPayload[index];
        return numArray;
      }
    }

    public bool HasValidChecksum
    {
      get
      {
        return ApplicationFeatureKey.ValidPayload(this.m_RawPayload);
      }
    }

    public static bool IsValidKey(string key)
    {
      if (key == null)
        return false;
      string serialNumber = ApplicationFeatureKey.NormalizeKey(key);
      if (string.IsNullOrEmpty(serialNumber) || serialNumber.Length != 25)
        return false;
      if (serialNumber.IndexOfAny(ApplicationFeatureKey.IllegalCharacters) >= 0)
        return false;
      try
      {
        return (int) ApplicationFeatureKey.ComputeChecksum(serialNumber) == (int) serialNumber[24];
      }
      catch (ArgumentException ex)
      {
        return false;
      }
    }

    public static string TrimKey(string rawKey, ref int selectionStart, ref int selectionEnd)
    {
      if (string.IsNullOrEmpty(rawKey))
      {
        selectionStart = 0;
        selectionEnd = 0;
        return string.Empty;
      }
      int num1 = selectionStart;
      if (num1 < 0)
        num1 = 0;
      int num2 = selectionEnd;
      if (num2 < num1)
        num2 = num1;
      int length = rawKey.Length;
      int num3 = -1;
      int num4 = -1;
      rawKey = rawKey.ToUpperInvariant();
      StringBuilder stringBuilder = new StringBuilder();
      for (int index = 0; index < length; ++index)
      {
        if (index == num1)
          num3 = stringBuilder.Length;
        if (index == num2)
          num4 = stringBuilder.Length;
        char ch = rawKey[index];
        if ("ABCDEFGHJKLMNPQRSTUVWXYZ23456789".IndexOf(ch) >= 0)
          stringBuilder.Append(ch);
      }
      selectionStart = num3 < 0 ? stringBuilder.Length : num3;
      selectionEnd = num4 < 0 ? stringBuilder.Length : num4;
      return stringBuilder.ToString();
    }

    public static int CalculateFormattedPosition(int position, int length)
    {
      if (length < 0)
        length = 0;
      if (position <= 0)
        position = 0;
      else if (position >= length)
        position = length;
      int num = position / 5;
      if (num > 4)
        num = 4;
      return position + num;
    }

    public static string FormatKey(string rawKey)
    {
      if (string.IsNullOrEmpty(rawKey))
        return string.Empty;
      string str1 = ApplicationFeatureKey.NormalizeKey(rawKey);
      string str2;
      if (str1.Length < 5)
      {
        str2 = str1;
      }
      else
      {
        string str3 = str1.Substring(0, 5) + "-";
        if (str1.Length < 10)
        {
          str2 = str3 + str1.Substring(5);
        }
        else
        {
          string str4 = str3 + str1.Substring(5, 5) + "-";
          if (str1.Length < 15)
          {
            str2 = str4 + str1.Substring(10);
          }
          else
          {
            string str5 = str4 + str1.Substring(10, 5) + "-";
            if (str1.Length < 20)
            {
              str2 = str5 + str1.Substring(15);
            }
            else
            {
              str2 = str5 + str1.Substring(15, 5) + "-";
              if (str1.Length > 20)
                str2 += str1.Substring(20);
            }
          }
        }
      }
      return str2;
    }

    public static string GenerateKey(int productCode, int applicationCode, int featureCode, byte[] uniqueKey, bool production, bool trial, bool expiring)
    {
      if (((productCode | applicationCode | featureCode) & -256) != 0)
        return (string) null;
      byte[] numArray = new byte[15];
      for (int index = 7; index >= 0; --index)
        numArray[index] = uniqueKey[index];
      numArray[8] = (byte) productCode;
      numArray[9] = (byte) applicationCode;
      numArray[10] = (byte) featureCode;
      LicenseKeyFlags licenseKeyFlags = (LicenseKeyFlags) ((production ? 1 : 0) | (trial ? 2 : 0) | (expiring ? 4 : 0));
      numArray[11] = (byte) licenseKeyFlags;
      int checksum = ApplicationFeatureKey.ComputeChecksum(numArray);
      for (int index = 14; index >= 12; --index)
      {
        numArray[index] = (byte) (checksum & (int) byte.MaxValue);
        checksum >>= 8;
      }
      return new ApplicationFeatureKey(numArray).Key;
    }

    public static string NormalizeKey(string userKey)
    {
      if (string.IsNullOrEmpty(userKey))
        return (string) null;
      return userKey.ToUpperInvariant().Trim().Replace("-", string.Empty).Replace(" ", string.Empty);
    }

    private LicenseKeyFlags KeyFlags
    {
      get
      {
        return (LicenseKeyFlags) this.m_RawPayload[11];
      }
    }

    private static string RawToString(byte[] rawPayload)
    {
      if (rawPayload == null)
        throw new ArgumentNullException(nameof (rawPayload), "Payload must be a valid array of 15 bytes.");
      if (rawPayload.Length != 15)
        throw new ArgumentOutOfRangeException(nameof (rawPayload), "Payload must be an array of 15 bytes.");
      string empty = string.Empty;
      long chunk = 0;
      for (int index = 0; index < 15; ++index)
      {
        chunk = chunk << 8 | (long) rawPayload[index];
        if (index % 5 == 4)
        {
          empty += ApplicationFeatureKey.ChunkToString(chunk);
          chunk = 0L;
        }
      }
      return empty + (object) ApplicationFeatureKey.ComputeChecksum(empty);
    }

    private static byte[] StringToRaw(string key)
    {
      if (key == null)
        throw new ArgumentNullException(nameof (key), "Serial number must be a valid string of length 25.");
      if (key.Length < 24 || key.Length > 25)
        throw new ArgumentOutOfRangeException(nameof (key), "Serial number must be of length 25.");
      if (key.Length == 25 && !ApplicationFeatureKey.IsValidKey(key))
        throw new ArgumentOutOfRangeException(nameof (key), "Serial number failed basic validity check.");
      byte[] numArray = new byte[15];
      int num = 0;
      int startIndex = 0;
      while (startIndex < 24)
      {
        long chunk = ApplicationFeatureKey.StringToChunk(key.Substring(startIndex, 8));
        for (int index = 4; index >= 0; --index)
        {
          numArray[num + index] = (byte) ((ulong) chunk & (ulong) byte.MaxValue);
          chunk >>= 8;
        }
        num += 5;
        startIndex += 8;
      }
      return numArray;
    }

    private static bool ValidPayload(byte[] payload)
    {
      if (payload == null || payload.Length != 15)
        return false;
      int checksum = ApplicationFeatureKey.ComputeChecksum(payload);
      int num = 0;
      for (int index = 12; index < 15; ++index)
        num = num << 8 | (int) payload[index];
      return num == checksum;
    }

    private int StoredChecksum
    {
      get
      {
        int num = 0;
        for (int index = 12; index < 15; ++index)
          num = num << 8 | (int) this.m_RawPayload[index];
        return num;
      }
    }

    private static char BitsToChar(long bits)
    {
      if (bits < 0L || bits >= 32L)
        throw new ArgumentOutOfRangeException(nameof (bits), "Invalid bits value for Base32 conversion.");
      return "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"[(int) bits];
    }

    private static long CharToBits(char chr)
    {
      long num = (long) "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".IndexOf(chr);
      if (num < 0L || num >= 32L)
        throw new ArgumentOutOfRangeException(nameof (chr), "Invalid Base32 character.");
      return num;
    }

    private static string ChunkToString(long chunk)
    {
      if (chunk < 0L || chunk > 1099511627775L)
        throw new ArgumentOutOfRangeException(nameof (chunk), "Invalid chunk value for 40-bit conversion.");
      char[] chArray = new char[8];
      for (int index = 7; index >= 0; --index)
      {
        long bits = chunk & 31L;
        chArray[index] = ApplicationFeatureKey.BitsToChar(bits);
        chunk >>= 5;
      }
      return new string(chArray);
    }

    private static long StringToChunk(string chunkString)
    {
      if (chunkString == null || chunkString.Length != 8)
        throw new ArgumentOutOfRangeException(nameof (chunkString), "Invalid chunk string for 40-bit conversion; must be of length 8.");
      long num = 0;
      for (int index = 0; index < 8; ++index)
        num = num << 5 | ApplicationFeatureKey.CharToBits(chunkString[index]);
      return num;
    }

    private static char ComputeChecksum(string serialNumber)
    {
      if (serialNumber == null)
        throw new ArgumentNullException(nameof (serialNumber), "Serial number must be a valid string of length 24 or 25.");
      if (serialNumber.Length < 24 || serialNumber.Length > 25)
        throw new ArgumentOutOfRangeException(nameof (serialNumber), "Serial number must be of length 24 or 25.");
      long num1 = 0;
      for (int index = 0; index < 24; ++index)
      {
        long num2 = num1 + ApplicationFeatureKey.CharToBits(serialNumber[index]);
        num1 = (num2 >> 5) + (num2 & 31L);
      }
      return ApplicationFeatureKey.BitsToChar((num1 >> 5) + (num1 & 31L));
    }

    private static int ComputeChecksum(byte[] payload)
    {
      if (payload == null)
        throw new ArgumentNullException(nameof (payload), "Payload must be a valid array of 12 to 15 bytes in length.");
      if (payload.Length < 12 || payload.Length > 15)
        throw new ArgumentOutOfRangeException(nameof (payload), "Payload must be of length 12 to 15 bytes.");
      int num1 = 0;
      int num2 = 0;
      for (int index = 0; index < 12; ++index)
      {
        num1 += (int) payload[index];
        num2 += num1;
      }
      return (num1 & (int) byte.MaxValue) << 16 ^ (num1 & 768) << 6 ^ num2 & 16383;
    }

    internal static string ArrayToBase32(byte[] array)
    {
      if (array == null)
        throw new ArgumentNullException(nameof (array), "Array argument must be a valid array of bytes.");
      if (array.Length == 0)
        return string.Empty;
      StringBuilder stringBuilder = new StringBuilder();
      int length = array.Length;
      int num1 = length + 4;
      int num2 = num1 - num1 % 5;
      long chunk = 0;
      int index = 0;
      while (index < num2)
      {
        chunk <<= 8;
        if (index < length)
          chunk |= (long) array[index];
        ++index;
        if (index % 5 == 0)
        {
          stringBuilder.Append(ApplicationFeatureKey.ChunkToString(chunk));
          chunk = 0L;
        }
      }
      return stringBuilder.ToString();
    }

    [StrongNameIdentityPermission(SecurityAction.LinkDemand, PublicKey = "00240000048000009400000006020000002400005253413100040000010001000fb2ab13e9db180c89e558e0ac32d517f34ddd626fa40293275378577e4a202d2c8095b2327eaac86dc884333d41b1763cfaad61c7bc7e9e959739f08854d71024feff627e8ef86945f430062c4d959bc50da3d27198db758498f406899ab06f1e32fcb6b213525d751e97ec0aa06776bfd21cc9992775a627c317e231d6adc7")]
    private static byte[] ShaCfbCipher(byte[] rawPayload, bool encrypting)
    {
      if (rawPayload == null)
        return (byte[]) null;
      int length = rawPayload.Length;
      if (length == 0)
        return new byte[0];
      byte[] publicKey = Assembly.GetExecutingAssembly().GetName().GetPublicKey();
      byte[] numArray1 = (byte[]) null;
      SHA1 shA1 = (SHA1) new SHA1CryptoServiceProvider();
      try
      {
        byte[] hash1 = shA1.ComputeHash(publicKey);
        int num1 = hash1.Length;
        if (num1 > 20)
          num1 = 20;
        byte[] numArray2 = Convert.FromBase64String("bJBBoOMLNW9N5/RfshY8b0GUixU=");
        byte[] buffer = new byte[20 + length];
        for (int index = 0; index < 20; ++index)
        {
          int num2 = (int) numArray2[index] ^ (int) "qldibnslclbmanajkrlg"[index];
          int num3 = (num2 ^ num2 >> 8) & (int) byte.MaxValue;
          buffer[index] = (byte) ((index < num1 ? (int) hash1[index] : 0) ^ num3);
        }
        numArray1 = new byte[length];
        for (int index = 0; index < length; ++index)
        {
          if (!shA1.CanReuseTransform && index < length - 1)
          {
            //shA1.Dispose();
            shA1 = (SHA1) new SHA1CryptoServiceProvider();
          }
          int count = 20 + index;
          byte[] hash2 = shA1.ComputeHash(buffer, 0, count);
          numArray1[index] = (byte) ((uint) rawPayload[index] ^ (uint) hash2[0]);
          buffer[count] = encrypting ? numArray1[index] : rawPayload[index];
        }
      }
      finally
      {
       // shA1.Dispose();
      }
      return numArray1;
    }
  }
}
