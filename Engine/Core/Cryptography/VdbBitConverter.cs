namespace VistaDB.Engine.Core.Cryptography
{
  internal class VdbBitConverter
  {
    internal const int SizeOfByte = 1;
    internal const int SizeOfShort = 2;
    internal const int SizeOfInt = 4;
    internal const int SizeOfLong = 8;
    internal const int SizeOfFloat = 4;
    internal const int SizeOfDouble = 8;
    internal const int SizeOfGuid = 16;

    internal static int GetBytes(ushort val, byte[] buffer, int offset, int len)
    {
      for (; len > 0; --len)
      {
        buffer[offset++] = (byte) val;
        val >>= 8;
      }
      return offset;
    }

    internal static int GetBytes(uint val, byte[] buffer, int offset, int len)
    {
      for (; len > 0; --len)
      {
        buffer[offset++] = (byte) val;
        val >>= 8;
      }
      return offset;
    }

    internal static int GetBytes(ulong val, byte[] buffer, int offset, int len)
    {
      for (; len > 0; --len)
      {
        buffer[offset++] = (byte) val;
        val >>= 8;
      }
      return offset;
    }
  }
}
