using System;
using System.Text;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class Md5
  {
    private byte[] paddingBuffer = new byte[64];

    internal Md5()
    {
      paddingBuffer[0] = (byte) 128;
      for (int index = 1; index < paddingBuffer.Length; ++index)
        paddingBuffer[index] = (byte) 0;
    }

    private void Encode(byte[] source, int srcOffset, uint[] target, int trgOffset, int count)
    {
      count /= 4;
      for (int index = 0; index < count; ++index)
      {
        target[trgOffset] = (uint) source[srcOffset++];
        target[trgOffset] = target[trgOffset] | (uint) source[srcOffset++] << 8;
        target[trgOffset] = target[trgOffset] | (uint) source[srcOffset++] << 16;
        target[trgOffset] = target[trgOffset] | (uint) source[srcOffset++] << 24;
        ++trgOffset;
      }
    }

    private void Decode(uint[] source, int srcOffset, byte[] target, int trgOffset, int count)
    {
      for (int index = 0; index < count; ++index)
      {
        target[trgOffset++] = (byte) (source[srcOffset] & (uint) byte.MaxValue);
        target[trgOffset++] = (byte) (source[srcOffset] >> 8 & (uint) byte.MaxValue);
        target[trgOffset++] = (byte) (source[srcOffset] >> 16 & (uint) byte.MaxValue);
        target[trgOffset++] = (byte) (source[srcOffset] >> 24 & (uint) byte.MaxValue);
        ++srcOffset;
      }
    }

    private void Transform(byte[] buffer, int offset, uint[] state)
    {
      uint[] target = new uint[16];
      Encode(buffer, 0, target, 0, 64);
      uint a1 = state[0];
      uint a2 = state[1];
      uint a3 = state[2];
      uint a4 = state[3];
      FF(ref a1, a2, a3, a4, target[offset], (byte) 7, 3614090360U);
      FF(ref a4, a1, a2, a3, target[offset + 1], (byte) 12, 3905402710U);
      FF(ref a3, a4, a1, a2, target[offset + 2], (byte) 17, 606105819U);
      FF(ref a2, a3, a4, a1, target[offset + 3], (byte) 22, 3250441966U);
      FF(ref a1, a2, a3, a4, target[offset + 4], (byte) 7, 4118548399U);
      FF(ref a4, a1, a2, a3, target[offset + 5], (byte) 12, 1200080426U);
      FF(ref a3, a4, a1, a2, target[offset + 6], (byte) 17, 2821735955U);
      FF(ref a2, a3, a4, a1, target[offset + 7], (byte) 22, 4249261313U);
      FF(ref a1, a2, a3, a4, target[offset + 8], (byte) 7, 1770035416U);
      FF(ref a4, a1, a2, a3, target[offset + 9], (byte) 12, 2336552879U);
      FF(ref a3, a4, a1, a2, target[offset + 10], (byte) 17, 4294925233U);
      FF(ref a2, a3, a4, a1, target[offset + 11], (byte) 22, 2304563134U);
      FF(ref a1, a2, a3, a4, target[offset + 12], (byte) 7, 1804603682U);
      FF(ref a4, a1, a2, a3, target[offset + 13], (byte) 12, 4254626195U);
      FF(ref a3, a4, a1, a2, target[offset + 14], (byte) 17, 2792965006U);
      FF(ref a2, a3, a4, a1, target[offset + 15], (byte) 22, 1236535329U);
      GG(ref a1, a2, a3, a4, target[offset + 1], (byte) 5, 4129170786U);
      GG(ref a4, a1, a2, a3, target[offset + 6], (byte) 9, 3225465664U);
      GG(ref a3, a4, a1, a2, target[offset + 11], (byte) 14, 643717713U);
      GG(ref a2, a3, a4, a1, target[offset], (byte) 20, 3921069994U);
      GG(ref a1, a2, a3, a4, target[offset + 5], (byte) 5, 3593408605U);
      GG(ref a4, a1, a2, a3, target[offset + 10], (byte) 9, 38016083U);
      GG(ref a3, a4, a1, a2, target[offset + 15], (byte) 14, 3634488961U);
      GG(ref a2, a3, a4, a1, target[offset + 4], (byte) 20, 3889429448U);
      GG(ref a1, a2, a3, a4, target[offset + 9], (byte) 5, 568446438U);
      GG(ref a4, a1, a2, a3, target[offset + 14], (byte) 9, 3275163606U);
      GG(ref a3, a4, a1, a2, target[offset + 3], (byte) 14, 4107603335U);
      GG(ref a2, a3, a4, a1, target[offset + 8], (byte) 20, 1163531501U);
      GG(ref a1, a2, a3, a4, target[offset + 13], (byte) 5, 2850285829U);
      GG(ref a4, a1, a2, a3, target[offset + 2], (byte) 9, 4243563512U);
      GG(ref a3, a4, a1, a2, target[offset + 7], (byte) 14, 1735328473U);
      GG(ref a2, a3, a4, a1, target[offset + 12], (byte) 20, 2368359562U);
      HH(ref a1, a2, a3, a4, target[offset + 5], (byte) 4, 4294588738U);
      HH(ref a4, a1, a2, a3, target[offset + 8], (byte) 11, 2272392833U);
      HH(ref a3, a4, a1, a2, target[offset + 11], (byte) 16, 1839030562U);
      HH(ref a2, a3, a4, a1, target[offset + 14], (byte) 23, 4259657740U);
      HH(ref a1, a2, a3, a4, target[offset + 1], (byte) 4, 2763975236U);
      HH(ref a4, a1, a2, a3, target[offset + 4], (byte) 11, 1272893353U);
      HH(ref a3, a4, a1, a2, target[offset + 7], (byte) 16, 4139469664U);
      HH(ref a2, a3, a4, a1, target[offset + 10], (byte) 23, 3200236656U);
      HH(ref a1, a2, a3, a4, target[offset + 13], (byte) 4, 681279174U);
      HH(ref a4, a1, a2, a3, target[offset], (byte) 11, 3936430074U);
      HH(ref a3, a4, a1, a2, target[offset + 3], (byte) 16, 3572445317U);
      HH(ref a2, a3, a4, a1, target[offset + 6], (byte) 23, 76029189U);
      HH(ref a1, a2, a3, a4, target[offset + 9], (byte) 4, 3654602809U);
      HH(ref a4, a1, a2, a3, target[offset + 12], (byte) 11, 3873151461U);
      HH(ref a3, a4, a1, a2, target[offset + 15], (byte) 16, 530742520U);
      HH(ref a2, a3, a4, a1, target[offset + 2], (byte) 23, 3299628645U);
      II(ref a1, a2, a3, a4, target[offset], (byte) 6, 4096336452U);
      II(ref a4, a1, a2, a3, target[offset + 7], (byte) 10, 1126891415U);
      II(ref a3, a4, a1, a2, target[offset + 14], (byte) 15, 2878612391U);
      II(ref a2, a3, a4, a1, target[offset + 5], (byte) 21, 4237533241U);
      II(ref a1, a2, a3, a4, target[offset + 12], (byte) 6, 1700485571U);
      II(ref a4, a1, a2, a3, target[offset + 3], (byte) 10, 2399980690U);
      II(ref a3, a4, a1, a2, target[offset + 10], (byte) 15, 4293915773U);
      II(ref a2, a3, a4, a1, target[offset + 1], (byte) 21, 2240044497U);
      II(ref a1, a2, a3, a4, target[offset + 8], (byte) 6, 1873313359U);
      II(ref a4, a1, a2, a3, target[offset + 15], (byte) 10, 4264355552U);
      II(ref a3, a4, a1, a2, target[offset + 6], (byte) 15, 2734768916U);
      II(ref a2, a3, a4, a1, target[offset + 13], (byte) 21, 1309151649U);
      II(ref a1, a2, a3, a4, target[offset + 4], (byte) 6, 4149444226U);
      II(ref a4, a1, a2, a3, target[offset + 11], (byte) 10, 3174756917U);
      II(ref a3, a4, a1, a2, target[offset + 2], (byte) 15, 718787259U);
      II(ref a2, a3, a4, a1, target[offset + 9], (byte) 21, 3951481745U);
      state[0] += a1;
      state[1] += a2;
      state[2] += a3;
      state[3] += a4;
    }

    private void Init(MD5Context context)
    {
      context.r_State[0] = 1732584193U;
      context.r_State[1] = 4023233417U;
      context.r_State[2] = 2562383102U;
      context.r_State[3] = 271733878U;
      context.r_Count[0] = 0U;
      context.r_Count[1] = 0U;
      int index = 0;
      for (int length = context.r_Buffer.Length; index < length; ++index)
        context.r_Buffer[index] = (byte) 0;
    }

    private void Update(MD5Context context, byte[] inputBuffer, int len)
    {
      int destinationIndex = (int) (context.r_Count[0] >> 3) & 63;
      context.r_Count[0] += (uint) (len << 3);
      if ((long) context.r_Count[0] < (long) (len << 3))
        ++context.r_Count[1];
      context.r_Count[1] += (uint) (len >> 29);
      int length = 64 - destinationIndex;
      int num;
      if (len >= length)
      {
        Array.Copy((Array) inputBuffer, 0, (Array) context.r_Buffer, destinationIndex, length);
        Transform(context.r_Buffer, 0, context.r_State);
        num = length;
        while (num + 63 < len)
        {
          Transform(inputBuffer, num, context.r_State);
          num += 64;
        }
        destinationIndex = 0;
      }
      else
        num = 0;
      Array.Copy((Array) inputBuffer, num, (Array) context.r_Buffer, destinationIndex, len - num);
    }

    private void Final(MD5Context context, byte[] digest)
    {
      byte[] numArray = new byte[8];
      Decode(context.r_Count, 0, numArray, 0, 2);
      int num = (int) (context.r_Count[0] >> 3) & 63;
      int len = num < 56 ? 56 - num : 120 - num;
      Update(context, paddingBuffer, len);
      Update(context, numArray, 8);
      Decode(context.r_State, 0, digest, 0, 4);
      int index1 = 0;
      for (int length = context.r_Buffer.Length; index1 < length; ++index1)
        context.r_Buffer[index1] = (byte) 0;
      int index2 = 0;
      for (int length = context.r_Count.Length; index2 < length; ++index2)
        context.r_Count[index2] = 0U;
      int index3 = 0;
      for (int length = context.r_State.Length; index3 < length; ++index3)
        context.r_State[index3] = 0U;
    }

    private uint F(uint x, uint y, uint z)
    {
      return (uint) ((int) x & (int) y | ~(int) x & (int) z);
    }

    private uint G(uint x, uint y, uint z)
    {
      return (uint) ((int) x & (int) z | (int) y & ~(int) z);
    }

    private uint H(uint x, uint y, uint z)
    {
      return x ^ y ^ z;
    }

    private uint I(uint x, uint y, uint z)
    {
      return y ^ (x | ~z);
    }

    private void rot(ref uint x, byte n)
    {
      x = x << (int) n | x >> 32 - (int) n;
    }

    private void FF(ref uint a, uint b, uint c, uint d, uint x, byte s, uint ac)
    {
      a += F(b, c, d) + x + ac;
      rot(ref a, s);
      a += b;
    }

    private void GG(ref uint a, uint b, uint c, uint d, uint x, byte s, uint ac)
    {
      a += G(b, c, d) + x + ac;
      rot(ref a, s);
      a += b;
    }

    private void HH(ref uint a, uint b, uint c, uint d, uint x, byte s, uint ac)
    {
      a += H(b, c, d) + x + ac;
      rot(ref a, s);
      a += b;
    }

    private void II(ref uint a, uint b, uint c, uint d, uint x, byte s, uint ac)
    {
      a += I(b, c, d) + x + ac;
      rot(ref a, s);
      a += b;
    }

        private string Print(byte[] dig)
    {
      char[] chArray = new char[16]{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
      string str = string.Empty;
      for (int index = 0; index < 16; ++index)
        str = str + (object) chArray[(int) dig[index] >> 4 & 15] + (object) chArray[(int) dig[index] & 15];
      return str;
    }

    internal byte[] DigString(string message)
    {
      byte[] digest = new byte[16];
            MD5Context context = new MD5Context();
      Init(context);
      byte[] bytes = Encoding.GetEncoding(1252).GetBytes(message);
      Update(context, bytes, bytes.Length);
      Final(context, digest);
      return digest;
    }

    internal Signature DigByteArray(byte[] array)
    {
            Signature signature = new Signature(0L, 0L);
            MD5Context context = new MD5Context();
      Init(context);
      Update(context, array, array.Length);
      Final(context, signature.sign);
      return signature;
    }

    internal static bool Match(byte[] Dig1, byte[] Dig2)
    {
      for (int index = 0; index < 16; ++index)
      {
        if ((int) Dig1[index] != (int) Dig2[index])
          return false;
      }
      return true;
    }

    internal static bool IsEmpty(byte[] digest)
    {
      return Match(new byte[16], digest);
    }

    internal void Reset(byte[] digest)
    {
      for (int index = 0; index < 16; ++index)
        digest[index] = (byte) 0;
    }

    private class MD5Context
    {
      internal uint[] r_State = new uint[4];
      internal uint[] r_Count = new uint[2];
      internal byte[] r_Buffer = new byte[64];
    }

    internal struct Signature
    {
      internal static Signature EmptySignature = new Signature(0L, 0L);
      internal byte[] sign;

      internal Signature(long low, long high)
      {
        sign = new byte[16];
        Array.Copy((Array) BitConverter.GetBytes(low), 0, (Array) sign, 0, 8);
        Array.Copy((Array) BitConverter.GetBytes(high), 0, (Array) sign, 8, 8);
      }

      internal long Low
      {
        get
        {
          return BitConverter.ToInt64(sign, 0);
        }
      }

      internal long High
      {
        get
        {
          return BitConverter.ToInt64(sign, 8);
        }
      }

      internal bool Empty
      {
        get
        {
          return IsEmpty(sign);
        }
      }

      internal bool Equals(Signature sign)
      {
        return Match(this.sign, sign.sign);
      }
    }
  }
}
