using System.Runtime.InteropServices;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class Lzh
  {
    private static object synchRoot = new object();
    private byte[] textBuffer = new byte[Constants.N + Constants.F - 1];
    private short[] lson = new short[Constants.N + 1];
    private short[] dad = new short[Constants.N + 1];
    private short[] rson = new short[Constants.N + 1 + 256];
    private ushort[] freq = new ushort[Constants.T + 1];
    private short[] prnt = new short[Constants.T + Constants.N_CHAR];
    private short[] son = new short[Constants.T];
    private ushort getbuf;
    private ushort putbuf;
    private byte getlen;
    private byte putlen;
    private int codeSize;
    private int matchPosition;
    private int matchLength;
    private LzhStreaming funcReadBlock;
    private LzhStreaming funcWriteBlock;
    private byte[] readBuffer;
    private byte[] writeBuffer;
    private int readOffset;
    private int writeOffset;

    internal Lzh(LzhStreaming funcReadBlock, LzhStreaming funcWriteBlock)
    {
      this.funcReadBlock = funcReadBlock;
      this.funcWriteBlock = funcWriteBlock;
    }

    private void InitTree()
    {
      for (int index = Constants.N + 1; index <= Constants.N + 256; ++index)
        rson[index] = Constants.NUL;
      for (int index = 0; index <= Constants.N; ++index)
        dad[index] = Constants.NUL;
    }

    private void InsertNode(short r)
    {
      short num1 = (short) (Constants.N + 1 + textBuffer[r]);
      int num2 = 1;
      int num3 = Constants.F - 1;
      rson[r] = Constants.NUL;
      lson[r] = Constants.NUL;
      matchLength = 0;
      while (matchLength < num3)
      {
        if (num2 >= 0)
        {
          if (rson[num1] != Constants.NUL)
          {
            num1 = rson[num1];
          }
          else
          {
            rson[num1] = r;
            dad[r] = num1;
            return;
          }
        }
        else if (lson[num1] != Constants.NUL)
        {
          num1 = lson[num1];
        }
        else
        {
          lson[num1] = r;
          dad[r] = num1;
          return;
        }
        int num4 = 0;
        for (num2 = 0; num4 < num3 && num2 == 0; num2 = textBuffer[r + num4] - textBuffer[num1 + num4])
          ++num4;
        if (num4 > Constants.THRESHOLD)
        {
          int num5 = (short)(r - num1 & Constants.N - 1) - 1;
          if (num4 > matchLength)
          {
            matchPosition = num5;
            matchLength = num4;
          }
          if (matchLength < num3 && num4 == matchLength && num5 < matchPosition)
            matchPosition = num5;
        }
      }
      dad[r] = dad[num1];
      lson[r] = lson[num1];
      rson[r] = rson[num1];
      dad[lson[num1]] = r;
      dad[rson[num1]] = r;
      if (rson[dad[num1]] == num1)
        rson[dad[num1]] = r;
      else
        lson[dad[num1]] = r;
      dad[num1] = Constants.NUL;
    }

    private void DeleteNode(short p)
    {
      if (dad[p] == Constants.NUL)
        return;
      short num;
      if (rson[p] == Constants.NUL)
        num = lson[p];
      else if (lson[p] == Constants.NUL)
      {
        num = rson[p];
      }
      else
      {
        num = lson[p];
        if (rson[num] != Constants.NUL)
        {
          do
          {
            num = rson[num];
          }
          while (rson[num] != Constants.NUL);
          rson[dad[num]] = lson[num];
          dad[lson[num]] = dad[num];
          lson[num] = lson[p];
          dad[lson[p]] = num;
        }
        rson[num] = rson[p];
        dad[rson[p]] = num;
      }
      dad[num] = dad[p];
      if (rson[dad[p]] == p)
        rson[dad[p]] = num;
      else
        lson[dad[p]] = num;
      dad[p] = Constants.NUL;
    }

    private ushort GetBit()
    {
      byte input = 0;
      while (getlen <= 8)
      {
        this.getbuf |= (ushort) ((funcReadBlock(ref input, readBuffer, ref readOffset) == 1 ? input : 0U) << 8 - getlen);
        getlen += 8;
      }
      ushort getbuf = this.getbuf;
      this.getbuf <<= 1;
      --getlen;
      return (short) getbuf >= 0 ? (ushort) 0 : (ushort) 1;
    }

    private int GetByte()
    {
      byte input = 0;
      while (getlen <= 8)
      {
        this.getbuf |= (ushort) ((funcReadBlock(ref input, readBuffer, ref readOffset) == 1 ? input : 0U) << 8 - getlen);
        getlen += 8;
      }
      ushort getbuf = this.getbuf;
      this.getbuf <<= 8;
      getlen -= 8;
      return getbuf >> 8;
    }

    private bool Putcode(ushort l, ushort c)
    {
      putbuf |= (ushort) ((uint) c >> putlen);
      putlen += (byte) l;
      if (putlen >= 8)
      {
        byte input = (byte) ((uint) putbuf >> 8);
        if (funcWriteBlock(ref input, writeBuffer, ref writeOffset) == 0)
          return false;
        putlen -= 8;
        if (putlen >= 8)
        {
          byte putbuf = (byte) this.putbuf;
          if (funcWriteBlock(ref putbuf, writeBuffer, ref writeOffset) == 0)
            return false;
          putlen -= 8;
          this.putbuf = (ushort) ((uint) c << l - putlen);
          codeSize += 2;
        }
        else
        {
          putbuf <<= 8;
          ++codeSize;
        }
      }
      return true;
    }

    private void StartHuff()
    {
      for (short index = 0; index < Constants.N_CHAR; ++index)
      {
        freq[index] = 1;
        son[index] = (short) (index + Constants.T);
        prnt[index + Constants.T] = index;
      }
      short num = 0;
      for (short nChar = Constants.N_CHAR; nChar <= Constants.R; ++nChar)
      {
        freq[nChar] = (ushort) (freq[num] + (uint) freq[num + 1]);
        son[nChar] = num;
        prnt[num] = nChar;
        prnt[num + 1] = nChar;
        num += 2;
      }
      freq[Constants.T] = ushort.MaxValue;
      prnt[Constants.R] = 0;
    }

    private void Reconstruct()
    {
      int index1 = 0;
      for (short index2 = 0; index2 < Constants.T; ++index2)
      {
        if (son[index2] >= Constants.T)
        {
          freq[index1] = (ushort) ((freq[index2] + 1) / 2);
          son[index1] = son[index2];
          ++index1;
        }
      }
      short num1 = 0;
      int nChar = Constants.N_CHAR;
      for (; nChar < Constants.T; ++nChar)
      {
        int index2 = num1 + 1;
        ushort num2 = (ushort) (freq[num1] + (uint) freq[index2]);
        freq[nChar] = num2;
        int index3 = nChar - 1;
        while (num2 < freq[index3])
          --index3;
        int index4 = index3 + 1;
        ushort num3 = (ushort) (nChar - index4 << 1);
        int num4 = index4 + 1;
        for (int index5 = num3 - 1; index5 >= 0; --index5)
          freq[num4 + index5] = freq[index4 + index5];
        freq[index4] = num2;
        for (int index5 = num3 - 1; index5 >= 0; --index5)
          son[num4 + index5] = son[index4 + index5];
        son[index4] = num1;
        num1 += 2;
      }
      for (short index2 = 0; index2 < Constants.T; ++index2)
      {
        int index3 = son[index2];
        prnt[index3] = index2;
        if (index3 < Constants.T)
          prnt[index3 + 1] = index2;
      }
    }

    private void Update(short c)
    {
      if (freq[Constants.R] == Constants.MAX_FREQ)
        Reconstruct();
      c = prnt[c + Constants.T];
      do
      {
        ++freq[c];
        ushort num1 = freq[c];
        short num2 = (short) (c + 1);
        if (num1 > freq[num2])
        {
          while (num1 > freq[num2])
            ++num2;
          short num3 = (short) (num2 - 1);
          freq[c] = freq[num3];
          freq[num3] = num1;
          short num4 = son[c];
          prnt[num4] = num3;
          if (num4 < Constants.T)
            prnt[num4 + 1] = num3;
          short num5 = son[num3];
          son[num3] = num4;
          prnt[num5] = c;
          if (num5 < Constants.T)
            prnt[num5 + 1] = c;
          son[c] = num5;
          c = num3;
        }
        c = prnt[c];
      }
      while (c != 0);
    }

    private bool EncodeChar(ushort c)
    {
      ushort c1 = 0;
      ushort l = 0;
      int index = prnt[c + Constants.T];
      do
      {
        c1 >>= 1;
        if ((index & 1) != 0)
          c1 += 32768;
        ++l;
        index = prnt[index];
      }
      while (index != Constants.R);
      if (!Putcode(l, c1))
        return false;
      Update((short) c);
      return true;
    }

    private bool EncodePosition(ushort c)
    {
      ushort num1 = (ushort) ((uint) c >> 6);
      ushort num2 = Constants.const_p_code[num1];
      if (Putcode(Constants.const_p_len[num1], (ushort) ((uint) num2 << 8)))
        return Putcode(6, (ushort) ((c & 63) << 10));
      return false;
    }

    private bool EncodeEnd()
    {
      if (putlen != 0)
      {
        byte input = (byte) ((uint) putbuf >> 8);
        if (funcWriteBlock(ref input, writeBuffer, ref writeOffset) == 0)
          return false;
        ++codeSize;
      }
      return true;
    }

    private short DecodeChar()
    {
      short num = son[Constants.R];
      while (num < Constants.T)
        num = son[num + GetBit()];
      short c = (short) (num - Constants.T);
      Update(c);
      return c;
    }

    private ushort DecodePosition()
    {
      int index1 = GetByte();
      ushort num = (ushort) ((uint)Constants.const_d_code[index1] << 6);
      for (int index2 = Constants.const_d_len[index1] - 2; index2 != 0; --index2)
        index1 = (index1 << 1) + GetBit();
      return (ushort) (num | (uint) (index1 & 63));
    }

    private void InitPackLZH()
    {
      putlen = 0;
      putbuf = 0;
      matchPosition = 0;
      matchLength = 0;
      codeSize = 0;
      int index1 = 0;
      for (int length = lson.Length; index1 < length; ++index1)
        lson[index1] = 0;
      int index2 = 0;
      for (int length = dad.Length; index2 < length; ++index2)
        dad[index2] = 0;
      int index3 = 0;
      for (int length = rson.Length; index3 < length; ++index3)
        rson[index3] = 0;
      int index4 = 0;
      for (int length = textBuffer.Length; index4 < length; ++index4)
        textBuffer[index4] = 0;
      int index5 = 0;
      for (int length = freq.Length; index5 < length; ++index5)
        freq[index5] = 0;
      int index6 = 0;
      for (int length = prnt.Length; index6 < length; ++index6)
        prnt[index6] = 0;
      int index7 = 0;
      for (int length = son.Length; index7 < length; ++index7)
        son[index7] = 0;
    }

    private void InitUnpackLZH()
    {
      getbuf = 0;
      getlen = 0;
      int index1 = 0;
      for (int length = freq.Length; index1 < length; ++index1)
        freq[index1] = 0;
      int index2 = 0;
      for (int length = prnt.Length; index2 < length; ++index2)
        prnt[index2] = 0;
      int index3 = 0;
      for (int length = son.Length; index3 < length; ++index3)
        son[index3] = 0;
    }

    internal int LZHPack(ref int writtenBytes, byte[] readBuffer, int readOffset, byte[] writeBuffer, int writeOffset)
    {
      lock (synchRoot)
      {
        int num1 = 0;
        writtenBytes = 0;
        InitPackLZH();
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.readOffset = readOffset;
        this.writeOffset = writeOffset;
        try
        {
          StartHuff();
          InitTree();
          short p = 0;
          short r = (short) (Constants.N - Constants.F);
          for (int index = 0; index < r; ++index)
            textBuffer[index] = 32;
          int num2 = 0;
          byte input;
          for (input = 0; num2 < Constants.F && funcReadBlock(ref input, this.readBuffer, ref this.readOffset) != 0; ++num2)
            textBuffer[r + num2] = input;
          num1 = num2;
          for (short index = 1; index <= Constants.F; ++index)
            InsertNode((short) (r - index));
          InsertNode(r);
          do
          {
            if (this.matchLength > num2)
              this.matchLength = num2;
            if (this.matchLength <= Constants.THRESHOLD)
            {
              this.matchLength = 1;
              if (!EncodeChar(textBuffer[r]))
                return codeSize;
            }
            else if (!EncodeChar((ushort) (byte.MaxValue - Constants.THRESHOLD + this.matchLength)) || !EncodePosition((ushort) matchPosition))
              return codeSize;
            int matchLength = (short)this.matchLength;
            short num3;
            for (num3 = 0; num3 < matchLength && funcReadBlock(ref input, this.readBuffer, ref this.readOffset) != 0; ++num3)
            {
              DeleteNode(p);
              textBuffer[p] = input;
              if (p < (short)(Constants.F - 1))
                textBuffer[p + Constants.N] = input;
              p = (short) (p + 1 & Constants.N - 1);
              r = (short) (r + 1 & Constants.N - 1);
              InsertNode(r);
            }
            num1 += num3;
            while (num3 < matchLength)
            {
              ++num3;
              DeleteNode(p);
              p = (short) (p + 1 & Constants.N - 1);
              r = (short) (r + 1 & Constants.N - 1);
              --num2;
              if (num2 != 0)
                InsertNode(r);
            }
          }
          while (num2 > 0);
          if (!EncodeEnd())
            return codeSize;
        }
        finally
        {
          this.readBuffer = null;
          this.writeBuffer = null;
        }
        writtenBytes = num1;
        return codeSize;
      }
    }

    internal bool LZHUnpack(int originTextSize, byte[] readBuffer, int readOffset, byte[] writeBuffer, int writeOffset)
    {
      lock (synchRoot)
      {
        InitUnpackLZH();
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.readOffset = readOffset;
        this.writeOffset = writeOffset;
        try
        {
          StartHuff();
          int index1 = Constants.N - Constants.F;
          for (int index2 = 0; index2 < index1; ++index2)
            textBuffer[index2] = 32;
          int num1 = 0;
          while (num1 < originTextSize)
          {
            short num2 = DecodeChar();
            byte input = 0;
            if (num2 < 256)
            {
              input = (byte) num2;
              if (funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
                return false;
              textBuffer[index1] = (byte) num2;
              index1 = index1 + 1 & Constants.N - 1;
              ++num1;
            }
            else
            {
              int num3 = index1 - (DecodePosition() + 1) & Constants.N - 1;
              int num4 = num2 - byte.MaxValue + Constants.THRESHOLD;
              for (int index2 = 0; index2 < num4; ++index2)
              {
                short num5 = textBuffer[(short)(num3 + index2 & Constants.N - 1)];
                input = (byte) num5;
                if (funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
                  return false;
                textBuffer[index1] = (byte) num5;
                index1 = index1 + 1 & Constants.N - 1;
                ++num1;
              }
            }
          }
        }
        finally
        {
          this.readBuffer = null;
          this.writeBuffer = null;
        }
        return true;
      }
    }

    internal delegate int LzhStreaming(ref byte input, byte[] buffer, ref int offset);

    [StructLayout(LayoutKind.Sequential, Size = 1)]
    private struct Constants
    {
      internal static short N = 4096;
      internal static short F = 60;
      internal static short THRESHOLD = 2;
      internal static short NUL = N;
      internal static short N_CHAR = (short) (256 - THRESHOLD + F);
      internal static short T = (short) (N_CHAR * 2 - 1);
      internal static short R = (short) (T - 1);
      internal static ushort MAX_FREQ = 32768;
      internal static byte[] const_p_len = new byte[64]{ 3, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 };
      internal static byte[] const_p_code = new byte[64]{ 0, 32, 48, 64, 80, 88, 96, 104, 112, 120, 128, 136, 144, 148, 152, 156, 160, 164, 168, 172, 176, 180, 184, 188, 192, 194, 196, 198, 200, 202, 204, 206, 208, 210, 212, 214, 216, 218, 220, 222, 224, 226, 228, 230, 232, 234, 236, 238, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, byte.MaxValue };
      internal static byte[] const_d_code = new byte[256]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19, 19, 20, 20, 20, 20, 21, 21, 21, 21, 22, 22, 22, 22, 23, 23, 23, 23, 24, 24, 25, 25, 26, 26, 27, 27, 28, 28, 29, 29, 30, 30, 31, 31, 32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 37, 37, 38, 38, 39, 39, 40, 40, 41, 41, 42, 42, 43, 43, 44, 44, 45, 45, 46, 46, 47, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63 };
      internal static byte[] const_d_len = new byte[256]{ 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 };
    }
  }
}
