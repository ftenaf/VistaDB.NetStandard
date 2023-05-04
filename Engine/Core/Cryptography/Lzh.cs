using System.Runtime.InteropServices;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class Lzh
  {
    private static object synchRoot = new object();
    private byte[] textBuffer = new byte[(int)Constants.N + (int)Constants.F - 1];
    private short[] lson = new short[(int)Constants.N + 1];
    private short[] dad = new short[(int)Constants.N + 1];
    private short[] rson = new short[(int)Constants.N + 1 + 256];
    private ushort[] freq = new ushort[(int)Constants.T + 1];
    private short[] prnt = new short[(int)Constants.T + (int)Constants.N_CHAR];
    private short[] son = new short[(int)Constants.T];
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
      for (int index = (int)Constants.N + 1; index <= (int)Constants.N + 256; ++index)
        rson[index] = Constants.NUL;
      for (int index = 0; index <= (int)Constants.N; ++index)
        dad[index] = Constants.NUL;
    }

    private void InsertNode(short r)
    {
      short num1 = (short) ((int)Constants.N + 1 + (int) textBuffer[(int) r]);
      int num2 = 1;
      int num3 = (int)Constants.F - 1;
      rson[(int) r] = Constants.NUL;
      lson[(int) r] = Constants.NUL;
      matchLength = 0;
      while (matchLength < num3)
      {
        if (num2 >= 0)
        {
          if ((int) rson[(int) num1] != (int)Constants.NUL)
          {
            num1 = rson[(int) num1];
          }
          else
          {
            rson[(int) num1] = r;
            dad[(int) r] = num1;
            return;
          }
        }
        else if ((int) lson[(int) num1] != (int)Constants.NUL)
        {
          num1 = lson[(int) num1];
        }
        else
        {
          lson[(int) num1] = r;
          dad[(int) r] = num1;
          return;
        }
        int num4 = 0;
        for (num2 = 0; num4 < num3 && num2 == 0; num2 = (int) textBuffer[(int) r + num4] - (int) textBuffer[(int) num1 + num4])
          ++num4;
        if (num4 > (int)Constants.THRESHOLD)
        {
          int num5 = (int) (short) ((int) r - (int) num1 & (int)Constants.N - 1) - 1;
          if (num4 > matchLength)
          {
            matchPosition = num5;
            matchLength = num4;
          }
          if (matchLength < num3 && num4 == matchLength && num5 < matchPosition)
            matchPosition = num5;
        }
      }
      dad[(int) r] = dad[(int) num1];
      lson[(int) r] = lson[(int) num1];
      rson[(int) r] = rson[(int) num1];
      dad[(int) lson[(int) num1]] = r;
      dad[(int) rson[(int) num1]] = r;
      if ((int) rson[(int) dad[(int) num1]] == (int) num1)
        rson[(int) dad[(int) num1]] = r;
      else
        lson[(int) dad[(int) num1]] = r;
      dad[(int) num1] = Constants.NUL;
    }

    private void DeleteNode(short p)
    {
      if ((int) dad[(int) p] == (int)Constants.NUL)
        return;
      short num;
      if ((int) rson[(int) p] == (int)Constants.NUL)
        num = lson[(int) p];
      else if ((int) lson[(int) p] == (int)Constants.NUL)
      {
        num = rson[(int) p];
      }
      else
      {
        num = lson[(int) p];
        if ((int) rson[(int) num] != (int)Constants.NUL)
        {
          do
          {
            num = rson[(int) num];
          }
          while ((int) rson[(int) num] != (int)Constants.NUL);
          rson[(int) dad[(int) num]] = lson[(int) num];
          dad[(int) lson[(int) num]] = dad[(int) num];
          lson[(int) num] = lson[(int) p];
          dad[(int) lson[(int) p]] = num;
        }
        rson[(int) num] = rson[(int) p];
        dad[(int) rson[(int) p]] = num;
      }
      dad[(int) num] = dad[(int) p];
      if ((int) rson[(int) dad[(int) p]] == (int) p)
        rson[(int) dad[(int) p]] = num;
      else
        lson[(int) dad[(int) p]] = num;
      dad[(int) p] = Constants.NUL;
    }

    private ushort GetBit()
    {
      byte input = 0;
      while (getlen <= (byte) 8)
      {
        this.getbuf |= (ushort) ((funcReadBlock(ref input, readBuffer, ref readOffset) == 1 ? (uint) (ushort) input : 0U) << 8 - (int) getlen);
        getlen += (byte) 8;
      }
      ushort getbuf = this.getbuf;
      this.getbuf <<= 1;
      --getlen;
      return (short) getbuf >= (short) 0 ? (ushort) 0 : (ushort) 1;
    }

    private int GetByte()
    {
      byte input = 0;
      while (getlen <= (byte) 8)
      {
        this.getbuf |= (ushort) ((funcReadBlock(ref input, readBuffer, ref readOffset) == 1 ? (uint) (ushort) input : 0U) << 8 - (int) getlen);
        getlen += (byte) 8;
      }
      ushort getbuf = this.getbuf;
      this.getbuf <<= 8;
      getlen -= (byte) 8;
      return (int) getbuf >> 8;
    }

    private bool Putcode(ushort l, ushort c)
    {
      putbuf |= (ushort) ((uint) c >> (int) putlen);
      putlen += (byte) l;
      if (putlen >= (byte) 8)
      {
        byte input = (byte) ((uint) putbuf >> 8);
        if (funcWriteBlock(ref input, writeBuffer, ref writeOffset) == 0)
          return false;
        putlen -= (byte) 8;
        if (putlen >= (byte) 8)
        {
          byte putbuf = (byte) this.putbuf;
          if (funcWriteBlock(ref putbuf, writeBuffer, ref writeOffset) == 0)
            return false;
          putlen -= (byte) 8;
          this.putbuf = (ushort) ((uint) c << (int) l - (int) putlen);
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
      for (short index = 0; (int) index < (int)Constants.N_CHAR; ++index)
      {
        freq[(int) index] = (ushort) 1;
        son[(int) index] = (short) ((int) index + (int)Constants.T);
        prnt[(int) index + (int)Constants.T] = index;
      }
      short num = 0;
      for (short nChar = Constants.N_CHAR; (int) nChar <= (int)Constants.R; ++nChar)
      {
        freq[(int) nChar] = (ushort) ((uint) freq[(int) num] + (uint) freq[(int) num + 1]);
        son[(int) nChar] = num;
        prnt[(int) num] = nChar;
        prnt[(int) num + 1] = nChar;
        num += (short) 2;
      }
      freq[(int)Constants.T] = ushort.MaxValue;
      prnt[(int)Constants.R] = (short) 0;
    }

    private void Reconstruct()
    {
      int index1 = 0;
      for (short index2 = 0; (int) index2 < (int)Constants.T; ++index2)
      {
        if ((int) son[(int) index2] >= (int)Constants.T)
        {
          freq[index1] = (ushort) (((int) freq[(int) index2] + 1) / 2);
          son[index1] = son[(int) index2];
          ++index1;
        }
      }
      short num1 = 0;
      int nChar = (int)Constants.N_CHAR;
      for (; nChar < (int)Constants.T; ++nChar)
      {
        int index2 = (int) num1 + 1;
        ushort num2 = (ushort) ((uint) freq[(int) num1] + (uint) freq[index2]);
        freq[nChar] = num2;
        int index3 = nChar - 1;
        while ((int) num2 < (int) freq[index3])
          --index3;
        int index4 = index3 + 1;
        ushort num3 = (ushort) (nChar - index4 << 1);
        int num4 = index4 + 1;
        for (int index5 = (int) num3 - 1; index5 >= 0; --index5)
          freq[num4 + index5] = freq[index4 + index5];
        freq[index4] = num2;
        for (int index5 = (int) num3 - 1; index5 >= 0; --index5)
          son[num4 + index5] = son[index4 + index5];
        son[index4] = num1;
        num1 += (short) 2;
      }
      for (short index2 = 0; (int) index2 < (int)Constants.T; ++index2)
      {
        int index3 = (int) son[(int) index2];
        prnt[index3] = index2;
        if (index3 < (int)Constants.T)
          prnt[index3 + 1] = index2;
      }
    }

    private void Update(short c)
    {
      if ((int) freq[(int)Constants.R] == (int)Constants.MAX_FREQ)
        Reconstruct();
      c = prnt[(int) c + (int)Constants.T];
      do
      {
        ++freq[(int) c];
        ushort num1 = freq[(int) c];
        short num2 = (short) ((int) c + 1);
        if ((int) num1 > (int) freq[(int) num2])
        {
          while ((int) num1 > (int) freq[(int) num2])
            ++num2;
          short num3 = (short) ((int) num2 - 1);
          freq[(int) c] = freq[(int) num3];
          freq[(int) num3] = num1;
          short num4 = son[(int) c];
          prnt[(int) num4] = num3;
          if ((int) num4 < (int)Constants.T)
            prnt[(int) num4 + 1] = num3;
          short num5 = son[(int) num3];
          son[(int) num3] = num4;
          prnt[(int) num5] = c;
          if ((int) num5 < (int)Constants.T)
            prnt[(int) num5 + 1] = c;
          son[(int) c] = num5;
          c = num3;
        }
        c = prnt[(int) c];
      }
      while (c != (short) 0);
    }

    private bool EncodeChar(ushort c)
    {
      ushort c1 = 0;
      ushort l = 0;
      int index = (int) prnt[(int) c + (int)Constants.T];
      do
      {
        c1 >>= 1;
        if ((index & 1) != 0)
          c1 += (ushort) 32768;
        ++l;
        index = (int) prnt[index];
      }
      while (index != (int)Constants.R);
      if (!Putcode(l, c1))
        return false;
      Update((short) c);
      return true;
    }

    private bool EncodePosition(ushort c)
    {
      ushort num1 = (ushort) ((uint) c >> 6);
      ushort num2 = (ushort)Constants.const_p_code[(int) num1];
      if (Putcode((ushort)Constants.const_p_len[(int) num1], (ushort) ((uint) num2 << 8)))
        return Putcode((ushort) 6, (ushort) (((int) c & 63) << 10));
      return false;
    }

    private bool EncodeEnd()
    {
      if (putlen != (byte) 0)
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
      short num = son[(int)Constants.R];
      while ((int) num < (int)Constants.T)
        num = son[(int) num + (int) GetBit()];
      short c = (short) ((int) num - (int)Constants.T);
      Update(c);
      return c;
    }

    private ushort DecodePosition()
    {
      int index1 = GetByte();
      ushort num = (ushort) ((uint)Constants.const_d_code[index1] << 6);
      for (int index2 = (int)Constants.const_d_len[index1] - 2; index2 != 0; --index2)
        index1 = (index1 << 1) + (int) GetBit();
      return (ushort) ((uint) num | (uint) (index1 & 63));
    }

    private void InitPackLZH()
    {
      putlen = (byte) 0;
      putbuf = (ushort) 0;
      matchPosition = 0;
      matchLength = 0;
      codeSize = 0;
      int index1 = 0;
      for (int length = lson.Length; index1 < length; ++index1)
        lson[index1] = (short) 0;
      int index2 = 0;
      for (int length = dad.Length; index2 < length; ++index2)
        dad[index2] = (short) 0;
      int index3 = 0;
      for (int length = rson.Length; index3 < length; ++index3)
        rson[index3] = (short) 0;
      int index4 = 0;
      for (int length = textBuffer.Length; index4 < length; ++index4)
        textBuffer[index4] = (byte) 0;
      int index5 = 0;
      for (int length = freq.Length; index5 < length; ++index5)
        freq[index5] = (ushort) 0;
      int index6 = 0;
      for (int length = prnt.Length; index6 < length; ++index6)
        prnt[index6] = (short) 0;
      int index7 = 0;
      for (int length = son.Length; index7 < length; ++index7)
        son[index7] = (short) 0;
    }

    private void InitUnpackLZH()
    {
      getbuf = (ushort) 0;
      getlen = (byte) 0;
      int index1 = 0;
      for (int length = freq.Length; index1 < length; ++index1)
        freq[index1] = (ushort) 0;
      int index2 = 0;
      for (int length = prnt.Length; index2 < length; ++index2)
        prnt[index2] = (short) 0;
      int index3 = 0;
      for (int length = son.Length; index3 < length; ++index3)
        son[index3] = (short) 0;
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
          short r = (short) ((int)Constants.N - (int)Constants.F);
          for (int index = 0; index < (int) r; ++index)
            textBuffer[index] = (byte) 32;
          int num2 = 0;
          byte input;
          for (input = (byte) 0; num2 < (int)Constants.F && funcReadBlock(ref input, this.readBuffer, ref this.readOffset) != 0; ++num2)
            textBuffer[(int) r + num2] = input;
          num1 = num2;
          for (short index = 1; (int) index <= (int)Constants.F; ++index)
            InsertNode((short) ((int) r - (int) index));
          InsertNode(r);
          do
          {
            if (this.matchLength > num2)
              this.matchLength = num2;
            if (this.matchLength <= (int)Constants.THRESHOLD)
            {
              this.matchLength = 1;
              if (!EncodeChar((ushort) textBuffer[(int) r]))
                return codeSize;
            }
            else if (!EncodeChar((ushort) ((int) byte.MaxValue - (int)Constants.THRESHOLD + this.matchLength)) || !EncodePosition((ushort) matchPosition))
              return codeSize;
            int matchLength = (int) (short) this.matchLength;
            short num3;
            for (num3 = (short) 0; (int) num3 < matchLength && funcReadBlock(ref input, this.readBuffer, ref this.readOffset) != 0; ++num3)
            {
              DeleteNode(p);
              textBuffer[(int) p] = input;
              if ((int) p < (int) (short) ((int)Constants.F - 1))
                textBuffer[(int) p + (int)Constants.N] = input;
              p = (short) ((int) p + 1 & (int)Constants.N - 1);
              r = (short) ((int) r + 1 & (int)Constants.N - 1);
              InsertNode(r);
            }
            num1 += (int) num3;
            while ((int) num3 < matchLength)
            {
              ++num3;
              DeleteNode(p);
              p = (short) ((int) p + 1 & (int)Constants.N - 1);
              r = (short) ((int) r + 1 & (int)Constants.N - 1);
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
          this.readBuffer = (byte[]) null;
          this.writeBuffer = (byte[]) null;
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
          int index1 = (int)Constants.N - (int)Constants.F;
          for (int index2 = 0; index2 < index1; ++index2)
            textBuffer[index2] = (byte) 32;
          int num1 = 0;
          while (num1 < originTextSize)
          {
            short num2 = DecodeChar();
            byte input = 0;
            if (num2 < (short) 256)
            {
              input = (byte) num2;
              if (funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
                return false;
              textBuffer[index1] = (byte) num2;
              index1 = index1 + 1 & (int)Constants.N - 1;
              ++num1;
            }
            else
            {
              int num3 = index1 - ((int) DecodePosition() + 1) & (int)Constants.N - 1;
              int num4 = (int) num2 - (int) byte.MaxValue + (int)Constants.THRESHOLD;
              for (int index2 = 0; index2 < num4; ++index2)
              {
                short num5 = (short) textBuffer[(int) (short) (num3 + index2 & (int)Constants.N - 1)];
                input = (byte) num5;
                if (funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
                  return false;
                textBuffer[index1] = (byte) num5;
                index1 = index1 + 1 & (int)Constants.N - 1;
                ++num1;
              }
            }
          }
        }
        finally
        {
          this.readBuffer = (byte[]) null;
          this.writeBuffer = (byte[]) null;
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
      internal static short N_CHAR = (short) (256 - (int)THRESHOLD + (int)F);
      internal static short T = (short) ((int)N_CHAR * 2 - 1);
      internal static short R = (short) ((int)T - 1);
      internal static ushort MAX_FREQ = 32768;
      internal static byte[] const_p_len = new byte[64]{ (byte) 3, (byte) 4, (byte) 4, (byte) 4, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8 };
      internal static byte[] const_p_code = new byte[64]{ (byte) 0, (byte) 32, (byte) 48, (byte) 64, (byte) 80, (byte) 88, (byte) 96, (byte) 104, (byte) 112, (byte) 120, (byte) 128, (byte) 136, (byte) 144, (byte) 148, (byte) 152, (byte) 156, (byte) 160, (byte) 164, (byte) 168, (byte) 172, (byte) 176, (byte) 180, (byte) 184, (byte) 188, (byte) 192, (byte) 194, (byte) 196, (byte) 198, (byte) 200, (byte) 202, (byte) 204, (byte) 206, (byte) 208, (byte) 210, (byte) 212, (byte) 214, (byte) 216, (byte) 218, (byte) 220, (byte) 222, (byte) 224, (byte) 226, (byte) 228, (byte) 230, (byte) 232, (byte) 234, (byte) 236, (byte) 238, (byte) 240, (byte) 241, (byte) 242, (byte) 243, (byte) 244, (byte) 245, (byte) 246, (byte) 247, (byte) 248, (byte) 249, (byte) 250, (byte) 251, (byte) 252, (byte) 253, (byte) 254, byte.MaxValue };
      internal static byte[] const_d_code = new byte[256]{ (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 12, (byte) 12, (byte) 12, (byte) 12, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 14, (byte) 14, (byte) 14, (byte) 14, (byte) 15, (byte) 15, (byte) 15, (byte) 15, (byte) 16, (byte) 16, (byte) 16, (byte) 16, (byte) 17, (byte) 17, (byte) 17, (byte) 17, (byte) 18, (byte) 18, (byte) 18, (byte) 18, (byte) 19, (byte) 19, (byte) 19, (byte) 19, (byte) 20, (byte) 20, (byte) 20, (byte) 20, (byte) 21, (byte) 21, (byte) 21, (byte) 21, (byte) 22, (byte) 22, (byte) 22, (byte) 22, (byte) 23, (byte) 23, (byte) 23, (byte) 23, (byte) 24, (byte) 24, (byte) 25, (byte) 25, (byte) 26, (byte) 26, (byte) 27, (byte) 27, (byte) 28, (byte) 28, (byte) 29, (byte) 29, (byte) 30, (byte) 30, (byte) 31, (byte) 31, (byte) 32, (byte) 32, (byte) 33, (byte) 33, (byte) 34, (byte) 34, (byte) 35, (byte) 35, (byte) 36, (byte) 36, (byte) 37, (byte) 37, (byte) 38, (byte) 38, (byte) 39, (byte) 39, (byte) 40, (byte) 40, (byte) 41, (byte) 41, (byte) 42, (byte) 42, (byte) 43, (byte) 43, (byte) 44, (byte) 44, (byte) 45, (byte) 45, (byte) 46, (byte) 46, (byte) 47, (byte) 47, (byte) 48, (byte) 49, (byte) 50, (byte) 51, (byte) 52, (byte) 53, (byte) 54, (byte) 55, (byte) 56, (byte) 57, (byte) 58, (byte) 59, (byte) 60, (byte) 61, (byte) 62, (byte) 63 };
      internal static byte[] const_d_len = new byte[256]{ (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8 };
    }
  }
}
