using System.Runtime.InteropServices;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class Lzh
  {
    private static object synchRoot = new object();
    private byte[] textBuffer = new byte[(int) Lzh.Constants.N + (int) Lzh.Constants.F - 1];
    private short[] lson = new short[(int) Lzh.Constants.N + 1];
    private short[] dad = new short[(int) Lzh.Constants.N + 1];
    private short[] rson = new short[(int) Lzh.Constants.N + 1 + 256];
    private ushort[] freq = new ushort[(int) Lzh.Constants.T + 1];
    private short[] prnt = new short[(int) Lzh.Constants.T + (int) Lzh.Constants.N_CHAR];
    private short[] son = new short[(int) Lzh.Constants.T];
    private ushort getbuf;
    private ushort putbuf;
    private byte getlen;
    private byte putlen;
    private int codeSize;
    private int matchPosition;
    private int matchLength;
    private Lzh.LzhStreaming funcReadBlock;
    private Lzh.LzhStreaming funcWriteBlock;
    private byte[] readBuffer;
    private byte[] writeBuffer;
    private int readOffset;
    private int writeOffset;

    internal Lzh(Lzh.LzhStreaming funcReadBlock, Lzh.LzhStreaming funcWriteBlock)
    {
      this.funcReadBlock = funcReadBlock;
      this.funcWriteBlock = funcWriteBlock;
    }

    private void InitTree()
    {
      for (int index = (int) Lzh.Constants.N + 1; index <= (int) Lzh.Constants.N + 256; ++index)
        this.rson[index] = Lzh.Constants.NUL;
      for (int index = 0; index <= (int) Lzh.Constants.N; ++index)
        this.dad[index] = Lzh.Constants.NUL;
    }

    private void InsertNode(short r)
    {
      short num1 = (short) ((int) Lzh.Constants.N + 1 + (int) this.textBuffer[(int) r]);
      int num2 = 1;
      int num3 = (int) Lzh.Constants.F - 1;
      this.rson[(int) r] = Lzh.Constants.NUL;
      this.lson[(int) r] = Lzh.Constants.NUL;
      this.matchLength = 0;
      while (this.matchLength < num3)
      {
        if (num2 >= 0)
        {
          if ((int) this.rson[(int) num1] != (int) Lzh.Constants.NUL)
          {
            num1 = this.rson[(int) num1];
          }
          else
          {
            this.rson[(int) num1] = r;
            this.dad[(int) r] = num1;
            return;
          }
        }
        else if ((int) this.lson[(int) num1] != (int) Lzh.Constants.NUL)
        {
          num1 = this.lson[(int) num1];
        }
        else
        {
          this.lson[(int) num1] = r;
          this.dad[(int) r] = num1;
          return;
        }
        int num4 = 0;
        for (num2 = 0; num4 < num3 && num2 == 0; num2 = (int) this.textBuffer[(int) r + num4] - (int) this.textBuffer[(int) num1 + num4])
          ++num4;
        if (num4 > (int) Lzh.Constants.THRESHOLD)
        {
          int num5 = (int) (short) ((int) r - (int) num1 & (int) Lzh.Constants.N - 1) - 1;
          if (num4 > this.matchLength)
          {
            this.matchPosition = num5;
            this.matchLength = num4;
          }
          if (this.matchLength < num3 && num4 == this.matchLength && num5 < this.matchPosition)
            this.matchPosition = num5;
        }
      }
      this.dad[(int) r] = this.dad[(int) num1];
      this.lson[(int) r] = this.lson[(int) num1];
      this.rson[(int) r] = this.rson[(int) num1];
      this.dad[(int) this.lson[(int) num1]] = r;
      this.dad[(int) this.rson[(int) num1]] = r;
      if ((int) this.rson[(int) this.dad[(int) num1]] == (int) num1)
        this.rson[(int) this.dad[(int) num1]] = r;
      else
        this.lson[(int) this.dad[(int) num1]] = r;
      this.dad[(int) num1] = Lzh.Constants.NUL;
    }

    private void DeleteNode(short p)
    {
      if ((int) this.dad[(int) p] == (int) Lzh.Constants.NUL)
        return;
      short num;
      if ((int) this.rson[(int) p] == (int) Lzh.Constants.NUL)
        num = this.lson[(int) p];
      else if ((int) this.lson[(int) p] == (int) Lzh.Constants.NUL)
      {
        num = this.rson[(int) p];
      }
      else
      {
        num = this.lson[(int) p];
        if ((int) this.rson[(int) num] != (int) Lzh.Constants.NUL)
        {
          do
          {
            num = this.rson[(int) num];
          }
          while ((int) this.rson[(int) num] != (int) Lzh.Constants.NUL);
          this.rson[(int) this.dad[(int) num]] = this.lson[(int) num];
          this.dad[(int) this.lson[(int) num]] = this.dad[(int) num];
          this.lson[(int) num] = this.lson[(int) p];
          this.dad[(int) this.lson[(int) p]] = num;
        }
        this.rson[(int) num] = this.rson[(int) p];
        this.dad[(int) this.rson[(int) p]] = num;
      }
      this.dad[(int) num] = this.dad[(int) p];
      if ((int) this.rson[(int) this.dad[(int) p]] == (int) p)
        this.rson[(int) this.dad[(int) p]] = num;
      else
        this.lson[(int) this.dad[(int) p]] = num;
      this.dad[(int) p] = Lzh.Constants.NUL;
    }

    private ushort GetBit()
    {
      byte input = 0;
      while (this.getlen <= (byte) 8)
      {
        this.getbuf |= (ushort) ((this.funcReadBlock(ref input, this.readBuffer, ref this.readOffset) == 1 ? (uint) (ushort) input : 0U) << 8 - (int) this.getlen);
        this.getlen += (byte) 8;
      }
      ushort getbuf = this.getbuf;
      this.getbuf <<= 1;
      --this.getlen;
      return (short) getbuf >= (short) 0 ? (ushort) 0 : (ushort) 1;
    }

    private int GetByte()
    {
      byte input = 0;
      while (this.getlen <= (byte) 8)
      {
        this.getbuf |= (ushort) ((this.funcReadBlock(ref input, this.readBuffer, ref this.readOffset) == 1 ? (uint) (ushort) input : 0U) << 8 - (int) this.getlen);
        this.getlen += (byte) 8;
      }
      ushort getbuf = this.getbuf;
      this.getbuf <<= 8;
      this.getlen -= (byte) 8;
      return (int) getbuf >> 8;
    }

    private bool Putcode(ushort l, ushort c)
    {
      this.putbuf |= (ushort) ((uint) c >> (int) this.putlen);
      this.putlen += (byte) l;
      if (this.putlen >= (byte) 8)
      {
        byte input = (byte) ((uint) this.putbuf >> 8);
        if (this.funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
          return false;
        this.putlen -= (byte) 8;
        if (this.putlen >= (byte) 8)
        {
          byte putbuf = (byte) this.putbuf;
          if (this.funcWriteBlock(ref putbuf, this.writeBuffer, ref this.writeOffset) == 0)
            return false;
          this.putlen -= (byte) 8;
          this.putbuf = (ushort) ((uint) c << (int) l - (int) this.putlen);
          this.codeSize += 2;
        }
        else
        {
          this.putbuf <<= 8;
          ++this.codeSize;
        }
      }
      return true;
    }

    private void StartHuff()
    {
      for (short index = 0; (int) index < (int) Lzh.Constants.N_CHAR; ++index)
      {
        this.freq[(int) index] = (ushort) 1;
        this.son[(int) index] = (short) ((int) index + (int) Lzh.Constants.T);
        this.prnt[(int) index + (int) Lzh.Constants.T] = index;
      }
      short num = 0;
      for (short nChar = Lzh.Constants.N_CHAR; (int) nChar <= (int) Lzh.Constants.R; ++nChar)
      {
        this.freq[(int) nChar] = (ushort) ((uint) this.freq[(int) num] + (uint) this.freq[(int) num + 1]);
        this.son[(int) nChar] = num;
        this.prnt[(int) num] = nChar;
        this.prnt[(int) num + 1] = nChar;
        num += (short) 2;
      }
      this.freq[(int) Lzh.Constants.T] = ushort.MaxValue;
      this.prnt[(int) Lzh.Constants.R] = (short) 0;
    }

    private void Reconstruct()
    {
      int index1 = 0;
      for (short index2 = 0; (int) index2 < (int) Lzh.Constants.T; ++index2)
      {
        if ((int) this.son[(int) index2] >= (int) Lzh.Constants.T)
        {
          this.freq[index1] = (ushort) (((int) this.freq[(int) index2] + 1) / 2);
          this.son[index1] = this.son[(int) index2];
          ++index1;
        }
      }
      short num1 = 0;
      int nChar = (int) Lzh.Constants.N_CHAR;
      for (; nChar < (int) Lzh.Constants.T; ++nChar)
      {
        int index2 = (int) num1 + 1;
        ushort num2 = (ushort) ((uint) this.freq[(int) num1] + (uint) this.freq[index2]);
        this.freq[nChar] = num2;
        int index3 = nChar - 1;
        while ((int) num2 < (int) this.freq[index3])
          --index3;
        int index4 = index3 + 1;
        ushort num3 = (ushort) (nChar - index4 << 1);
        int num4 = index4 + 1;
        for (int index5 = (int) num3 - 1; index5 >= 0; --index5)
          this.freq[num4 + index5] = this.freq[index4 + index5];
        this.freq[index4] = num2;
        for (int index5 = (int) num3 - 1; index5 >= 0; --index5)
          this.son[num4 + index5] = this.son[index4 + index5];
        this.son[index4] = num1;
        num1 += (short) 2;
      }
      for (short index2 = 0; (int) index2 < (int) Lzh.Constants.T; ++index2)
      {
        int index3 = (int) this.son[(int) index2];
        this.prnt[index3] = index2;
        if (index3 < (int) Lzh.Constants.T)
          this.prnt[index3 + 1] = index2;
      }
    }

    private void Update(short c)
    {
      if ((int) this.freq[(int) Lzh.Constants.R] == (int) Lzh.Constants.MAX_FREQ)
        this.Reconstruct();
      c = this.prnt[(int) c + (int) Lzh.Constants.T];
      do
      {
        ++this.freq[(int) c];
        ushort num1 = this.freq[(int) c];
        short num2 = (short) ((int) c + 1);
        if ((int) num1 > (int) this.freq[(int) num2])
        {
          while ((int) num1 > (int) this.freq[(int) num2])
            ++num2;
          short num3 = (short) ((int) num2 - 1);
          this.freq[(int) c] = this.freq[(int) num3];
          this.freq[(int) num3] = num1;
          short num4 = this.son[(int) c];
          this.prnt[(int) num4] = num3;
          if ((int) num4 < (int) Lzh.Constants.T)
            this.prnt[(int) num4 + 1] = num3;
          short num5 = this.son[(int) num3];
          this.son[(int) num3] = num4;
          this.prnt[(int) num5] = c;
          if ((int) num5 < (int) Lzh.Constants.T)
            this.prnt[(int) num5 + 1] = c;
          this.son[(int) c] = num5;
          c = num3;
        }
        c = this.prnt[(int) c];
      }
      while (c != (short) 0);
    }

    private bool EncodeChar(ushort c)
    {
      ushort c1 = 0;
      ushort l = 0;
      int index = (int) this.prnt[(int) c + (int) Lzh.Constants.T];
      do
      {
        c1 >>= 1;
        if ((index & 1) != 0)
          c1 += (ushort) 32768;
        ++l;
        index = (int) this.prnt[index];
      }
      while (index != (int) Lzh.Constants.R);
      if (!this.Putcode(l, c1))
        return false;
      this.Update((short) c);
      return true;
    }

    private bool EncodePosition(ushort c)
    {
      ushort num1 = (ushort) ((uint) c >> 6);
      ushort num2 = (ushort) Lzh.Constants.const_p_code[(int) num1];
      if (this.Putcode((ushort) Lzh.Constants.const_p_len[(int) num1], (ushort) ((uint) num2 << 8)))
        return this.Putcode((ushort) 6, (ushort) (((int) c & 63) << 10));
      return false;
    }

    private bool EncodeEnd()
    {
      if (this.putlen != (byte) 0)
      {
        byte input = (byte) ((uint) this.putbuf >> 8);
        if (this.funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
          return false;
        ++this.codeSize;
      }
      return true;
    }

    private short DecodeChar()
    {
      short num = this.son[(int) Lzh.Constants.R];
      while ((int) num < (int) Lzh.Constants.T)
        num = this.son[(int) num + (int) this.GetBit()];
      short c = (short) ((int) num - (int) Lzh.Constants.T);
      this.Update(c);
      return c;
    }

    private ushort DecodePosition()
    {
      int index1 = this.GetByte();
      ushort num = (ushort) ((uint) Lzh.Constants.const_d_code[index1] << 6);
      for (int index2 = (int) Lzh.Constants.const_d_len[index1] - 2; index2 != 0; --index2)
        index1 = (index1 << 1) + (int) this.GetBit();
      return (ushort) ((uint) num | (uint) (index1 & 63));
    }

    private void InitPackLZH()
    {
      this.putlen = (byte) 0;
      this.putbuf = (ushort) 0;
      this.matchPosition = 0;
      this.matchLength = 0;
      this.codeSize = 0;
      int index1 = 0;
      for (int length = this.lson.Length; index1 < length; ++index1)
        this.lson[index1] = (short) 0;
      int index2 = 0;
      for (int length = this.dad.Length; index2 < length; ++index2)
        this.dad[index2] = (short) 0;
      int index3 = 0;
      for (int length = this.rson.Length; index3 < length; ++index3)
        this.rson[index3] = (short) 0;
      int index4 = 0;
      for (int length = this.textBuffer.Length; index4 < length; ++index4)
        this.textBuffer[index4] = (byte) 0;
      int index5 = 0;
      for (int length = this.freq.Length; index5 < length; ++index5)
        this.freq[index5] = (ushort) 0;
      int index6 = 0;
      for (int length = this.prnt.Length; index6 < length; ++index6)
        this.prnt[index6] = (short) 0;
      int index7 = 0;
      for (int length = this.son.Length; index7 < length; ++index7)
        this.son[index7] = (short) 0;
    }

    private void InitUnpackLZH()
    {
      this.getbuf = (ushort) 0;
      this.getlen = (byte) 0;
      int index1 = 0;
      for (int length = this.freq.Length; index1 < length; ++index1)
        this.freq[index1] = (ushort) 0;
      int index2 = 0;
      for (int length = this.prnt.Length; index2 < length; ++index2)
        this.prnt[index2] = (short) 0;
      int index3 = 0;
      for (int length = this.son.Length; index3 < length; ++index3)
        this.son[index3] = (short) 0;
    }

    internal int LZHPack(ref int writtenBytes, byte[] readBuffer, int readOffset, byte[] writeBuffer, int writeOffset)
    {
      lock (Lzh.synchRoot)
      {
        int num1 = 0;
        writtenBytes = 0;
        this.InitPackLZH();
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.readOffset = readOffset;
        this.writeOffset = writeOffset;
        try
        {
          this.StartHuff();
          this.InitTree();
          short p = 0;
          short r = (short) ((int) Lzh.Constants.N - (int) Lzh.Constants.F);
          for (int index = 0; index < (int) r; ++index)
            this.textBuffer[index] = (byte) 32;
          int num2 = 0;
          byte input;
          for (input = (byte) 0; num2 < (int) Lzh.Constants.F && this.funcReadBlock(ref input, this.readBuffer, ref this.readOffset) != 0; ++num2)
            this.textBuffer[(int) r + num2] = input;
          num1 = num2;
          for (short index = 1; (int) index <= (int) Lzh.Constants.F; ++index)
            this.InsertNode((short) ((int) r - (int) index));
          this.InsertNode(r);
          do
          {
            if (this.matchLength > num2)
              this.matchLength = num2;
            if (this.matchLength <= (int) Lzh.Constants.THRESHOLD)
            {
              this.matchLength = 1;
              if (!this.EncodeChar((ushort) this.textBuffer[(int) r]))
                return this.codeSize;
            }
            else if (!this.EncodeChar((ushort) ((int) byte.MaxValue - (int) Lzh.Constants.THRESHOLD + this.matchLength)) || !this.EncodePosition((ushort) this.matchPosition))
              return this.codeSize;
            int matchLength = (int) (short) this.matchLength;
            short num3;
            for (num3 = (short) 0; (int) num3 < matchLength && this.funcReadBlock(ref input, this.readBuffer, ref this.readOffset) != 0; ++num3)
            {
              this.DeleteNode(p);
              this.textBuffer[(int) p] = input;
              if ((int) p < (int) (short) ((int) Lzh.Constants.F - 1))
                this.textBuffer[(int) p + (int) Lzh.Constants.N] = input;
              p = (short) ((int) p + 1 & (int) Lzh.Constants.N - 1);
              r = (short) ((int) r + 1 & (int) Lzh.Constants.N - 1);
              this.InsertNode(r);
            }
            num1 += (int) num3;
            while ((int) num3 < matchLength)
            {
              ++num3;
              this.DeleteNode(p);
              p = (short) ((int) p + 1 & (int) Lzh.Constants.N - 1);
              r = (short) ((int) r + 1 & (int) Lzh.Constants.N - 1);
              --num2;
              if (num2 != 0)
                this.InsertNode(r);
            }
          }
          while (num2 > 0);
          if (!this.EncodeEnd())
            return this.codeSize;
        }
        finally
        {
          this.readBuffer = (byte[]) null;
          this.writeBuffer = (byte[]) null;
        }
        writtenBytes = num1;
        return this.codeSize;
      }
    }

    internal bool LZHUnpack(int originTextSize, byte[] readBuffer, int readOffset, byte[] writeBuffer, int writeOffset)
    {
      lock (Lzh.synchRoot)
      {
        this.InitUnpackLZH();
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.readOffset = readOffset;
        this.writeOffset = writeOffset;
        try
        {
          this.StartHuff();
          int index1 = (int) Lzh.Constants.N - (int) Lzh.Constants.F;
          for (int index2 = 0; index2 < index1; ++index2)
            this.textBuffer[index2] = (byte) 32;
          int num1 = 0;
          while (num1 < originTextSize)
          {
            short num2 = this.DecodeChar();
            byte input = 0;
            if (num2 < (short) 256)
            {
              input = (byte) num2;
              if (this.funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
                return false;
              this.textBuffer[index1] = (byte) num2;
              index1 = index1 + 1 & (int) Lzh.Constants.N - 1;
              ++num1;
            }
            else
            {
              int num3 = index1 - ((int) this.DecodePosition() + 1) & (int) Lzh.Constants.N - 1;
              int num4 = (int) num2 - (int) byte.MaxValue + (int) Lzh.Constants.THRESHOLD;
              for (int index2 = 0; index2 < num4; ++index2)
              {
                short num5 = (short) this.textBuffer[(int) (short) (num3 + index2 & (int) Lzh.Constants.N - 1)];
                input = (byte) num5;
                if (this.funcWriteBlock(ref input, this.writeBuffer, ref this.writeOffset) == 0)
                  return false;
                this.textBuffer[index1] = (byte) num5;
                index1 = index1 + 1 & (int) Lzh.Constants.N - 1;
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
      internal static short NUL = Lzh.Constants.N;
      internal static short N_CHAR = (short) (256 - (int) Lzh.Constants.THRESHOLD + (int) Lzh.Constants.F);
      internal static short T = (short) ((int) Lzh.Constants.N_CHAR * 2 - 1);
      internal static short R = (short) ((int) Lzh.Constants.T - 1);
      internal static ushort MAX_FREQ = 32768;
      internal static byte[] const_p_len = new byte[64]{ (byte) 3, (byte) 4, (byte) 4, (byte) 4, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8 };
      internal static byte[] const_p_code = new byte[64]{ (byte) 0, (byte) 32, (byte) 48, (byte) 64, (byte) 80, (byte) 88, (byte) 96, (byte) 104, (byte) 112, (byte) 120, (byte) 128, (byte) 136, (byte) 144, (byte) 148, (byte) 152, (byte) 156, (byte) 160, (byte) 164, (byte) 168, (byte) 172, (byte) 176, (byte) 180, (byte) 184, (byte) 188, (byte) 192, (byte) 194, (byte) 196, (byte) 198, (byte) 200, (byte) 202, (byte) 204, (byte) 206, (byte) 208, (byte) 210, (byte) 212, (byte) 214, (byte) 216, (byte) 218, (byte) 220, (byte) 222, (byte) 224, (byte) 226, (byte) 228, (byte) 230, (byte) 232, (byte) 234, (byte) 236, (byte) 238, (byte) 240, (byte) 241, (byte) 242, (byte) 243, (byte) 244, (byte) 245, (byte) 246, (byte) 247, (byte) 248, (byte) 249, (byte) 250, (byte) 251, (byte) 252, (byte) 253, (byte) 254, byte.MaxValue };
      internal static byte[] const_d_code = new byte[256]{ (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 9, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 10, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 11, (byte) 12, (byte) 12, (byte) 12, (byte) 12, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 14, (byte) 14, (byte) 14, (byte) 14, (byte) 15, (byte) 15, (byte) 15, (byte) 15, (byte) 16, (byte) 16, (byte) 16, (byte) 16, (byte) 17, (byte) 17, (byte) 17, (byte) 17, (byte) 18, (byte) 18, (byte) 18, (byte) 18, (byte) 19, (byte) 19, (byte) 19, (byte) 19, (byte) 20, (byte) 20, (byte) 20, (byte) 20, (byte) 21, (byte) 21, (byte) 21, (byte) 21, (byte) 22, (byte) 22, (byte) 22, (byte) 22, (byte) 23, (byte) 23, (byte) 23, (byte) 23, (byte) 24, (byte) 24, (byte) 25, (byte) 25, (byte) 26, (byte) 26, (byte) 27, (byte) 27, (byte) 28, (byte) 28, (byte) 29, (byte) 29, (byte) 30, (byte) 30, (byte) 31, (byte) 31, (byte) 32, (byte) 32, (byte) 33, (byte) 33, (byte) 34, (byte) 34, (byte) 35, (byte) 35, (byte) 36, (byte) 36, (byte) 37, (byte) 37, (byte) 38, (byte) 38, (byte) 39, (byte) 39, (byte) 40, (byte) 40, (byte) 41, (byte) 41, (byte) 42, (byte) 42, (byte) 43, (byte) 43, (byte) 44, (byte) 44, (byte) 45, (byte) 45, (byte) 46, (byte) 46, (byte) 47, (byte) 47, (byte) 48, (byte) 49, (byte) 50, (byte) 51, (byte) 52, (byte) 53, (byte) 54, (byte) 55, (byte) 56, (byte) 57, (byte) 58, (byte) 59, (byte) 60, (byte) 61, (byte) 62, (byte) 63 };
      internal static byte[] const_d_len = new byte[256]{ (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 3, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 4, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 5, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 6, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 7, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8, (byte) 8 };
    }
  }
}
