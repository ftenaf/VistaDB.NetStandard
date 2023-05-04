using System.Text;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class TestLZH
  {
    private string text = "Hello";
    private Lzh lzhCoding;
    private Encoding encoding;
    private byte[] unpackedBuffer;
    private byte[] packedBuffer;

    internal TestLZH(string text)
    {
      this.text = text;
      lzhCoding = new Lzh(new Lzh.LzhStreaming(ReadStreaming), new Lzh.LzhStreaming(WriteStreaming));
      encoding = Encoding.Unicode;
      Reset();
    }

    private int ReadStreaming(ref byte input, byte[] readBuffer, ref int offset)
    {
      if (offset >= readBuffer.Length)
        return 0;
      input = readBuffer[offset++];
      return 1;
    }

    private int WriteStreaming(ref byte input, byte[] writeBuffer, ref int offset)
    {
      if (offset >= writeBuffer.Length)
        return 0;
      writeBuffer[offset++] = input;
      return 1;
    }

    private void Reset()
    {
      unpackedBuffer = encoding.GetBytes(text);
      packedBuffer = new byte[unpackedBuffer.Length * 4];
    }
  }
}
