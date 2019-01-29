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
      this.lzhCoding = new Lzh(new Lzh.LzhStreaming(this.ReadStreaming), new Lzh.LzhStreaming(this.WriteStreaming));
      this.encoding = Encoding.Unicode;
      this.Reset();
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
      this.unpackedBuffer = this.encoding.GetBytes(this.text);
      this.packedBuffer = new byte[this.unpackedBuffer.Length * 4];
    }
  }
}
