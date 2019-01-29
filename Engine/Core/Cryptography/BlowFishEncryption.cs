using System;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class BlowFishEncryption : Encryption
  {
    private static readonly int MINKEY_BLOWFISH_ENCRYPTION = 8;
    private static readonly int MAXKEY_BLOWFISH_ENCRYPTION = 56;
    private static readonly int step = 8;
    private static readonly int halfStep = 4;
    private BlowFish algo;

    internal BlowFishEncryption(string encryptionKeyBase)
      : base(EncryptionKey.Create(encryptionKeyBase), BlowFishEncryption.MINKEY_BLOWFISH_ENCRYPTION, BlowFishEncryption.MAXKEY_BLOWFISH_ENCRYPTION)
    {
      this.algo = new BlowFish(this.EncryptionKeyString);
    }

    protected override void OnEncrypt(byte[] source, byte[] destination, int offset, int len)
    {
      int startIndex1 = offset;
      int offset1 = offset;
      int num = len;
      while (len >= BlowFishEncryption.step)
      {
        int int32_1 = BitConverter.ToInt32(source, startIndex1);
        int startIndex2 = startIndex1 + BlowFishEncryption.halfStep;
        int int32_2 = BitConverter.ToInt32(source, startIndex2);
        startIndex1 = startIndex2 + BlowFishEncryption.halfStep;
        this.algo.Encrypt(ref int32_1, ref int32_2);
        int bytes = VdbBitConverter.GetBytes((uint) int32_1, destination, offset1, BlowFishEncryption.halfStep);
        offset1 = VdbBitConverter.GetBytes((uint) int32_2, destination, bytes, BlowFishEncryption.halfStep);
        len -= BlowFishEncryption.step;
      }
      if (len <= 0)
        return;
      base.OnEncrypt(source, destination, offset + (num - len), len);
    }

    protected override void OnDecrypt(byte[] source, byte[] destination, int offset, int len)
    {
      int startIndex1 = offset;
      int offset1 = offset;
      int num = len;
      while (len >= BlowFishEncryption.step)
      {
        int int32_1 = BitConverter.ToInt32(source, startIndex1);
        int startIndex2 = startIndex1 + BlowFishEncryption.halfStep;
        int int32_2 = BitConverter.ToInt32(source, startIndex2);
        startIndex1 = startIndex2 + BlowFishEncryption.halfStep;
        this.algo.Decrypt(ref int32_1, ref int32_2);
        int bytes = VdbBitConverter.GetBytes((uint) int32_1, destination, offset1, BlowFishEncryption.halfStep);
        offset1 = VdbBitConverter.GetBytes((uint) int32_2, destination, bytes, BlowFishEncryption.halfStep);
        len -= BlowFishEncryption.step;
      }
      if (len <= 0)
        return;
      base.OnEncrypt(source, destination, offset + (num - len), len);
    }
  }
}
