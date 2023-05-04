using System;

namespace VistaDB.Engine.Core.Cryptography
{
  internal class Encryption
  {
    private EncryptionKey encryptionKey;
    private bool isValid;
    private int minKeyLen;
    private int maxKeyLen;

    internal Encryption()
    {
      encryptionKey = EncryptionKey.NullEncryptionKey;
    }

    protected Encryption(EncryptionKey encryptionKey, int minKeyLen, int maxKeyLen)
    {
      isValid = false;
      this.encryptionKey = encryptionKey;
      this.minKeyLen = minKeyLen;
      this.maxKeyLen = maxKeyLen;
      if (this.encryptionKey.Key == null || this.encryptionKey.Key.Length <= 0)
        return;
      isValid = true;
    }

    internal static Encryption CreateEncryption(EncryptionKey baseKey)
    {
      switch (baseKey.Type)
      {
        case EncryptionKey.Cypher.Blowfish:
          return new BlowFishEncryption(baseKey.Key);
        case EncryptionKey.Cypher.None:
          return null;
        default:
          return new Encryption();
      }
    }

    internal EncryptionKey EncryptionKeyString
    {
      get
      {
        return encryptionKey;
      }
    }

    internal int EncryptionKeyStringLength
    {
      get
      {
        return encryptionKey.Key.Length;
      }
    }

    internal int Step
    {
      get
      {
        return 8;
      }
    }

    internal void Encrypt(byte[] source, byte[] dest, int len)
    {
      if (!isValid)
        return;
      OnEncrypt(source, dest, 0, len);
    }

    internal void Encrypt(byte[] buffer, int offset, int len)
    {
      if (!isValid)
        return;
      OnEncrypt(buffer, buffer, offset, len);
    }

    internal void Decrypt(byte[] source, byte[] dest, int len)
    {
      if (!isValid)
        return;
      OnDecrypt(source, dest, 0, len);
    }

    internal void Decrypt(byte[] buffer, int offset, int len)
    {
      if (!isValid)
        return;
      OnDecrypt(buffer, buffer, offset, len);
    }

    protected virtual void OnEncrypt(byte[] source, byte[] destination, int offset, int len)
    {
      throw new NotImplementedException("Base OnEncrypt should never be called as it has no default implementation");
    }

    protected virtual void OnDecrypt(byte[] source, byte[] destination, int offset, int len)
    {
      throw new NotImplementedException("Base OnEncrypt should never be called as it has no default implementation");
    }
  }
}
