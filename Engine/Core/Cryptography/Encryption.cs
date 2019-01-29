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
      this.encryptionKey = EncryptionKey.NullEncryptionKey;
    }

    protected Encryption(EncryptionKey encryptionKey, int minKeyLen, int maxKeyLen)
    {
      this.isValid = false;
      this.encryptionKey = encryptionKey;
      this.minKeyLen = minKeyLen;
      this.maxKeyLen = maxKeyLen;
      if (this.encryptionKey.Key == null || this.encryptionKey.Key.Length <= 0)
        return;
      this.isValid = true;
    }

    internal static Encryption CreateEncryption(EncryptionKey baseKey)
    {
      switch (baseKey.Type)
      {
        case EncryptionKey.Cypher.Blowfish:
          return (Encryption) new BlowFishEncryption(baseKey.Key);
        case EncryptionKey.Cypher.None:
          return (Encryption) null;
        default:
          return new Encryption();
      }
    }

    internal EncryptionKey EncryptionKeyString
    {
      get
      {
        return this.encryptionKey;
      }
    }

    internal int EncryptionKeyStringLength
    {
      get
      {
        return this.encryptionKey.Key.Length;
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
      if (!this.isValid)
        return;
      this.OnEncrypt(source, dest, 0, len);
    }

    internal void Encrypt(byte[] buffer, int offset, int len)
    {
      if (!this.isValid)
        return;
      this.OnEncrypt(buffer, buffer, offset, len);
    }

    internal void Decrypt(byte[] source, byte[] dest, int len)
    {
      if (!this.isValid)
        return;
      this.OnDecrypt(source, dest, 0, len);
    }

    internal void Decrypt(byte[] buffer, int offset, int len)
    {
      if (!this.isValid)
        return;
      this.OnDecrypt(buffer, buffer, offset, len);
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
