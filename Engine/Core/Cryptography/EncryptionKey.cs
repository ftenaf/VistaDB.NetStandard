using System;
using System.Text;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.Cryptography
{
  internal struct EncryptionKey
  {
    private static EncryptionKey nullEncryptionKey = new EncryptionKey((string) null, EncryptionKey.Cypher.None);
    private string userKeyString;
    private EncryptionKey.Cypher cypher;

    internal static EncryptionKey Create(string keyString)
    {
      if (keyString != null)
        return new EncryptionKey(keyString, EncryptionKey.Cypher.Blowfish);
      return EncryptionKey.NullEncryptionKey;
    }

    private EncryptionKey(string userEncryptionKey, EncryptionKey.Cypher cypher)
    {
      if (userEncryptionKey != null && userEncryptionKey.Length == 0)
        throw new VistaDBException(461);
      this.userKeyString = userEncryptionKey;
      this.cypher = cypher;
    }

    internal static EncryptionKey NullEncryptionKey
    {
      get
      {
        return EncryptionKey.nullEncryptionKey;
      }
    }

    internal string Key
    {
      get
      {
        return this.userKeyString;
      }
    }

    internal EncryptionKey.Cypher Type
    {
      get
      {
        return this.cypher;
      }
    }

    internal Md5.Signature Md5Signature
    {
      get
      {
        if (this.Key == null || this.Type == EncryptionKey.Cypher.None)
          return Md5.Signature.EmptySignature;
        byte[] bytes = Encoding.Unicode.GetBytes(this.Key);
        byte[] array = new byte[bytes.Length + 1];
        array[0] = (byte) this.Type;
        Array.Copy((Array) bytes, 0, (Array) array, 1, bytes.Length);
        return new Md5().DigByteArray(array);
      }
    }

    public enum Cypher
    {
      Blowfish,
      None,
    }
  }
}
