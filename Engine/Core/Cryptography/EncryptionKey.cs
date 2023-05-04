using System;
using System.Text;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.Cryptography
{
  internal struct EncryptionKey
  {
    private static EncryptionKey nullEncryptionKey = new EncryptionKey((string) null, Cypher.None);
    private string userKeyString;
    private Cypher cypher;

    internal static EncryptionKey Create(string keyString)
    {
      if (keyString != null)
        return new EncryptionKey(keyString, Cypher.Blowfish);
      return NullEncryptionKey;
    }

    private EncryptionKey(string userEncryptionKey, Cypher cypher)
    {
      if (userEncryptionKey != null && userEncryptionKey.Length == 0)
        throw new VistaDBException(461);
      userKeyString = userEncryptionKey;
      this.cypher = cypher;
    }

    internal static EncryptionKey NullEncryptionKey
    {
      get
      {
        return nullEncryptionKey;
      }
    }

    internal string Key
    {
      get
      {
        return userKeyString;
      }
    }

    internal Cypher Type
    {
      get
      {
        return cypher;
      }
    }

    internal Md5.Signature Md5Signature
    {
      get
      {
        if (Key == null || Type == Cypher.None)
          return Md5.Signature.EmptySignature;
        byte[] bytes = Encoding.Unicode.GetBytes(Key);
        byte[] array = new byte[bytes.Length + 1];
        array[0] = (byte) Type;
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
