using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBKeyedCollection<TKey, TValue> : ICollection<TValue>, IEnumerable<TValue>, IEnumerable
  {
    ICollection<TKey> Keys { get; }

    ICollection<TValue> Values { get; }

    bool ContainsKey(TKey key);

    bool TryGetValue(TKey key, out TValue value);

    TValue this[TKey key] { get; }
  }
}
