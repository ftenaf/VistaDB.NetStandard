using System;
using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal class VistaDBKeyedCollection<TKey, TValue> : Dictionary<TKey, TValue>, IVistaDBKeyedCollection<TKey, TValue>, ICollection<TValue>, IEnumerable<TValue>, IEnumerable
  {
    internal VistaDBKeyedCollection(IEqualityComparer<TKey> comparer)
      : base(25, comparer)
    {
    }

    internal VistaDBKeyedCollection()
      : base(25)
    {
    }

    ICollection<TKey> IVistaDBKeyedCollection<TKey, TValue>.Keys
    {
      get
      {
        return (ICollection<TKey>) this.Keys;
      }
    }

    ICollection<TValue> IVistaDBKeyedCollection<TKey, TValue>.Values
    {
      get
      {
        return (ICollection<TValue>) this.Values;
      }
    }

    bool IVistaDBKeyedCollection<TKey, TValue>.ContainsKey(TKey key)
    {
      return this.ContainsKey(key);
    }

    bool IVistaDBKeyedCollection<TKey, TValue>.TryGetValue(TKey key, out TValue value)
    {
      return this.TryGetValue(key, out value);
    }

    TValue IVistaDBKeyedCollection<TKey, TValue>.this[TKey key]
    {
      get
      {
        TValue obj;
        if (!this.TryGetValue(key, out obj))
          return default (TValue);
        return obj;
      }
    }

    void ICollection<TValue>.Add(TValue item)
    {
      throw new NotSupportedException();
    }

    void ICollection<TValue>.Clear()
    {
      throw new NotSupportedException();
    }

    bool ICollection<TValue>.Contains(TValue item)
    {
      return this.ContainsValue(item);
    }

    void ICollection<TValue>.CopyTo(TValue[] array, int arrayIndex)
    {
      new List<TValue>((IEnumerable<TValue>) this.Values).ToArray().CopyTo((Array) array, arrayIndex);
    }

    int ICollection<TValue>.Count
    {
      get
      {
        return this.Count;
      }
    }

    bool ICollection<TValue>.IsReadOnly
    {
      get
      {
        return true;
      }
    }

    bool ICollection<TValue>.Remove(TValue item)
    {
      throw new NotSupportedException();
    }

    IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator()
    {
      return (IEnumerator<TValue>) this.Values.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return (IEnumerator) this.Values.GetEnumerator();
    }
  }
}
