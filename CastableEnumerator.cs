





using System;
using System.Collections;
using System.Collections.Generic;

namespace VistaDB
{
  internal class CastableEnumerator<T, K> : IEnumerator<K>, IDisposable, IEnumerator where T : K
  {
    private IEnumerator<T> _enum;

    public CastableEnumerator(IEnumerator<T> enumerator)
    {
      _enum = enumerator;
    }

    public K Current
    {
      get
      {
        return (K) _enum.Current;
      }
    }

    public void Dispose()
    {
      _enum.Dispose();
    }

    object IEnumerator.Current
    {
      get
      {
        return (object) Current;
      }
    }

    public bool MoveNext()
    {
      return _enum.MoveNext();
    }

    public void Reset()
    {
      _enum.Reset();
    }
  }
}
