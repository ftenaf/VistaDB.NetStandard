





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
        return _enum.Current;
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
        return Current;
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
