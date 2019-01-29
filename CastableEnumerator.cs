





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
      this._enum = enumerator;
    }

    public K Current
    {
      get
      {
        return (K) this._enum.Current;
      }
    }

    public void Dispose()
    {
      this._enum.Dispose();
    }

    object IEnumerator.Current
    {
      get
      {
        return (object) this.Current;
      }
    }

    public bool MoveNext()
    {
      return this._enum.MoveNext();
    }

    public void Reset()
    {
      this._enum.Reset();
    }
  }
}
