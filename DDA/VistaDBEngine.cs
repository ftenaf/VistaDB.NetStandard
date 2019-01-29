using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.DDA
{
  public sealed class VistaDBEngine : IDictionary<long, IVistaDBDDA>, ICollection<KeyValuePair<long, IVistaDBDDA>>, IEnumerable<KeyValuePair<long, IVistaDBDDA>>, IEnumerable
  {
    public static readonly VistaDBEngine Connections = new VistaDBEngine();
    private IDictionary<long, IVistaDBDDA> engines = (IDictionary<long, IVistaDBDDA>) new Dictionary<long, IVistaDBDDA>();
    private long nextEngineId;

    private VistaDBEngine()
    {
    }

    public IVistaDBDDA this[long id]
    {
      get
      {
        lock (this.engines)
        {
          IVistaDBDDA vistaDbdda;
          if (!this.engines.TryGetValue(id, out vistaDbdda))
            return (IVistaDBDDA) null;
          return vistaDbdda;
        }
      }
    }

    public IVistaDBDDA OpenDDA()
    {
      lock (this.engines)
      {
        try
        {
          DirectConnection instance = DirectConnection.CreateInstance(this, ++this.nextEngineId);
          this.engines.Add(instance.Id, (IVistaDBDDA) instance);
          return (IVistaDBDDA) instance;
        }
        catch (Exception ex)
        {
          throw new Exception(ex.Message + "\n Cannot instantiate local connection");
        }
      }
    }

    internal ILocalSQLConnection OpenSQLConnection(VistaDBConnection parentConnection, IDatabase database)
    {
      lock (this.engines)
      {
        try
        {
          return (ILocalSQLConnection) LocalSQLConnection.CreateInstance(this, ++this.nextEngineId, parentConnection, database);
        }
        catch (Exception ex)
        {
          throw new Exception(ex.Message + "\n Cannot instantiate local sql connection");
        }
      }
    }

    public int Count
    {
      get
      {
        lock (this.engines)
          return this.engines.Count;
      }
    }

    internal void Remove(long id)
    {
      lock (this.engines)
        this.engines.Remove(id);
    }

    public void Clear()
    {
      List<IVistaDBDDA> vistaDbddaList;
      lock (this.engines)
      {
        vistaDbddaList = new List<IVistaDBDDA>((IEnumerable<IVistaDBDDA>) this.engines.Values);
        this.engines.Clear();
      }
      foreach (IDisposable disposable in vistaDbddaList)
        disposable.Dispose();
    }

    void IDictionary<long, IVistaDBDDA>.Add(long key, IVistaDBDDA value)
    {
      throw new ReadOnlyException();
    }

    public bool ContainsKey(long key)
    {
      lock (this.engines)
        return this.engines.ContainsKey(key);
    }

    public ICollection<long> Keys
    {
      get
      {
        lock (this.engines)
          return this.engines.Keys;
      }
    }

    bool IDictionary<long, IVistaDBDDA>.Remove(long key)
    {
      throw new ReadOnlyException();
    }

    public bool TryGetValue(long key, out IVistaDBDDA value)
    {
      lock (this.engines)
        return this.engines.TryGetValue(key, out value);
    }

    public ICollection<IVistaDBDDA> Values
    {
      get
      {
        lock (this.engines)
          return this.engines.Values;
      }
    }

    IVistaDBDDA IDictionary<long, IVistaDBDDA>.this[long key]
    {
      get
      {
        return this[key];
      }
      set
      {
        throw new ReadOnlyException();
      }
    }

    void ICollection<KeyValuePair<long, IVistaDBDDA>>.Add(KeyValuePair<long, IVistaDBDDA> item)
    {
      throw new ReadOnlyException();
    }

    public bool Contains(KeyValuePair<long, IVistaDBDDA> item)
    {
      lock (this.engines)
        return this.engines.ContainsKey(item.Key);
    }

    public void CopyTo(KeyValuePair<long, IVistaDBDDA>[] array, int arrayIndex)
    {
      lock (this.engines)
        this.engines.CopyTo(array, arrayIndex);
    }

    public bool IsReadOnly
    {
      get
      {
        return true;
      }
    }

    bool ICollection<KeyValuePair<long, IVistaDBDDA>>.Remove(KeyValuePair<long, IVistaDBDDA> item)
    {
      throw new ReadOnlyException();
    }

    public IEnumerator<KeyValuePair<long, IVistaDBDDA>> GetEnumerator()
    {
      lock (this.engines)
        return this.engines.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return (IEnumerator) this.GetEnumerator();
    }
  }
}
