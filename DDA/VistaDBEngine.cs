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
    private IDictionary<long, IVistaDBDDA> engines = new Dictionary<long, IVistaDBDDA>();
    private long nextEngineId;

    private VistaDBEngine()
    {
    }

    public IVistaDBDDA this[long id]
    {
      get
      {
        lock (engines)
        {
          IVistaDBDDA vistaDbdda;
          if (!engines.TryGetValue(id, out vistaDbdda))
            return null;
          return vistaDbdda;
        }
      }
    }

    public IVistaDBDDA OpenDDA()
    {
      lock (engines)
      {
        try
        {
          DirectConnection instance = DirectConnection.CreateInstance(this, ++nextEngineId);
          engines.Add(instance.Id, instance);
          return instance;
        }
        catch (Exception ex)
        {
          throw new Exception(ex.Message + "\n Cannot instantiate local connection");
        }
      }
    }

    internal ILocalSQLConnection OpenSQLConnection(VistaDBConnection parentConnection, IDatabase database)
    {
      lock (engines)
      {
        try
        {
          return LocalSQLConnection.CreateInstance(this, ++nextEngineId, parentConnection, database);
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
        lock (engines)
          return engines.Count;
      }
    }

    internal void Remove(long id)
    {
      lock (engines)
        engines.Remove(id);
    }

    public void Clear()
    {
      List<IVistaDBDDA> vistaDbddaList;
      lock (engines)
      {
        vistaDbddaList = new List<IVistaDBDDA>(engines.Values);
        engines.Clear();
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
      lock (engines)
        return engines.ContainsKey(key);
    }

    public ICollection<long> Keys
    {
      get
      {
        lock (engines)
          return engines.Keys;
      }
    }

    bool IDictionary<long, IVistaDBDDA>.Remove(long key)
    {
      throw new ReadOnlyException();
    }

    public bool TryGetValue(long key, out IVistaDBDDA value)
    {
      lock (engines)
        return engines.TryGetValue(key, out value);
    }

    public ICollection<IVistaDBDDA> Values
    {
      get
      {
        lock (engines)
          return engines.Values;
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
      lock (engines)
        return engines.ContainsKey(item.Key);
    }

    public void CopyTo(KeyValuePair<long, IVistaDBDDA>[] array, int arrayIndex)
    {
      lock (engines)
        engines.CopyTo(array, arrayIndex);
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
      lock (engines)
        return engines.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }
  }
}
