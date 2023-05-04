using System;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;

namespace VistaDB.Engine.Internal
{
  internal class Connection : IVistaDBConnection, IDisposable
  {
    internal readonly object syncRoot = new object();
    private VistaDBEngine parentEngine;
    private long connectionId;
    private Environment environment;

    protected Connection(VistaDBEngine engine, long id)
    {
      parentEngine = engine;
      connectionId = id;
      environment = new Environment();
    }

    internal VistaDBEngine ParentEngine
    {
      get
      {
        return parentEngine;
      }
    }

    internal object SyncRoot
    {
      get
      {
        return syncRoot;
      }
    }

    internal void AddNotification(DataStorage dataStorage)
    {
      environment.AddNotification(dataStorage);
    }

    internal void RemoveNotification(DataStorage dataStorage)
    {
      if (environment == null)
        return;
      environment.RemoveNotification(dataStorage);
    }

    protected virtual void Dispose(bool disposing)
    {
      lock (syncRoot)
      {
        if (disposing)
        {
          GC.SuppressFinalize(this);
          if (environment != null)
            environment.Clear();
          if (parentEngine != null)
            parentEngine.Remove(Id);
        }
        environment = null;
        parentEngine = null;
      }
    }

    public long Id
    {
      get
      {
        return connectionId;
      }
    }

    public int LockTimeout
    {
      get
      {
        return (int) environment.Get(Settings.LOCKTIMEOUT);
      }
      set
      {
        if (value < 0)
          value = Environment.DEFAULT_LOCK_TIMEOUT;
        if (value > 3600)
          value = 3600;
        environment.Set(Settings.LOCKTIMEOUT, value);
      }
    }

    public int PageSize
    {
      get
      {
        return (int) environment.Get(Settings.PAGESIZE);
      }
      set
      {
        environment.Set(Settings.PAGESIZE, value <= 0 ? Environment.DEFAULT_PAGELEN : value);
      }
    }

    public bool PersistentLockFiles
    {
      get
      {
        return (bool) environment.Get(Settings.PERSISTENTLOCKS);
      }
      set
      {
        environment.Set(Settings.PERSISTENTLOCKS, value);
      }
    }

    public int LCID
    {
      get
      {
        return (int) environment.Get(Settings.LCID);
      }
      set
      {
        environment.Set(Settings.LCID, value <= 0 ? Environment.DEFAULT_LCID : value);
      }
    }

    void IDisposable.Dispose()
    {
      Dispose(true);
    }

    internal enum Settings
    {
      LOCKTIMEOUT,
      LCID,
      PERSISTENTLOCKS,
      PAGESIZE,
    }

    private class Environment : Dictionary<Settings, object>
    {
      internal static readonly int DEFAULT_PAGELEN = 8;
      internal static readonly int DEFAULT_LCID = CultureInfo.CurrentCulture.LCID;
      internal static readonly int DEFAULT_LOCK_TIMEOUT = 10;
      private static readonly bool DEFAULT_PERSISTENTLOCKS = false;
      private NotifyList notifications;

      internal Environment()
      {
        notifications = new NotifyList();
        InitDefault();
      }

      internal Environment(Environment parent)
        : this()
      {
        foreach (Settings key in parent.Keys)
        {
          if (!ContainsKey(key))
            Add(key, parent[key]);
          else
            this[key] = parent[key];
        }
      }

      private bool Notify(Settings variable, object newValue)
      {
        foreach (DataStorage notification in (List<DataStorage>) notifications)
        {
          if (!notification.NotifyChangedEnvironment(variable, newValue))
            return false;
        }
        return true;
      }

      private string ConvertToString(Settings variable)
      {
        return null;
      }

      private void InitDefault()
      {
        Add(Settings.LOCKTIMEOUT, DEFAULT_LOCK_TIMEOUT);
        Add(Settings.PAGESIZE, DEFAULT_PAGELEN);
        Add(Settings.LCID, DEFAULT_LCID);
        Add(Settings.PERSISTENTLOCKS, DEFAULT_PERSISTENTLOCKS);
      }

      internal void AddNotification(DataStorage storage)
      {
        notifications.AddNotification(storage);
      }

      internal void RemoveNotification(DataStorage storage)
      {
        notifications.RemoveNotification(storage);
      }

      internal void Set(Settings variable, object newValue)
      {
        try
        {
          if (this[variable].Equals(newValue) || !Notify(variable, newValue))
            return;
          this[variable] = newValue;
        }
        catch (Exception ex)
        {
          throw new VistaDBException(ex, 320, ConvertToString(variable));
        }
      }

      internal object Get(Settings variable)
      {
        return this[variable];
      }

      private class NotifyList : List<DataStorage>
      {
        internal void AddNotification(DataStorage storage)
        {
          Add(storage);
        }

        internal void RemoveNotification(DataStorage storage)
        {
          Remove(storage);
        }
      }
    }
  }
}
