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
    private Connection.Environment environment;

    protected Connection(VistaDBEngine engine, long id)
    {
      this.parentEngine = engine;
      this.connectionId = id;
      this.environment = new Connection.Environment();
    }

    internal VistaDBEngine ParentEngine
    {
      get
      {
        return this.parentEngine;
      }
    }

    internal object SyncRoot
    {
      get
      {
        return this.syncRoot;
      }
    }

    internal void AddNotification(DataStorage dataStorage)
    {
      this.environment.AddNotification(dataStorage);
    }

    internal void RemoveNotification(DataStorage dataStorage)
    {
      if (this.environment == null)
        return;
      this.environment.RemoveNotification(dataStorage);
    }

    protected virtual void Dispose(bool disposing)
    {
      lock (this.syncRoot)
      {
        if (disposing)
        {
          GC.SuppressFinalize((object) this);
          if (this.environment != null)
            this.environment.Clear();
          if (this.parentEngine != null)
            this.parentEngine.Remove(this.Id);
        }
        this.environment = (Connection.Environment) null;
        this.parentEngine = (VistaDBEngine) null;
      }
    }

    public long Id
    {
      get
      {
        return this.connectionId;
      }
    }

    public int LockTimeout
    {
      get
      {
        return (int) this.environment.Get(Connection.Settings.LOCKTIMEOUT);
      }
      set
      {
        if (value < 0)
          value = Connection.Environment.DEFAULT_LOCK_TIMEOUT;
        if (value > 3600)
          value = 3600;
        this.environment.Set(Connection.Settings.LOCKTIMEOUT, (object) value);
      }
    }

    public int PageSize
    {
      get
      {
        return (int) this.environment.Get(Connection.Settings.PAGESIZE);
      }
      set
      {
        this.environment.Set(Connection.Settings.PAGESIZE, (object) (value <= 0 ? Connection.Environment.DEFAULT_PAGELEN : value));
      }
    }

    public bool PersistentLockFiles
    {
      get
      {
        return (bool) this.environment.Get(Connection.Settings.PERSISTENTLOCKS);
      }
      set
      {
        this.environment.Set(Connection.Settings.PERSISTENTLOCKS, (object) value);
      }
    }

    public int LCID
    {
      get
      {
        return (int) this.environment.Get(Connection.Settings.LCID);
      }
      set
      {
        this.environment.Set(Connection.Settings.LCID, (object) (value <= 0 ? Connection.Environment.DEFAULT_LCID : value));
      }
    }

    void IDisposable.Dispose()
    {
      this.Dispose(true);
    }

    internal enum Settings
    {
      LOCKTIMEOUT,
      LCID,
      PERSISTENTLOCKS,
      PAGESIZE,
    }

    private class Environment : Dictionary<Connection.Settings, object>
    {
      internal static readonly int DEFAULT_PAGELEN = 8;
      internal static readonly int DEFAULT_LCID = CultureInfo.CurrentCulture.LCID;
      internal static readonly int DEFAULT_LOCK_TIMEOUT = 10;
      private static readonly bool DEFAULT_PERSISTENTLOCKS = false;
      private Connection.Environment.NotifyList notifications;

      internal Environment()
      {
        this.notifications = new Connection.Environment.NotifyList();
        this.InitDefault();
      }

      internal Environment(Connection.Environment parent)
        : this()
      {
        foreach (Connection.Settings key in parent.Keys)
        {
          if (!this.ContainsKey(key))
            this.Add(key, parent[key]);
          else
            this[key] = parent[key];
        }
      }

      private bool Notify(Connection.Settings variable, object newValue)
      {
        foreach (DataStorage notification in (List<DataStorage>) this.notifications)
        {
          if (!notification.NotifyChangedEnvironment(variable, newValue))
            return false;
        }
        return true;
      }

      private string ConvertToString(Connection.Settings variable)
      {
        return (string) null;
      }

      private void InitDefault()
      {
        this.Add(Connection.Settings.LOCKTIMEOUT, (object) Connection.Environment.DEFAULT_LOCK_TIMEOUT);
        this.Add(Connection.Settings.PAGESIZE, (object) Connection.Environment.DEFAULT_PAGELEN);
        this.Add(Connection.Settings.LCID, (object) Connection.Environment.DEFAULT_LCID);
        this.Add(Connection.Settings.PERSISTENTLOCKS, (object) Connection.Environment.DEFAULT_PERSISTENTLOCKS);
      }

      internal void AddNotification(DataStorage storage)
      {
        this.notifications.AddNotification(storage);
      }

      internal void RemoveNotification(DataStorage storage)
      {
        this.notifications.RemoveNotification(storage);
      }

      internal void Set(Connection.Settings variable, object newValue)
      {
        try
        {
          if (this[variable].Equals(newValue) || !this.Notify(variable, newValue))
            return;
          this[variable] = newValue;
        }
        catch (Exception ex)
        {
          throw new VistaDBException(ex, 320, this.ConvertToString(variable));
        }
      }

      internal object Get(Connection.Settings variable)
      {
        return this[variable];
      }

      private class NotifyList : List<DataStorage>
      {
        internal void AddNotification(DataStorage storage)
        {
          this.Add(storage);
        }

        internal void RemoveNotification(DataStorage storage)
        {
          this.Remove(storage);
        }
      }
    }
  }
}
