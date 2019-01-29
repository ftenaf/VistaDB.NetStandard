using System;
using System.Collections.Generic;
using System.IO;
using System.Security;

namespace VistaDB.Engine.Core.IO
{
  internal class StorageManager : List<StorageHandle>, IDisposable
  {
    private static Random random = new Random();
    private static object SyncObj = new object();
    private bool serverMode;
    private bool isDisposed;

    internal StorageManager()
    {
    }

    internal bool ServerMode
    {
      get
      {
        return false;
      }
    }

    private bool TryGetHandle(string fileName, StorageHandle.StorageMode mode, bool persistent, out StorageHandle newHandle)
    {
      int count = this.Count;
      for (int index = 0; index < count; ++index)
      {
        StorageHandle storageHandle = this[index];
        if (storageHandle.Name.Equals(fileName, StringComparison.OrdinalIgnoreCase) && storageHandle.Mode == mode)
        {
          newHandle = storageHandle;
          return true;
        }
      }
      newHandle = (StorageHandle) null;
      return false;
    }

    private StorageHandle FindDuplicate(StorageHandle newHandle)
    {
      return (StorageHandle) null;
    }

    private StorageHandle LookForSameCompatible(string fileName, StorageHandle.StorageMode mode)
    {
      for (int index = 0; index <= this.Count - 1; ++index)
      {
        StorageHandle storageHandle = this[index];
        if (!(fileName != storageHandle.Name))
        {
          storageHandle.CheckCompatibility(mode, true);
          return storageHandle;
        }
      }
      return (StorageHandle) null;
    }

    internal StorageHandle CreateTemporaryStorage(int SizeofPage, bool transacted, bool isolated)
    {
      string str;
      try
      {
        str = Path.GetTempFileName();
        File.Delete(str);
      }
      catch (SecurityException ex)
      {
        str = "VistaDB." + Guid.NewGuid().ToString() + ".tmp";
      }
      return this.CreateTemporaryStorage(str, SizeofPage, transacted, isolated);
    }

    internal StorageHandle CreateTemporaryStorage(string fileName, int SizeofPage, bool transacted, bool isolated)
    {
      return this.OpenStorage(fileName, new StorageHandle.StorageMode(FileMode.CreateNew, FileShare.None, FileAccess.ReadWrite, FileAttributes.Hidden | FileAttributes.Archive | FileAttributes.Temporary | FileAttributes.NotContentIndexed, transacted, true, isolated), SizeofPage, false);
    }

    internal StorageHandle OpenOrCreateTemporaryStorage(string fileName, bool shared, int SizeofPage, bool isolated, bool persistent)
    {
      return this.OpenStorage(fileName, new StorageHandle.StorageMode(FileMode.OpenOrCreate, shared ? FileShare.ReadWrite : FileShare.None, FileAccess.ReadWrite, FileAttributes.Hidden | FileAttributes.Archive | FileAttributes.Temporary, false, !persistent, isolated), SizeofPage, persistent);
    }

    internal StorageHandle OpenStorage(string fileName, StorageHandle.StorageMode mode, int SizeofPage, bool persistent)
    {
      lock (StorageManager.SyncObj)
      {
        if (this.serverMode && (mode.Attributes & FileAttributes.Temporary) != FileAttributes.Temporary)
        {
          StorageHandle storageHandle = this.LookForSameCompatible(fileName, mode);
          if (storageHandle != null)
          {
            storageHandle.AddRef();
            return storageHandle;
          }
          mode.SetExclusive();
        }
        mode.VirtualLocks = mode.VirtualLocks || this.ServerMode;
        StorageHandle storageHandle1 = new StorageHandle(fileName, mode, SizeofPage, persistent);
        this.Add(storageHandle1);
        storageHandle1.AddRef();
        return storageHandle1;
      }
    }

    internal void CloseStorage(StorageHandle handle)
    {
      if (handle == null)
        return;
      lock (StorageManager.SyncObj)
      {
        if (!this.Contains(handle) || !handle.ReleaseRef())
          return;
        this.Remove(handle);
        handle.Dispose();
      }
    }

    public void Dispose()
    {
      lock (StorageManager.SyncObj)
      {
        if (!this.isDisposed)
        {
          this.Destroy();
          this.isDisposed = true;
        }
        GC.SuppressFinalize((object) this);
      }
    }

    ~StorageManager()
    {
      if (this.isDisposed)
        return;
      this.Destroy();
      this.isDisposed = true;
    }

    private void Destroy()
    {
      try
      {
        foreach (StorageHandle storageHandle in (List<StorageHandle>) this)
          storageHandle?.Dispose();
      }
      catch (Exception ex)
      {
        throw;
      }
      finally
      {
        this.Clear();
      }
    }
  }
}
