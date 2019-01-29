using System;
using System.Collections.Generic;
using System.Threading;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.IO
{
  internal class LockManager : IDisposable
  {
    private ulong storageLock = ulong.MaxValue;
    private LockManager.LockCollection userLocks = new LockManager.LockCollection();
    private LockManager.LockCollection rowIdCollection = new LockManager.LockCollection();
    private DataStorage storage;
    private bool isDisposed;

    internal LockManager(DataStorage storage)
    {
      this.storage = storage;
    }

    private void LockStorage()
    {
      this.storage.LowLevelLockStorage(0UL, 0);
      this.storageLock = 1UL;
    }

    private void UnlockStorage()
    {
      if (this.storage != null)
        this.storage.LowLevelUnlockStorage(0UL, 0);
      this.storageLock = ulong.MaxValue;
    }

    private void LockRow(ulong id)
    {
      this.storage.LowLevelLockRow((uint) id);
      lock (this.rowIdCollection.syncObject)
        this.rowIdCollection.SetLock(id);
    }

    private void UnlockRow(ulong id)
    {
      this.storage.LowLevelUnlockRow((uint) id);
      lock (this.rowIdCollection.syncObject)
        this.rowIdCollection.ResetLock(id);
    }

    private void UnlockAllRows()
    {
      lock (this.rowIdCollection.syncObject)
      {
        foreach (uint key in this.rowIdCollection.Keys)
          this.storage.LowLevelUnlockRow(key);
        this.rowIdCollection.Clear();
      }
    }

    private bool FindLock(LockManager.LockType type, ulong id)
    {
      if (type != LockManager.LockType.FileLock)
        return this.rowIdCollection.LockedStatus(id);
      return this.storageLock != ulong.MaxValue;
    }

    private bool IncreaseRef(LockManager.LockType type, ulong id)
    {
      if (!this.FindLock(type, id))
        return false;
      if (type == LockManager.LockType.FileLock)
      {
        ++this.storageLock;
      }
      else
      {
        lock (this.rowIdCollection.syncObject)
        {
          int rowId = this.rowIdCollection[id];
          int num;
          this.rowIdCollection[id] = num = rowId + 1;
        }
      }
      return true;
    }

    private bool DecreaseRef(LockManager.LockType type, ulong id)
    {
      if (!this.FindLock(type, id))
        return false;
      if (type == LockManager.LockType.FileLock)
      {
        if (this.storageLock > 0UL)
          --this.storageLock;
        return this.storageLock == 0UL;
      }
      lock (this.rowIdCollection.syncObject)
      {
        int rowId = this.rowIdCollection[id];
        if (rowId > 0)
          this.rowIdCollection[id] = --rowId;
        return rowId == 0;
      }
    }

    internal void LockObject(bool userLock, ulong id, LockManager.LockType type, ref bool actualLock, int lockTimeout)
    {
      actualLock = false;
      if (userLock)
      {
        if (this.userLocks.LockedStatus(id))
          return;
      }
      try
      {
        if (this.IncreaseRef(type, id))
          return;
        int millisecondsTimeout = 20;
        while (lockTimeout >= 0)
        {
          try
          {
            if (type == LockManager.LockType.FileLock)
              this.LockStorage();
            else
              this.LockRow(id);
            actualLock = true;
            break;
          }
          catch (Exception ex)
          {
            if (lockTimeout == 0)
              throw new VistaDBException(ex, 163, this.storage.Name);
          }
          Thread.Sleep(millisecondsTimeout);
          lockTimeout -= millisecondsTimeout;
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 161, this.storage.Name);
      }
      if (!userLock)
        return;
      this.userLocks.SetLock(id);
    }

    internal void UnlockObject(bool userLock, ulong id, LockManager.LockType type, bool waitForSynchAll)
    {
      if (this.DecreaseRef(type, id) && !waitForSynchAll)
      {
        if (type == LockManager.LockType.FileLock)
          this.UnlockStorage();
        else
          this.UnlockRow(id);
      }
      if (!userLock)
        return;
      this.userLocks.ResetLock(id);
    }

    internal void SynchAll()
    {
      if (this.storageLock == 0UL)
        this.UnlockStorage();
      lock (this.rowIdCollection.syncObject)
      {
        ulong[] numArray = new ulong[this.rowIdCollection.Count];
        int num = 0;
        foreach (ulong key in this.rowIdCollection.Keys)
        {
          if (this.rowIdCollection[key] == 0 && !this.userLocks.LockedStatus((ulong) (uint) key))
            numArray[num++] = key;
        }
        for (int index = 0; index < num; ++index)
          this.UnlockRow(numArray[index]);
      }
    }

    internal void UnlockAllItems()
    {
      if (this.storageLock != ulong.MaxValue)
        this.UnlockStorage();
      this.UnlockAllRows();
      this.userLocks.Clear();
    }

    public void Dispose()
    {
      if (this.isDisposed)
        return;
      this.isDisposed = true;
      GC.SuppressFinalize((object) this);
      try
      {
        if (this.storage != null)
          this.UnlockAllItems();
        this.userLocks = (LockManager.LockCollection) null;
        if (this.rowIdCollection != null)
          this.rowIdCollection.Clear();
        this.rowIdCollection = (LockManager.LockCollection) null;
        this.storage = (DataStorage) null;
      }
      catch (Exception ex)
      {
        throw;
      }
    }

    internal enum LockType
    {
      RowLock,
      FileLock,
    }

    private class LockCollection : Dictionary<ulong, int>
    {
      internal object syncObject = new object();

      internal void SetLock(ulong objectId)
      {
        this.Add(objectId, 1);
      }

      internal void ResetLock(ulong objectId)
      {
        this.Remove(objectId);
      }

      internal bool LockedStatus(ulong objectId)
      {
        return this.ContainsKey(objectId);
      }
    }
  }
}
