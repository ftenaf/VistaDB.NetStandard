using System;
using System.Collections.Generic;
using System.Threading;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.IO
{
  internal class LockManager : IDisposable
  {
    private ulong storageLock = ulong.MaxValue;
    private LockCollection userLocks = new LockCollection();
    private LockCollection rowIdCollection = new LockCollection();
    private DataStorage storage;
    private bool isDisposed;

    internal LockManager(DataStorage storage)
    {
      this.storage = storage;
    }

    private void LockStorage()
    {
      storage.LowLevelLockStorage(0UL, 0);
      storageLock = 1UL;
    }

    private void UnlockStorage()
    {
      if (storage != null)
        storage.LowLevelUnlockStorage(0UL, 0);
      storageLock = ulong.MaxValue;
    }

    private void LockRow(ulong id)
    {
      storage.LowLevelLockRow((uint) id);
      lock (rowIdCollection.syncObject)
        rowIdCollection.SetLock(id);
    }

    private void UnlockRow(ulong id)
    {
      storage.LowLevelUnlockRow((uint) id);
      lock (rowIdCollection.syncObject)
        rowIdCollection.ResetLock(id);
    }

    private void UnlockAllRows()
    {
      lock (rowIdCollection.syncObject)
      {
        foreach (uint key in rowIdCollection.Keys)
          storage.LowLevelUnlockRow(key);
        rowIdCollection.Clear();
      }
    }

    private bool FindLock(LockType type, ulong id)
    {
      if (type != LockType.FileLock)
        return rowIdCollection.LockedStatus(id);
      return storageLock != ulong.MaxValue;
    }

    private bool IncreaseRef(LockType type, ulong id)
    {
      if (!FindLock(type, id))
        return false;
      if (type == LockType.FileLock)
      {
        ++storageLock;
      }
      else
      {
        lock (rowIdCollection.syncObject)
        {
          int rowId = rowIdCollection[id];
          int num;
          rowIdCollection[id] = num = rowId + 1;
        }
      }
      return true;
    }

    private bool DecreaseRef(LockType type, ulong id)
    {
      if (!FindLock(type, id))
        return false;
      if (type == LockType.FileLock)
      {
        if (storageLock > 0UL)
          --storageLock;
        return storageLock == 0UL;
      }
      lock (rowIdCollection.syncObject)
      {
        int rowId = rowIdCollection[id];
        if (rowId > 0)
          rowIdCollection[id] = --rowId;
        return rowId == 0;
      }
    }

    internal void LockObject(bool userLock, ulong id, LockType type, ref bool actualLock, int lockTimeout)
    {
      actualLock = false;
      if (userLock)
      {
        if (userLocks.LockedStatus(id))
          return;
      }
      try
      {
        if (IncreaseRef(type, id))
          return;
        int millisecondsTimeout = 20;
        while (lockTimeout >= 0)
        {
          try
          {
            if (type == LockType.FileLock)
              LockStorage();
            else
              LockRow(id);
            actualLock = true;
            break;
          }
          catch (Exception ex)
          {
            if (lockTimeout == 0)
              throw new VistaDBException(ex, 163, storage.Name);
          }
          Thread.Sleep(millisecondsTimeout);
          lockTimeout -= millisecondsTimeout;
        }
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 161, storage.Name);
      }
      if (!userLock)
        return;
      userLocks.SetLock(id);
    }

    internal void UnlockObject(bool userLock, ulong id, LockType type, bool waitForSynchAll)
    {
      if (DecreaseRef(type, id) && !waitForSynchAll)
      {
        if (type == LockType.FileLock)
          UnlockStorage();
        else
          UnlockRow(id);
      }
      if (!userLock)
        return;
      userLocks.ResetLock(id);
    }

    internal void SynchAll()
    {
      if (storageLock == 0UL)
        UnlockStorage();
      lock (rowIdCollection.syncObject)
      {
        ulong[] numArray = new ulong[rowIdCollection.Count];
        int num = 0;
        foreach (ulong key in rowIdCollection.Keys)
        {
          if (rowIdCollection[key] == 0 && !userLocks.LockedStatus((uint)key))
            numArray[num++] = key;
        }
        for (int index = 0; index < num; ++index)
          UnlockRow(numArray[index]);
      }
    }

    internal void UnlockAllItems()
    {
      if (storageLock != ulong.MaxValue)
        UnlockStorage();
      UnlockAllRows();
      userLocks.Clear();
    }

    public void Dispose()
    {
      if (isDisposed)
        return;
      isDisposed = true;
      GC.SuppressFinalize(this);
      try
      {
        if (storage != null)
          UnlockAllItems();
        userLocks = null;
        if (rowIdCollection != null)
          rowIdCollection.Clear();
        rowIdCollection = null;
        storage = null;
      }
      catch (Exception)
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
        Add(objectId, 1);
      }

      internal void ResetLock(ulong objectId)
      {
        Remove(objectId);
      }

      internal bool LockedStatus(ulong objectId)
      {
        return ContainsKey(objectId);
      }
    }
  }
}
