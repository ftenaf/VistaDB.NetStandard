using System;
using System.Collections.Generic;
using System.IO;
using System.IO.IsolatedStorage;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.IO
{
  internal class StorageHandle : IDisposable
  {
    internal static readonly int DEFAULT_SIZE_OF_PAGE = 1024;
    private object syncObject = new object();
    private FreeSpaceCache freeCache = new FreeSpaceCache();
    internal FileStream fileStream;
    private int pageSize;
    private StorageMode storageMode;
    private StreamLocksDictionary lockTables;
    private int instanceCount;
    private CacheSystem cache;
    private bool noReadAheadCache;
    private bool noWriteBehindCache;
    private bool persistentFile;
    private bool isDisposed;

    internal StorageHandle(string fileName, StorageMode storageMode, int pageSize, bool persistent)
    {
      this.storageMode = storageMode;
      this.pageSize = pageSize;
      persistentFile = persistent;
      Open(fileName, pageSize, !storageMode.Temporary);
      lockTables = new StreamLocksDictionary(this);
    }

    internal object SyncObject
    {
      get
      {
        return syncObject;
      }
    }

    internal StorageMode Mode
    {
      get
      {
        return storageMode;
      }
    }

    internal string Name
    {
      get
      {
        return fileStream.Name;
      }
    }

    internal bool Transacted
    {
      get
      {
        return storageMode.Transacted;
      }
    }

    internal bool IsolatedStorage
    {
      get
      {
        return storageMode.IsolatedStorage;
      }
    }

    internal bool NoReadAheadCache
    {
      get
      {
        if (cache != null)
          return noReadAheadCache;
        return true;
      }
      set
      {
        noReadAheadCache = value;
      }
    }

    internal bool NoWriteBehindCache
    {
      get
      {
        if (cache != null)
          return noWriteBehindCache;
        return true;
      }
      set
      {
        noWriteBehindCache = value;
      }
    }

    internal bool Persistent
    {
      set
      {
        persistentFile = value;
      }
    }

    private ulong FileLength
    {
      get
      {
        return GetFileLength();
      }
      set
      {
        SetFileLength(value);
      }
    }

    private void Open(string fileName, int pageSize, bool caching)
    {
      if (storageMode.Mode == FileMode.Open && storageMode.Share == FileShare.ReadWrite && (!IsolatedStorage && File.GetAttributes(fileName) == FileAttributes.ReadOnly))
        storageMode.SetSharedReadOnly();
      if (!storageMode.CreationStatus)
      {
        if (!IsolatedStorage)
        {
          if (!storageMode.ReadOnly)
          {
            if ((File.GetAttributes(fileName) & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
              throw new VistaDBException(108, fileName);
          }
          else if (storageMode.Shared)
          {
            if ((File.GetAttributes(fileName) & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
              storageMode.SetSharedReadOnly();
          }
        }
      }
      try
      {
        if (IsolatedStorage)
        {
          fileStream = (FileStream) new IsolatedStorageFileStream(fileName, storageMode.Mode, storageMode.Access, storageMode.Share);
        }
        else
        {
          FileOptions options = FileOptions.RandomAccess;
          if (Mode.DeleteOnCloseBySystem)
            options |= FileOptions.DeleteOnClose;
          fileStream = new FileStream(fileName, storageMode.Mode, storageMode.Access, storageMode.Share, 8, options);
        }
      }
      catch (Exception)
            {
        throw;
      }
      if ((storageMode.Attributes & FileAttributes.Temporary) == FileAttributes.Temporary)
        File.SetAttributes(fileStream.Name, storageMode.Attributes);
      if (!caching)
        return;
      cache = new CacheSystem(pageSize, fileStream, syncObject);
    }

    private void Close(bool persistent)
    {
      if (fileStream == null)
        return;
      try
      {
        if (cache != null)
        {
          cache.Clear();
          cache = (CacheSystem) null;
        }
        fileStream.Close();
      }
      finally
      {
        if (storageMode.DeleteOnClose)
        {
          if (!persistent)
          {
            try
            {
              File.Delete(fileStream.Name);
            }
            catch (IOException)
                        {
            }
            catch (Exception)
                        {
            }
          }
        }
      }
    }

    private ulong GetFileLength()
    {
      if (!NoWriteBehindCache)
        return cache.FileLength;
      return (ulong) fileStream.Length;
    }

    private void SetFileLength(ulong position)
    {
      if (NoWriteBehindCache)
      {
        fileStream.SetLength((long) position);
      }
      else
      {
        lock (syncObject)
          cache.FileLength = position;
      }
    }

    private int ReadFromStream(ulong position, byte[] buffer, int offset, int len)
    {
      int num = buffer.Length - offset;
      len = len < num ? len : num;
      lock (syncObject)
      {
        fileStream.Position = (long) position;
        return fileStream.Read(buffer, offset, len);
      }
    }

    private int WriteToStream(ulong position, byte[] buffer, int offset, int len)
    {
      int num = buffer.Length - offset;
      len = len < num ? len : num;
      lock (syncObject)
      {
        if (cache != null)
          cache.RemovePage(position);
        fileStream.Position = (long) position;
        fileStream.Write(buffer, offset, len);
      }
      return len;
    }

    internal void CheckCompatibility(StorageMode requestMode, bool forceWriting)
    {
      Mode.CheckCompatibility(requestMode, forceWriting, fileStream.Name);
    }

    internal void ResetPageSize(int newPageSize)
    {
      pageSize = newPageSize;
      if (cache == null)
        return;
      cache.Clear();
      cache.ResetPageSize(newPageSize);
    }

    internal void AddRef()
    {
      ++instanceCount;
    }

    internal bool ReleaseRef()
    {
      --instanceCount;
      return instanceCount == 0;
    }

    internal ulong GetFreeCluster(int pageCount, int storagePageSize)
    {
      lock (syncObject)
      {
        ulong cluster = freeCache.GetCluster(pageCount);
        if ((long) cluster != (long) Row.EmptyReference)
          return cluster;
        ulong fileLength = FileLength;
        FileLength = fileLength + (ulong) (storagePageSize * pageCount);
        return fileLength;
      }
    }

    internal void SetFreeCluster(ulong position, int pageCount)
    {
      lock (syncObject)
      {
        freeCache.PutCluster(position, pageCount);
        if (cache == null)
          return;
        ulong pageId = position;
        for (int index = 0; index < pageCount; ++index)
        {
          cache.RemovePage(pageId);
          pageId += (ulong) pageSize;
        }
      }
    }

    internal void PendingFreeCluster(ulong storageId, ulong position, int pageCount)
    {
      lock (syncObject)
        freeCache.PutCluster(storageId, position, pageCount);
    }

    internal void WriteRow(DataStorage storage, Row row, int length)
    {
      if (NoWriteBehindCache)
        WriteToStream(row.Position, row.Buffer, 0, length);
      else
        cache.WriteRow(storage, row, length);
    }

    internal void ReadRow(DataStorage storage, Row row, int length, bool force)
    {
      if (NoReadAheadCache)
        ReadFromStream(row.Position, row.Buffer, 0, length);
      else
        cache.ReadRow(storage, row, length, force);
    }

    internal int WritePage(DataStorage storage, ulong pageId, byte[] buffer, int offset, ref int length)
    {
      int num1 = length > pageSize ? pageSize : length;
      int num2 = NoWriteBehindCache ? WriteToStream(pageId, buffer, offset, num1) : cache.WritePage(storage, pageId, buffer, offset, num1);
      length -= num2;
      offset += num2;
      return offset;
    }

    internal int ReadPage(DataStorage storage, ulong pageId, byte[] buffer, int offset, ref int length)
    {
      int num1 = length > pageSize ? pageSize : length;
      int num2 = NoReadAheadCache ? ReadFromStream(pageId, buffer, offset, num1) : cache.ReadPage(storage, pageId, buffer, offset, num1);
      length -= num2;
      offset += num2;
      return offset;
    }

    internal long DirectSeek(long offset, SeekOrigin origin)
    {
      return fileStream.Seek(offset, origin);
    }

    internal void DirectWriteBuffer(byte[] buffer, int length)
    {
      fileStream.Write(buffer, 0, length);
    }

    internal int DirectReadBuffer(byte[] buffer, int length)
    {
      return fileStream.Read(buffer, 0, length);
    }

    internal long DirectGetLength()
    {
      return fileStream.Length;
    }

    internal void Lock(ulong pos, int count, ulong storageId)
    {
      lock (syncObject)
      {
        if (lockTables == null)
          return;
        lockTables.LockPosition(storageId, pos, count);
      }
    }

    internal void Unlock(ulong pos, int count, ulong storageId)
    {
      lock (syncObject)
      {
        if (lockTables == null)
          return;
        lockTables.UnlockPosition(storageId, pos, count);
      }
    }

    internal void LockFileStream(ulong pos, int count)
    {
      if (Mode.VirtualLocks)
        return;
      lock (syncObject)
      {
        if (!fileStream.CanWrite && Mode.Share != FileShare.ReadWrite)
          throw new VistaDBException(337, Name);
        fileStream.Lock((long) pos, (long) count);
      }
    }

    internal void UnlockFileStream(ulong pos, int count)
    {
      if (Mode.VirtualLocks)
        return;
      lock (syncObject)
      {
        if (!fileStream.CanWrite && Mode.Share != FileShare.ReadWrite)
          throw new VistaDBException(337, Name);
        fileStream.Unlock((long) pos, (long) count);
      }
    }

    internal void FlushCache()
    {
      lock (syncObject)
      {
                FreeSpaceCache.Cluster[] clusterArray = (FreeSpaceCache.Cluster[]) null;
        if (freeCache != null)
          clusterArray = freeCache.CommitRelease();
        if (cache == null)
          return;
        cache.Flush();
        if (clusterArray == null || clusterArray.Length == 0)
          return;
        foreach (FreeSpaceCache.Cluster cluster in clusterArray)
        {
          if (cluster != null)
          {
            ulong position = cluster.Position;
            for (int index = 0; index < cluster.PageCount; ++index)
            {
              cache.RemovePage(position);
              position += (ulong) pageSize;
            }
          }
        }
      }
    }

    internal void ClearCache()
    {
      freeCache.Clear();
      if (cache == null)
        return;
      cache.Clear();
    }

    internal void ClearWholeCacheButHeader(ulong storageId)
    {
      if (cache == null)
        return;
      cache.ClearAssociativeButHeader(storageId);
    }

    internal void ClearWholeCache(ulong storageId, bool rollbackStorage)
    {
      lock (syncObject)
      {
        if (freeCache != null)
          freeCache.RollbackRelease(storageId);
        if (cache == null)
          return;
        cache.ClearAssociative(storageId);
      }
    }

    internal void ResetCachedLength()
    {
      if (cache == null)
        return;
      cache.Initialize();
    }

    internal void CopyFrom(StorageHandle srcHandle)
    {
      int len = 8 * srcHandle.pageSize;
      byte[] buffer = new byte[len];
      ClearCache();
      fileStream.SetLength((long) srcHandle.FileLength);
      ulong position = 0;
      ulong fileLength = srcHandle.FileLength;
      while (position < fileLength)
      {
        srcHandle.ReadFromStream(position, buffer, 0, len);
        WriteToStream(position, buffer, 0, len);
        position += (ulong) len;
      }
    }

    public void Dispose()
    {
      lock (syncObject)
      {
        if (isDisposed)
          return;
        if (lockTables != null)
          lockTables.Clear();
        Close(persistentFile);
        if (freeCache != null)
        {
          freeCache.Clear();
          freeCache = (FreeSpaceCache) null;
        }
        if (cache != null)
        {
          cache.Clear();
          cache = (CacheSystem) null;
        }
        if (fileStream != null)
        {
          fileStream.Dispose();
          fileStream = (FileStream) null;
        }
        lockTables = (StreamLocksDictionary) null;
        isDisposed = true;
        GC.SuppressFinalize((object) this);
      }
    }

    internal class StorageMode
    {
      private FileMode fileMode;
      private FileShare fileShare;
      private FileAccess fileAccess;
      private FileAttributes fileAttributes;
      private bool transacted;
      private bool virtualLocks;
      private bool deleteOnClose;
      private bool deleteOnCloseBySystem;
      private bool isolated;

      internal StorageMode(FileMode fileMode, FileShare fileShare, FileAccess fileAccess, FileAttributes attributes, bool transacted, bool deleteOnClose, bool isolated)
      {
        this.fileMode = fileMode;
        this.fileShare = fileShare;
        this.fileAccess = fileAccess;
        fileAttributes = attributes;
        this.deleteOnClose = deleteOnClose;
        this.transacted = transacted;
        virtualLocks = false;
        this.isolated = isolated;
      }

      internal StorageMode(FileMode fileMode, bool shared, bool shareReadOnly, FileAccess fileAccess, bool transacted, bool isolated)
        : this(fileMode, InitShareFlag(shared, shareReadOnly), fileAccess, FileAttributes.Normal, transacted, false, isolated)
      {
      }

      private static FileShare InitShareFlag(bool shared, bool shareReadOnly)
      {
        if (!shared)
          return FileShare.None;
        return !shareReadOnly ? FileShare.ReadWrite : FileShare.Read;
      }

      private bool ForceWriteMode()
      {
        return false;
      }

      internal FileMode Mode
      {
        get
        {
          return fileMode;
        }
      }

      internal FileShare Share
      {
        get
        {
          return fileShare;
        }
      }

      internal FileAccess Access
      {
        get
        {
          return fileAccess;
        }
      }

      internal FileAttributes Attributes
      {
        get
        {
          return fileAttributes;
        }
      }

      internal bool DeleteOnClose
      {
        get
        {
          if (deleteOnClose)
            return !deleteOnCloseBySystem;
          return false;
        }
      }

      internal bool DeleteOnCloseBySystem
      {
        set
        {
          deleteOnCloseBySystem = value;
        }
        get
        {
          return deleteOnCloseBySystem;
        }
      }

      internal bool ReadOnly
      {
        get
        {
          return fileAccess == FileAccess.Read;
        }
      }

      internal bool ReadOnlyShared
      {
        get
        {
          return fileShare == FileShare.Read;
        }
      }

      internal bool Shared
      {
        get
        {
          return fileShare != FileShare.None;
        }
      }

      internal bool Temporary
      {
        get
        {
          return (fileAttributes & FileAttributes.Temporary) == FileAttributes.Temporary;
        }
        set
        {
          fileAttributes |= FileAttributes.Temporary;
        }
      }

      internal bool Transacted
      {
        get
        {
          return transacted;
        }
      }

      internal bool IsolatedStorage
      {
        get
        {
          return isolated;
        }
      }

      internal bool VirtualLocks
      {
        get
        {
          return virtualLocks;
        }
        set
        {
          virtualLocks = value;
        }
      }

      internal bool CreationStatus
      {
        get
        {
          if (fileMode != FileMode.CreateNew && fileMode != FileMode.Create)
            return fileMode == FileMode.OpenOrCreate;
          return true;
        }
      }

      internal bool OpeningStatus
      {
        get
        {
          return fileMode == FileMode.Open;
        }
      }

      internal void CheckCompatibility(StorageMode requestMode, bool forceWriting, string fileName)
      {
        if (Access == FileAccess.Read && requestMode.Access != FileAccess.Read && !ForceWriteMode())
          throw new VistaDBException(108, fileName);
      }

      internal void SetExclusive()
      {
        fileShare = FileShare.None;
      }

      internal void SetSharedReadOnly()
      {
        fileShare = FileShare.Read;
      }

      public static bool operator ==(StorageMode m1, StorageMode m2)
      {
        if (m1.fileMode == m2.fileMode && m1.fileShare == m2.fileShare && m1.fileAccess == m2.fileAccess)
          return m1.fileAccess == m2.fileAccess;
        return false;
      }

      public static bool operator !=(StorageMode m1, StorageMode m2)
      {
        return !(m1 == m2);
      }

      public override int GetHashCode()
      {
        return base.GetHashCode();
      }

      public override bool Equals(object obj)
      {
        return this == (StorageMode) obj;
      }
    }

    internal class StreamLocksDictionary : Dictionary<ulong, StreamLocksDictionary.StreamLocks>
    {
      private StorageHandle parentHandle;

      internal StreamLocksDictionary(StorageHandle parentHandle)
      {
        this.parentHandle = parentHandle;
      }

      private new StreamLocks this[ulong storageId]
      {
        get
        {
          if (ContainsKey(storageId))
            return base[storageId];
                    StreamLocks streamLocks = new StreamLocks(parentHandle);
          Add(storageId, streamLocks);
          return streamLocks;
        }
      }

      public new void Clear()
      {
        foreach (StreamLocks streamLocks in Values)
          streamLocks.Clear();
        base.Clear();
      }

      internal void LockPosition(ulong storageId, ulong pos, int count)
      {
        this[storageId].LockPosition(pos, count);
      }

      internal void UnlockPosition(ulong storageId, ulong pos, int count)
      {
        this[storageId].UnlockPosition(pos, count);
      }

      internal class StreamLocks : List<StreamLocks.Lock>
      {
        internal object syncObject = new object();
        internal StorageHandle handle;
        internal bool virtualLocks;

        internal StreamLocks(StorageHandle handle)
        {
          this.handle = handle;
          virtualLocks = handle.Mode.VirtualLocks;
        }

        internal void LockPosition(ulong position, int count)
        {
          ulong num1 = position;
          ulong num2 = num1 + ((ulong) count - 1UL);
          lock (syncObject)
          {
            for (int index = 0; index < Count; ++index)
            {
                            Lock @lock = this[index];
              if (@lock != null)
              {
                ulong position1 = @lock.Position;
                ulong num3 = (ulong) (@lock.Space - 1) + position1;
                if ((num1 >= position1 || num2 >= position1) && (num1 <= num3 || num2 <= num3))
                {
                  if ((long) num1 != (long) position1 || (long) num2 != (long) num3)
                    throw new VistaDBException(162, position.ToString());
                  @lock.IncreaseDepth();
                  return;
                }
              }
            }
          }
                    Lock lock1 = new Lock(position, count);
          handle.LockFileStream(position, count);
          try
          {
            lock (syncObject)
              Add(lock1);
          }
          catch (Exception ex)
          {
            handle.UnlockFileStream(lock1.Position, lock1.Space);
            throw new VistaDBException(ex, 162, position.ToString());
          }
        }

        internal void UnlockPosition(ulong position, int count)
        {
          lock (syncObject)
          {
            for (int index = 0; index < Count; ++index)
            {
                            Lock @lock = this[index];
              if (@lock != null && (long) @lock.Position == (long) position && @lock.Space == count)
              {
                if (!@lock.DecreaseDepth())
                {
                  try
                  {
                    handle.UnlockFileStream(@lock.Position, @lock.Space);
                    break;
                  }
                  finally
                  {
                    RemoveAt(index);
                  }
                }
              }
            }
          }
        }

        public new void Clear()
        {
          if (!virtualLocks)
          {
            lock (syncObject)
            {
              foreach (Lock @lock in (List<Lock>) this)
                handle.UnlockFileStream(@lock.Position, @lock.Space);
            }
          }
          base.Clear();
        }

        internal class Lock
        {
          private ulong position;
          private int space;
          private int depth;

          internal Lock(ulong position, int space)
          {
            this.position = position;
            this.space = space;
            depth = 0;
          }

          internal ulong Position
          {
            get
            {
              return position;
            }
          }

          internal int Space
          {
            get
            {
              return space;
            }
          }

          internal int Depth
          {
            get
            {
              return depth;
            }
          }

          internal bool IncreaseDepth()
          {
            if (depth < 0)
              return false;
            ++depth;
            return true;
          }

          internal bool DecreaseDepth()
          {
            if (depth < 1)
              return false;
            --depth;
            return true;
          }
        }
      }
    }

    private class FreeSpaceCache
    {
      private readonly Dictionary<int, LinkedList<Cluster>> sizedClusters = new Dictionary<int, LinkedList<Cluster>>(16);
      private readonly Dictionary<ulong, LinkedList<Cluster>> pending = new Dictionary<ulong, LinkedList<Cluster>>();
      private readonly Dictionary<ulong, Cluster> freeClusters = new Dictionary<ulong, Cluster>();
      private int pendingCount;
      private int pendingCountHighWaterMark;

      internal void Clear()
      {
        sizedClusters.Clear();
        pending.Clear();
        freeClusters.Clear();
        pendingCount = 0;
        pendingCountHighWaterMark = 0;
      }

      internal void RollbackRelease(ulong storageId)
      {
        LinkedList<Cluster> linkedList;
        if (pending.Count == 0 || !pending.TryGetValue(storageId, out linkedList))
          return;
        pending.Remove(storageId);
        pendingCount -= linkedList.Count;
      }

      internal Cluster[] CommitRelease()
      {
                Cluster[] clusterArray = new Cluster[pendingCount];
        int num = 0;
        foreach (LinkedList<Cluster> linkedList1 in pending.Values)
        {
          if (linkedList1 != null)
          {
            while (linkedList1.Count > 0)
            {
              LinkedListNode<Cluster> first = linkedList1.First;
              linkedList1.Remove(first);
                            Cluster cluster = first.Value;
              if (cluster != null && cluster.PageCount > 0)
              {
                LinkedList<Cluster> linkedList2;
                if (!sizedClusters.TryGetValue(cluster.PageCount, out linkedList2))
                  sizedClusters.Add(cluster.PageCount, linkedList2 = new LinkedList<Cluster>());
                linkedList2.AddLast(first);
                freeClusters.Add(cluster.Position, cluster);
                clusterArray[num++] = cluster;
              }
            }
          }
        }
        pending.Clear();
        pendingCount = 0;
        return clusterArray;
      }

      internal ulong GetCluster(int pageCount)
      {
        LinkedList<Cluster> linkedList;
        if (sizedClusters.TryGetValue(pageCount, out linkedList) && linkedList != null)
        {
          while (linkedList.Count > 0)
          {
            LinkedListNode<Cluster> first = linkedList.First;
            linkedList.Remove(first);
                        Cluster cluster = first.Value;
            if (cluster != null && cluster.PageCount == pageCount && freeClusters.Remove(cluster.Position))
              return cluster.Position;
          }
        }
        return Row.EmptyReference;
      }

      internal void PutCluster(ulong storageId, ulong position, int pageCount)
      {
        if (freeClusters.ContainsKey(position) || pageCount == 0)
          return;
                Cluster cluster = new Cluster(position, pageCount);
        LinkedList<Cluster> linkedList;
        if (!pending.TryGetValue(storageId, out linkedList))
          pending.Add(storageId, linkedList = new LinkedList<Cluster>());
        ++pendingCount;
        linkedList.AddLast(cluster);
        if (pendingCount <= pendingCountHighWaterMark)
          return;
        pendingCountHighWaterMark = pendingCount;
      }

      internal void PutCluster(ulong position, int pageCount)
      {
        if (freeClusters.ContainsKey(position) || pageCount == 0)
          return;
                Cluster cluster = new Cluster(position, pageCount);
        LinkedList<Cluster> linkedList;
        if (!sizedClusters.TryGetValue(cluster.PageCount, out linkedList))
          sizedClusters.Add(cluster.PageCount, linkedList = new LinkedList<Cluster>());
        linkedList.AddLast(cluster);
        freeClusters.Add(position, cluster);
      }

      internal class Cluster
      {
        private ulong position;
        private int pageCount;

        internal Cluster(ulong pos, int count)
        {
          position = pos;
          pageCount = count;
        }

        internal ulong Position
        {
          get
          {
            return position;
          }
        }

        internal int PageCount
        {
          get
          {
            return pageCount;
          }
        }
      }
    }
  }
}
