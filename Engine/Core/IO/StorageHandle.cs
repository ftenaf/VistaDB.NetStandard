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
    private StorageHandle.FreeSpaceCache freeCache = new StorageHandle.FreeSpaceCache();
    internal FileStream fileStream;
    private int pageSize;
    private StorageHandle.StorageMode storageMode;
    private StorageHandle.StreamLocksDictionary lockTables;
    private int instanceCount;
    private CacheSystem cache;
    private bool noReadAheadCache;
    private bool noWriteBehindCache;
    private bool persistentFile;
    private bool isDisposed;

    internal StorageHandle(string fileName, StorageHandle.StorageMode storageMode, int pageSize, bool persistent)
    {
      this.storageMode = storageMode;
      this.pageSize = pageSize;
      this.persistentFile = persistent;
      this.Open(fileName, pageSize, !storageMode.Temporary);
      this.lockTables = new StorageHandle.StreamLocksDictionary(this);
    }

    internal object SyncObject
    {
      get
      {
        return this.syncObject;
      }
    }

    internal StorageHandle.StorageMode Mode
    {
      get
      {
        return this.storageMode;
      }
    }

    internal string Name
    {
      get
      {
        return this.fileStream.Name;
      }
    }

    internal bool Transacted
    {
      get
      {
        return this.storageMode.Transacted;
      }
    }

    internal bool IsolatedStorage
    {
      get
      {
        return this.storageMode.IsolatedStorage;
      }
    }

    internal bool NoReadAheadCache
    {
      get
      {
        if (this.cache != null)
          return this.noReadAheadCache;
        return true;
      }
      set
      {
        this.noReadAheadCache = value;
      }
    }

    internal bool NoWriteBehindCache
    {
      get
      {
        if (this.cache != null)
          return this.noWriteBehindCache;
        return true;
      }
      set
      {
        this.noWriteBehindCache = value;
      }
    }

    internal bool Persistent
    {
      set
      {
        this.persistentFile = value;
      }
    }

    private ulong FileLength
    {
      get
      {
        return this.GetFileLength();
      }
      set
      {
        this.SetFileLength(value);
      }
    }

    private void Open(string fileName, int pageSize, bool caching)
    {
      if (this.storageMode.Mode == FileMode.Open && this.storageMode.Share == FileShare.ReadWrite && (!this.IsolatedStorage && File.GetAttributes(fileName) == FileAttributes.ReadOnly))
        this.storageMode.SetSharedReadOnly();
      if (!this.storageMode.CreationStatus)
      {
        if (!this.IsolatedStorage)
        {
          if (!this.storageMode.ReadOnly)
          {
            if ((File.GetAttributes(fileName) & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
              throw new VistaDBException(108, fileName);
          }
          else if (this.storageMode.Shared)
          {
            if ((File.GetAttributes(fileName) & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
              this.storageMode.SetSharedReadOnly();
          }
        }
      }
      try
      {
        if (this.IsolatedStorage)
        {
          this.fileStream = (FileStream) new IsolatedStorageFileStream(fileName, this.storageMode.Mode, this.storageMode.Access, this.storageMode.Share);
        }
        else
        {
          FileOptions options = FileOptions.RandomAccess;
          if (this.Mode.DeleteOnCloseBySystem)
            options |= FileOptions.DeleteOnClose;
          this.fileStream = new FileStream(fileName, this.storageMode.Mode, this.storageMode.Access, this.storageMode.Share, 8, options);
        }
      }
      catch (Exception ex)
      {
        throw;
      }
      if ((this.storageMode.Attributes & FileAttributes.Temporary) == FileAttributes.Temporary)
        File.SetAttributes(this.fileStream.Name, this.storageMode.Attributes);
      if (!caching)
        return;
      this.cache = new CacheSystem(pageSize, this.fileStream, this.syncObject);
    }

    private void Close(bool persistent)
    {
      if (this.fileStream == null)
        return;
      try
      {
        if (this.cache != null)
        {
          this.cache.Clear();
          this.cache = (CacheSystem) null;
        }
        this.fileStream.Close();
      }
      finally
      {
        if (this.storageMode.DeleteOnClose)
        {
          if (!persistent)
          {
            try
            {
              File.Delete(this.fileStream.Name);
            }
            catch (IOException ex)
            {
            }
            catch (Exception ex)
            {
            }
          }
        }
      }
    }

    private ulong GetFileLength()
    {
      if (!this.NoWriteBehindCache)
        return this.cache.FileLength;
      return (ulong) this.fileStream.Length;
    }

    private void SetFileLength(ulong position)
    {
      if (this.NoWriteBehindCache)
      {
        this.fileStream.SetLength((long) position);
      }
      else
      {
        lock (this.syncObject)
          this.cache.FileLength = position;
      }
    }

    private int ReadFromStream(ulong position, byte[] buffer, int offset, int len)
    {
      int num = buffer.Length - offset;
      len = len < num ? len : num;
      lock (this.syncObject)
      {
        this.fileStream.Position = (long) position;
        return this.fileStream.Read(buffer, offset, len);
      }
    }

    private int WriteToStream(ulong position, byte[] buffer, int offset, int len)
    {
      int num = buffer.Length - offset;
      len = len < num ? len : num;
      lock (this.syncObject)
      {
        if (this.cache != null)
          this.cache.RemovePage(position);
        this.fileStream.Position = (long) position;
        this.fileStream.Write(buffer, offset, len);
      }
      return len;
    }

    internal void CheckCompatibility(StorageHandle.StorageMode requestMode, bool forceWriting)
    {
      this.Mode.CheckCompatibility(requestMode, forceWriting, this.fileStream.Name);
    }

    internal void ResetPageSize(int newPageSize)
    {
      this.pageSize = newPageSize;
      if (this.cache == null)
        return;
      this.cache.Clear();
      this.cache.ResetPageSize(newPageSize);
    }

    internal void AddRef()
    {
      ++this.instanceCount;
    }

    internal bool ReleaseRef()
    {
      --this.instanceCount;
      return this.instanceCount == 0;
    }

    internal ulong GetFreeCluster(int pageCount, int storagePageSize)
    {
      lock (this.syncObject)
      {
        ulong cluster = this.freeCache.GetCluster(pageCount);
        if ((long) cluster != (long) Row.EmptyReference)
          return cluster;
        ulong fileLength = this.FileLength;
        this.FileLength = fileLength + (ulong) (storagePageSize * pageCount);
        return fileLength;
      }
    }

    internal void SetFreeCluster(ulong position, int pageCount)
    {
      lock (this.syncObject)
      {
        this.freeCache.PutCluster(position, pageCount);
        if (this.cache == null)
          return;
        ulong pageId = position;
        for (int index = 0; index < pageCount; ++index)
        {
          this.cache.RemovePage(pageId);
          pageId += (ulong) this.pageSize;
        }
      }
    }

    internal void PendingFreeCluster(ulong storageId, ulong position, int pageCount)
    {
      lock (this.syncObject)
        this.freeCache.PutCluster(storageId, position, pageCount);
    }

    internal void WriteRow(DataStorage storage, Row row, int length)
    {
      if (this.NoWriteBehindCache)
        this.WriteToStream(row.Position, row.Buffer, 0, length);
      else
        this.cache.WriteRow(storage, row, length);
    }

    internal void ReadRow(DataStorage storage, Row row, int length, bool force)
    {
      if (this.NoReadAheadCache)
        this.ReadFromStream(row.Position, row.Buffer, 0, length);
      else
        this.cache.ReadRow(storage, row, length, force);
    }

    internal int WritePage(DataStorage storage, ulong pageId, byte[] buffer, int offset, ref int length)
    {
      int num1 = length > this.pageSize ? this.pageSize : length;
      int num2 = this.NoWriteBehindCache ? this.WriteToStream(pageId, buffer, offset, num1) : this.cache.WritePage(storage, pageId, buffer, offset, num1);
      length -= num2;
      offset += num2;
      return offset;
    }

    internal int ReadPage(DataStorage storage, ulong pageId, byte[] buffer, int offset, ref int length)
    {
      int num1 = length > this.pageSize ? this.pageSize : length;
      int num2 = this.NoReadAheadCache ? this.ReadFromStream(pageId, buffer, offset, num1) : this.cache.ReadPage(storage, pageId, buffer, offset, num1);
      length -= num2;
      offset += num2;
      return offset;
    }

    internal long DirectSeek(long offset, SeekOrigin origin)
    {
      return this.fileStream.Seek(offset, origin);
    }

    internal void DirectWriteBuffer(byte[] buffer, int length)
    {
      this.fileStream.Write(buffer, 0, length);
    }

    internal int DirectReadBuffer(byte[] buffer, int length)
    {
      return this.fileStream.Read(buffer, 0, length);
    }

    internal long DirectGetLength()
    {
      return this.fileStream.Length;
    }

    internal void Lock(ulong pos, int count, ulong storageId)
    {
      lock (this.syncObject)
      {
        if (this.lockTables == null)
          return;
        this.lockTables.LockPosition(storageId, pos, count);
      }
    }

    internal void Unlock(ulong pos, int count, ulong storageId)
    {
      lock (this.syncObject)
      {
        if (this.lockTables == null)
          return;
        this.lockTables.UnlockPosition(storageId, pos, count);
      }
    }

    internal void LockFileStream(ulong pos, int count)
    {
      if (this.Mode.VirtualLocks)
        return;
      lock (this.syncObject)
      {
        if (!this.fileStream.CanWrite && this.Mode.Share != FileShare.ReadWrite)
          throw new VistaDBException(337, this.Name);
        this.fileStream.Lock((long) pos, (long) count);
      }
    }

    internal void UnlockFileStream(ulong pos, int count)
    {
      if (this.Mode.VirtualLocks)
        return;
      lock (this.syncObject)
      {
        if (!this.fileStream.CanWrite && this.Mode.Share != FileShare.ReadWrite)
          throw new VistaDBException(337, this.Name);
        this.fileStream.Unlock((long) pos, (long) count);
      }
    }

    internal void FlushCache()
    {
      lock (this.syncObject)
      {
        StorageHandle.FreeSpaceCache.Cluster[] clusterArray = (StorageHandle.FreeSpaceCache.Cluster[]) null;
        if (this.freeCache != null)
          clusterArray = this.freeCache.CommitRelease();
        if (this.cache == null)
          return;
        this.cache.Flush();
        if (clusterArray == null || clusterArray.Length == 0)
          return;
        foreach (StorageHandle.FreeSpaceCache.Cluster cluster in clusterArray)
        {
          if (cluster != null)
          {
            ulong position = cluster.Position;
            for (int index = 0; index < cluster.PageCount; ++index)
            {
              this.cache.RemovePage(position);
              position += (ulong) this.pageSize;
            }
          }
        }
      }
    }

    internal void ClearCache()
    {
      this.freeCache.Clear();
      if (this.cache == null)
        return;
      this.cache.Clear();
    }

    internal void ClearWholeCacheButHeader(ulong storageId)
    {
      if (this.cache == null)
        return;
      this.cache.ClearAssociativeButHeader(storageId);
    }

    internal void ClearWholeCache(ulong storageId, bool rollbackStorage)
    {
      lock (this.syncObject)
      {
        if (this.freeCache != null)
          this.freeCache.RollbackRelease(storageId);
        if (this.cache == null)
          return;
        this.cache.ClearAssociative(storageId);
      }
    }

    internal void ResetCachedLength()
    {
      if (this.cache == null)
        return;
      this.cache.Initialize();
    }

    internal void CopyFrom(StorageHandle srcHandle)
    {
      int len = 8 * srcHandle.pageSize;
      byte[] buffer = new byte[len];
      this.ClearCache();
      this.fileStream.SetLength((long) srcHandle.FileLength);
      ulong position = 0;
      ulong fileLength = srcHandle.FileLength;
      while (position < fileLength)
      {
        srcHandle.ReadFromStream(position, buffer, 0, len);
        this.WriteToStream(position, buffer, 0, len);
        position += (ulong) len;
      }
    }

    public void Dispose()
    {
      lock (this.syncObject)
      {
        if (this.isDisposed)
          return;
        if (this.lockTables != null)
          this.lockTables.Clear();
        this.Close(this.persistentFile);
        if (this.freeCache != null)
        {
          this.freeCache.Clear();
          this.freeCache = (StorageHandle.FreeSpaceCache) null;
        }
        if (this.cache != null)
        {
          this.cache.Clear();
          this.cache = (CacheSystem) null;
        }
        if (this.fileStream != null)
        {
          this.fileStream.Dispose();
          this.fileStream = (FileStream) null;
        }
        this.lockTables = (StorageHandle.StreamLocksDictionary) null;
        this.isDisposed = true;
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
        this.fileAttributes = attributes;
        this.deleteOnClose = deleteOnClose;
        this.transacted = transacted;
        this.virtualLocks = false;
        this.isolated = isolated;
      }

      internal StorageMode(FileMode fileMode, bool shared, bool shareReadOnly, FileAccess fileAccess, bool transacted, bool isolated)
        : this(fileMode, StorageHandle.StorageMode.InitShareFlag(shared, shareReadOnly), fileAccess, FileAttributes.Normal, transacted, false, isolated)
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
          return this.fileMode;
        }
      }

      internal FileShare Share
      {
        get
        {
          return this.fileShare;
        }
      }

      internal FileAccess Access
      {
        get
        {
          return this.fileAccess;
        }
      }

      internal FileAttributes Attributes
      {
        get
        {
          return this.fileAttributes;
        }
      }

      internal bool DeleteOnClose
      {
        get
        {
          if (this.deleteOnClose)
            return !this.deleteOnCloseBySystem;
          return false;
        }
      }

      internal bool DeleteOnCloseBySystem
      {
        set
        {
          this.deleteOnCloseBySystem = value;
        }
        get
        {
          return this.deleteOnCloseBySystem;
        }
      }

      internal bool ReadOnly
      {
        get
        {
          return this.fileAccess == FileAccess.Read;
        }
      }

      internal bool ReadOnlyShared
      {
        get
        {
          return this.fileShare == FileShare.Read;
        }
      }

      internal bool Shared
      {
        get
        {
          return this.fileShare != FileShare.None;
        }
      }

      internal bool Temporary
      {
        get
        {
          return (this.fileAttributes & FileAttributes.Temporary) == FileAttributes.Temporary;
        }
        set
        {
          this.fileAttributes |= FileAttributes.Temporary;
        }
      }

      internal bool Transacted
      {
        get
        {
          return this.transacted;
        }
      }

      internal bool IsolatedStorage
      {
        get
        {
          return this.isolated;
        }
      }

      internal bool VirtualLocks
      {
        get
        {
          return this.virtualLocks;
        }
        set
        {
          this.virtualLocks = value;
        }
      }

      internal bool CreationStatus
      {
        get
        {
          if (this.fileMode != FileMode.CreateNew && this.fileMode != FileMode.Create)
            return this.fileMode == FileMode.OpenOrCreate;
          return true;
        }
      }

      internal bool OpeningStatus
      {
        get
        {
          return this.fileMode == FileMode.Open;
        }
      }

      internal void CheckCompatibility(StorageHandle.StorageMode requestMode, bool forceWriting, string fileName)
      {
        if (this.Access == FileAccess.Read && requestMode.Access != FileAccess.Read && !this.ForceWriteMode())
          throw new VistaDBException(108, fileName);
      }

      internal void SetExclusive()
      {
        this.fileShare = FileShare.None;
      }

      internal void SetSharedReadOnly()
      {
        this.fileShare = FileShare.Read;
      }

      public static bool operator ==(StorageHandle.StorageMode m1, StorageHandle.StorageMode m2)
      {
        if (m1.fileMode == m2.fileMode && m1.fileShare == m2.fileShare && m1.fileAccess == m2.fileAccess)
          return m1.fileAccess == m2.fileAccess;
        return false;
      }

      public static bool operator !=(StorageHandle.StorageMode m1, StorageHandle.StorageMode m2)
      {
        return !(m1 == m2);
      }

      public override int GetHashCode()
      {
        return base.GetHashCode();
      }

      public override bool Equals(object obj)
      {
        return this == (StorageHandle.StorageMode) obj;
      }
    }

    internal class StreamLocksDictionary : Dictionary<ulong, StorageHandle.StreamLocksDictionary.StreamLocks>
    {
      private StorageHandle parentHandle;

      internal StreamLocksDictionary(StorageHandle parentHandle)
      {
        this.parentHandle = parentHandle;
      }

      private new StorageHandle.StreamLocksDictionary.StreamLocks this[ulong storageId]
      {
        get
        {
          if (this.ContainsKey(storageId))
            return base[storageId];
          StorageHandle.StreamLocksDictionary.StreamLocks streamLocks = new StorageHandle.StreamLocksDictionary.StreamLocks(this.parentHandle);
          this.Add(storageId, streamLocks);
          return streamLocks;
        }
      }

      public new void Clear()
      {
        foreach (StorageHandle.StreamLocksDictionary.StreamLocks streamLocks in this.Values)
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

      internal class StreamLocks : List<StorageHandle.StreamLocksDictionary.StreamLocks.Lock>
      {
        internal object syncObject = new object();
        internal StorageHandle handle;
        internal bool virtualLocks;

        internal StreamLocks(StorageHandle handle)
        {
          this.handle = handle;
          this.virtualLocks = handle.Mode.VirtualLocks;
        }

        internal void LockPosition(ulong position, int count)
        {
          ulong num1 = position;
          ulong num2 = num1 + ((ulong) count - 1UL);
          lock (this.syncObject)
          {
            for (int index = 0; index < this.Count; ++index)
            {
              StorageHandle.StreamLocksDictionary.StreamLocks.Lock @lock = this[index];
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
          StorageHandle.StreamLocksDictionary.StreamLocks.Lock lock1 = new StorageHandle.StreamLocksDictionary.StreamLocks.Lock(position, count);
          this.handle.LockFileStream(position, count);
          try
          {
            lock (this.syncObject)
              this.Add(lock1);
          }
          catch (Exception ex)
          {
            this.handle.UnlockFileStream(lock1.Position, lock1.Space);
            throw new VistaDBException(ex, 162, position.ToString());
          }
        }

        internal void UnlockPosition(ulong position, int count)
        {
          lock (this.syncObject)
          {
            for (int index = 0; index < this.Count; ++index)
            {
              StorageHandle.StreamLocksDictionary.StreamLocks.Lock @lock = this[index];
              if (@lock != null && (long) @lock.Position == (long) position && @lock.Space == count)
              {
                if (!@lock.DecreaseDepth())
                {
                  try
                  {
                    this.handle.UnlockFileStream(@lock.Position, @lock.Space);
                    break;
                  }
                  finally
                  {
                    this.RemoveAt(index);
                  }
                }
              }
            }
          }
        }

        public new void Clear()
        {
          if (!this.virtualLocks)
          {
            lock (this.syncObject)
            {
              foreach (StorageHandle.StreamLocksDictionary.StreamLocks.Lock @lock in (List<StorageHandle.StreamLocksDictionary.StreamLocks.Lock>) this)
                this.handle.UnlockFileStream(@lock.Position, @lock.Space);
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
            this.depth = 0;
          }

          internal ulong Position
          {
            get
            {
              return this.position;
            }
          }

          internal int Space
          {
            get
            {
              return this.space;
            }
          }

          internal int Depth
          {
            get
            {
              return this.depth;
            }
          }

          internal bool IncreaseDepth()
          {
            if (this.depth < 0)
              return false;
            ++this.depth;
            return true;
          }

          internal bool DecreaseDepth()
          {
            if (this.depth < 1)
              return false;
            --this.depth;
            return true;
          }
        }
      }
    }

    private class FreeSpaceCache
    {
      private readonly Dictionary<int, LinkedList<StorageHandle.FreeSpaceCache.Cluster>> sizedClusters = new Dictionary<int, LinkedList<StorageHandle.FreeSpaceCache.Cluster>>(16);
      private readonly Dictionary<ulong, LinkedList<StorageHandle.FreeSpaceCache.Cluster>> pending = new Dictionary<ulong, LinkedList<StorageHandle.FreeSpaceCache.Cluster>>();
      private readonly Dictionary<ulong, StorageHandle.FreeSpaceCache.Cluster> freeClusters = new Dictionary<ulong, StorageHandle.FreeSpaceCache.Cluster>();
      private int pendingCount;
      private int pendingCountHighWaterMark;

      internal void Clear()
      {
        this.sizedClusters.Clear();
        this.pending.Clear();
        this.freeClusters.Clear();
        this.pendingCount = 0;
        this.pendingCountHighWaterMark = 0;
      }

      internal void RollbackRelease(ulong storageId)
      {
        LinkedList<StorageHandle.FreeSpaceCache.Cluster> linkedList;
        if (this.pending.Count == 0 || !this.pending.TryGetValue(storageId, out linkedList))
          return;
        this.pending.Remove(storageId);
        this.pendingCount -= linkedList.Count;
      }

      internal StorageHandle.FreeSpaceCache.Cluster[] CommitRelease()
      {
        StorageHandle.FreeSpaceCache.Cluster[] clusterArray = new StorageHandle.FreeSpaceCache.Cluster[this.pendingCount];
        int num = 0;
        foreach (LinkedList<StorageHandle.FreeSpaceCache.Cluster> linkedList1 in this.pending.Values)
        {
          if (linkedList1 != null)
          {
            while (linkedList1.Count > 0)
            {
              LinkedListNode<StorageHandle.FreeSpaceCache.Cluster> first = linkedList1.First;
              linkedList1.Remove(first);
              StorageHandle.FreeSpaceCache.Cluster cluster = first.Value;
              if (cluster != null && cluster.PageCount > 0)
              {
                LinkedList<StorageHandle.FreeSpaceCache.Cluster> linkedList2;
                if (!this.sizedClusters.TryGetValue(cluster.PageCount, out linkedList2))
                  this.sizedClusters.Add(cluster.PageCount, linkedList2 = new LinkedList<StorageHandle.FreeSpaceCache.Cluster>());
                linkedList2.AddLast(first);
                this.freeClusters.Add(cluster.Position, cluster);
                clusterArray[num++] = cluster;
              }
            }
          }
        }
        this.pending.Clear();
        this.pendingCount = 0;
        return clusterArray;
      }

      internal ulong GetCluster(int pageCount)
      {
        LinkedList<StorageHandle.FreeSpaceCache.Cluster> linkedList;
        if (this.sizedClusters.TryGetValue(pageCount, out linkedList) && linkedList != null)
        {
          while (linkedList.Count > 0)
          {
            LinkedListNode<StorageHandle.FreeSpaceCache.Cluster> first = linkedList.First;
            linkedList.Remove(first);
            StorageHandle.FreeSpaceCache.Cluster cluster = first.Value;
            if (cluster != null && cluster.PageCount == pageCount && this.freeClusters.Remove(cluster.Position))
              return cluster.Position;
          }
        }
        return Row.EmptyReference;
      }

      internal void PutCluster(ulong storageId, ulong position, int pageCount)
      {
        if (this.freeClusters.ContainsKey(position) || pageCount == 0)
          return;
        StorageHandle.FreeSpaceCache.Cluster cluster = new StorageHandle.FreeSpaceCache.Cluster(position, pageCount);
        LinkedList<StorageHandle.FreeSpaceCache.Cluster> linkedList;
        if (!this.pending.TryGetValue(storageId, out linkedList))
          this.pending.Add(storageId, linkedList = new LinkedList<StorageHandle.FreeSpaceCache.Cluster>());
        ++this.pendingCount;
        linkedList.AddLast(cluster);
        if (this.pendingCount <= this.pendingCountHighWaterMark)
          return;
        this.pendingCountHighWaterMark = this.pendingCount;
      }

      internal void PutCluster(ulong position, int pageCount)
      {
        if (this.freeClusters.ContainsKey(position) || pageCount == 0)
          return;
        StorageHandle.FreeSpaceCache.Cluster cluster = new StorageHandle.FreeSpaceCache.Cluster(position, pageCount);
        LinkedList<StorageHandle.FreeSpaceCache.Cluster> linkedList;
        if (!this.sizedClusters.TryGetValue(cluster.PageCount, out linkedList))
          this.sizedClusters.Add(cluster.PageCount, linkedList = new LinkedList<StorageHandle.FreeSpaceCache.Cluster>());
        linkedList.AddLast(cluster);
        this.freeClusters.Add(position, cluster);
      }

      internal class Cluster
      {
        private ulong position;
        private int pageCount;

        internal Cluster(ulong pos, int count)
        {
          this.position = pos;
          this.pageCount = count;
        }

        internal ulong Position
        {
          get
          {
            return this.position;
          }
        }

        internal int PageCount
        {
          get
          {
            return this.pageCount;
          }
        }
      }
    }
  }
}
