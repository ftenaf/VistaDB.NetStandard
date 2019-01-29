using System;
using System.Collections.Generic;
using System.IO;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.IO
{
  internal class CacheSystem : Dictionary<ulong, CacheSystem.Page>
  {
    private long fileLength = -1;
    private readonly CacheSystem.PageCache pageCache = new CacheSystem.PageCache();
    private readonly object syncObject = new object();
    private int pageSize;
    private FileStream fileStream;

    internal CacheSystem(int pageSize, FileStream fileStream, object syncObject)
    {
      this.pageSize = pageSize;
      this.fileStream = fileStream;
      long fileLength = (long) this.FileLength;
      this.syncObject = syncObject;
    }

    internal ulong FileLength
    {
      get
      {
        if (this.fileLength == -1L)
          this.fileLength = this.fileStream.Length;
        return (ulong) this.fileLength;
      }
      set
      {
        this.fileLength = (long) value;
      }
    }

    internal void Initialize()
    {
      this.fileLength = -1L;
    }

    private CacheSystem.Page CreateNewPage(ulong storageId, ulong pageId)
    {
      CacheSystem.Page page = new CacheSystem.Page(storageId, pageId, this.pageSize, this);
      this.pageCache[pageId] = page;
      if ((long) storageId == (long) pageId)
        this.Add(pageId, page);
      return page;
    }

    private CacheSystem.Page OpenNewPage(ulong storageId, ulong pageId, bool forceRead)
    {
      CacheSystem.Page newValue = new CacheSystem.Page(storageId, pageId, this.pageSize, this);
      this.pageCache.AddToWeakCache(pageId, newValue);
      if (forceRead)
        newValue.Refresh(this.fileStream, true);
      this.Add(pageId, newValue);
      return newValue;
    }

    private CacheSystem.Page FindPage(ulong pageId)
    {
      CacheSystem.Page page;
      if (!this.TryGetValue(pageId, out page))
        page = this.pageCache[pageId];
      return page;
    }

    private int OnHandleDataIO(ulong storageId, byte[] data, int offset, ulong filePosition, int length, bool toRead, bool forceRead)
    {
      ulong num1 = filePosition;
      ulong num2 = filePosition + (ulong) length - 1UL;
      int length1 = length;
      int offset1 = offset;
      ulong num3 = num1 - num1 % (ulong) this.pageSize;
      ulong num4 = num2 - num2 % (ulong) this.pageSize;
      lock (this.SyncRoot)
      {
        ulong pageId = num3;
        while (pageId <= num4)
        {
          if (length1 > 0)
          {
            bool flag1 = filePosition > pageId || filePosition + (ulong) length1 < pageId + (ulong) this.pageSize;
            CacheSystem.Page page = this.FindPage(pageId);
            bool flag2 = page != null;
            if (!flag2)
            {
              page = toRead ? this.CreateNewPage(storageId, pageId) : this.OpenNewPage(storageId, pageId, forceRead && flag1);
              forceRead = forceRead || toRead;
            }
            else
              forceRead = forceRead && toRead;
            try
            {
              if (toRead)
              {
                if (forceRead && !page.IsDirty)
                  page.Refresh(this.fileStream, toRead, flag2 ? length1 : this.pageSize);
                length1 -= page.ReadFromCache(data, ref offset1, ref filePosition, length1);
              }
              else
              {
                length1 -= page.WriteToCache(data, ref offset1, ref filePosition, length1);
                lock (this.syncObject)
                {
                  if (this.FileLength <= pageId)
                    this.FileLength = pageId + (ulong) this.pageSize;
                }
              }
            }
            catch (Exception ex)
            {
              this.RemovePage(pageId);
              throw;
            }
            pageId += (ulong) this.pageSize;
          }
          else
            break;
        }
      }
      return length;
    }

    internal void DirtyPage(CacheSystem.Page page)
    {
      lock (this.SyncRoot)
      {
        if (this.ContainsKey(page.PageId))
          return;
        this.Add(page.PageId, page);
      }
    }

    internal void FreshPage(CacheSystem.Page page)
    {
      if ((long) page.PageId == (long) page.StorageId)
        return;
      lock (this.SyncRoot)
      {
        if (!this.ContainsKey(page.PageId))
          return;
        this.Remove(page.PageId);
        CacheSystem.Page page1 = this.pageCache[page.PageId];
      }
    }

    internal bool RemovePage(ulong pageId)
    {
      lock (this.SyncRoot)
      {
        bool flag = this.Remove(pageId);
        return this.pageCache.Remove(pageId) || flag;
      }
    }

    internal void Flush()
    {
      lock (this.SyncRoot)
      {
        foreach (CacheSystem.Page page in new List<CacheSystem.Page>((IEnumerable<CacheSystem.Page>) this.Values))
        {
          if (page.IsDirty)
            page.Refresh(this.fileStream, false);
          else if ((long) page.PageId != (long) page.StorageId)
            this.Remove(page.PageId);
        }
        this.Initialize();
      }
    }

    internal void ClearAssociativeButHeader(ulong storageId)
    {
    }

    internal void ClearAssociative(ulong storageId)
    {
      lock (this.SyncRoot)
      {
        foreach (CacheSystem.Page page in this.pageCache.GetAllByStorage(storageId))
          this.RemovePage(page.PageId);
      }
    }

    internal void ResetPageSize(int newPageSize)
    {
      this.pageSize = newPageSize;
    }

    internal int ReadPage(DataStorage storage, ulong pageId, byte[] buffer, int offset, int length)
    {
      return this.OnHandleDataIO(storage.StorageId, buffer, offset, pageId, length, true, false);
    }

    internal int WritePage(DataStorage storage, ulong pageId, byte[] buffer, int offset, int length)
    {
      return this.OnHandleDataIO(storage.StorageId, buffer, offset, pageId, length, false, false);
    }

    internal bool ReadRow(DataStorage storage, Row row, int maxLength, bool force)
    {
      this.OnHandleDataIO(storage.StorageId, row.Buffer, 0, row.Position, maxLength, true, force);
      return true;
    }

    internal bool WriteRow(DataStorage storage, Row row, int maxLength)
    {
      this.OnHandleDataIO(storage.StorageId, row.Buffer, 0, row.Position, maxLength, false, true);
      return true;
    }

    public object SyncRoot
    {
      get
      {
        return this.syncObject;
      }
    }

    public new void Clear()
    {
      base.Clear();
      this.pageCache.Clear();
      this.Initialize();
    }

    internal class Page
    {
      private ulong pageId;
      private ulong storageId;
      private bool isDirty;
      private byte[] buffer;
      private readonly CacheSystem cache;

      internal Page(ulong storageId, ulong pageId, int pageSize, CacheSystem cacheSystem)
      {
        this.storageId = storageId;
        this.pageId = pageId;
        this.buffer = new byte[pageSize];
        this.cache = cacheSystem;
      }

      internal bool IsDirty
      {
        get
        {
          return this.isDirty;
        }
      }

      internal ulong PageId
      {
        get
        {
          return this.pageId;
        }
      }

      internal ulong StorageId
      {
        get
        {
          return this.storageId;
        }
      }

      internal int ReadFromCache(byte[] dataBuffer, ref int offset, ref ulong filePosition, int length)
      {
        int sourceIndex = (int) ((long) filePosition - (long) this.pageId);
        int num = this.buffer.Length - sourceIndex;
        int length1 = length <= num ? length : num;
        Array.Copy((Array) this.buffer, sourceIndex, (Array) dataBuffer, offset, length1);
        offset += length1;
        filePosition += (ulong) length1;
        return length1;
      }

      internal int WriteToCache(byte[] dataBuffer, ref int offset, ref ulong filePosition, int length)
      {
        int destinationIndex = (int) ((long) filePosition - (long) this.pageId);
        int num = this.buffer.Length - destinationIndex;
        int length1 = length <= num ? length : num;
        Array.Copy((Array) dataBuffer, offset, (Array) this.buffer, destinationIndex, length1);
        if (this.cache != null && !this.isDirty)
          this.cache.DirtyPage(this);
        this.isDirty = true;
        offset += length1;
        filePosition += (ulong) length1;
        return length1;
      }

      internal void Refresh(FileStream fileStream, bool toRead)
      {
        this.Refresh(fileStream, toRead, this.buffer.Length);
      }

      internal void Refresh(FileStream fileStream, bool toRead, int len)
      {
        len = len < this.buffer.Length ? len : this.buffer.Length;
        try
        {
          fileStream.Seek((long) this.pageId, SeekOrigin.Begin);
          if (toRead)
          {
            if (!this.IsNewPageVersion())
              return;
            fileStream.Read(this.buffer, 0, len);
          }
          else
            fileStream.Write(this.buffer, 0, len);
        }
        finally
        {
          this.isDirty = false;
          if (this.cache != null && (long) this.pageId != (long) this.storageId)
            this.cache.FreshPage(this);
        }
      }

      private bool IsNewPageVersion()
      {
        return true;
      }
    }

    internal class PageCache : WeakReferenceCache<ulong, CacheSystem.Page>
    {
      internal PageCache()
        : base(50)
      {
      }

      internal IEnumerator<CacheSystem.Page> EnumerateByStorage(ulong storageId)
      {
        IEnumerator<CacheSystem.Page> enumerator = this.EnumerateEntireCache();
        while (enumerator.MoveNext())
        {
          CacheSystem.Page page = enumerator.Current;
          if (page != null && (long) page.StorageId == (long) storageId)
            yield return page;
        }
      }

      internal List<CacheSystem.Page> GetAllByStorage(ulong storageId)
      {
        List<CacheSystem.Page> pageList = new List<CacheSystem.Page>();
        IEnumerator<CacheSystem.Page> enumerator = this.EnumerateByStorage(storageId);
        while (enumerator.MoveNext())
        {
          CacheSystem.Page current = enumerator.Current;
          if (current != null)
            pageList.Add(current);
        }
        return pageList;
      }
    }
  }
}
