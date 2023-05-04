using System;
using System.Collections.Generic;
using System.IO;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.IO
{
  internal class CacheSystem : Dictionary<ulong, CacheSystem.Page>
  {
    private long fileLength = -1;
    private readonly PageCache pageCache = new PageCache();
    private readonly object syncObject = new object();
    private int pageSize;
    private FileStream fileStream;

    internal CacheSystem(int pageSize, FileStream fileStream, object syncObject)
    {
      this.pageSize = pageSize;
      this.fileStream = fileStream;
      long fileLength = (long) FileLength;
      this.syncObject = syncObject;
    }

    internal ulong FileLength
    {
      get
      {
        if (fileLength == -1L)
          fileLength = fileStream.Length;
        return (ulong) fileLength;
      }
      set
      {
        fileLength = (long) value;
      }
    }

    internal void Initialize()
    {
      fileLength = -1L;
    }

    private Page CreateNewPage(ulong storageId, ulong pageId)
    {
            Page page = new Page(storageId, pageId, pageSize, this);
      pageCache[pageId] = page;
      if ((long) storageId == (long) pageId)
        Add(pageId, page);
      return page;
    }

    private Page OpenNewPage(ulong storageId, ulong pageId, bool forceRead)
    {
            Page newValue = new Page(storageId, pageId, pageSize, this);
      pageCache.AddToWeakCache(pageId, newValue);
      if (forceRead)
        newValue.Refresh(fileStream, true);
      Add(pageId, newValue);
      return newValue;
    }

    private Page FindPage(ulong pageId)
    {
            Page page;
      if (!TryGetValue(pageId, out page))
        page = pageCache[pageId];
      return page;
    }

    private int OnHandleDataIO(ulong storageId, byte[] data, int offset, ulong filePosition, int length, bool toRead, bool forceRead)
    {
      ulong num1 = filePosition;
      ulong num2 = filePosition + (ulong) length - 1UL;
      int length1 = length;
      int offset1 = offset;
      ulong num3 = num1 - num1 % (ulong) pageSize;
      ulong num4 = num2 - num2 % (ulong) pageSize;
      lock (SyncRoot)
      {
        ulong pageId = num3;
        while (pageId <= num4)
        {
          if (length1 > 0)
          {
            bool flag1 = filePosition > pageId || filePosition + (ulong) length1 < pageId + (ulong) pageSize;
                        Page page = FindPage(pageId);
            bool flag2 = page != null;
            if (!flag2)
            {
              page = toRead ? CreateNewPage(storageId, pageId) : OpenNewPage(storageId, pageId, forceRead && flag1);
              forceRead = forceRead || toRead;
            }
            else
              forceRead = forceRead && toRead;
            try
            {
              if (toRead)
              {
                if (forceRead && !page.IsDirty)
                  page.Refresh(fileStream, toRead, flag2 ? length1 : pageSize);
                length1 -= page.ReadFromCache(data, ref offset1, ref filePosition, length1);
              }
              else
              {
                length1 -= page.WriteToCache(data, ref offset1, ref filePosition, length1);
                lock (syncObject)
                {
                  if (FileLength <= pageId)
                    FileLength = pageId + (ulong) pageSize;
                }
              }
            }
            catch (Exception)
                        {
              RemovePage(pageId);
              throw;
            }
            pageId += (ulong) pageSize;
          }
          else
            break;
        }
      }
      return length;
    }

    internal void DirtyPage(Page page)
    {
      lock (SyncRoot)
      {
        if (ContainsKey(page.PageId))
          return;
        Add(page.PageId, page);
      }
    }

    internal void FreshPage(Page page)
    {
      if ((long) page.PageId == (long) page.StorageId)
        return;
      lock (SyncRoot)
      {
        if (!ContainsKey(page.PageId))
          return;
        Remove(page.PageId);
                Page page1 = pageCache[page.PageId];
      }
    }

    internal bool RemovePage(ulong pageId)
    {
      lock (SyncRoot)
      {
        bool flag = Remove(pageId);
        return pageCache.Remove(pageId) || flag;
      }
    }

    internal void Flush()
    {
      lock (SyncRoot)
      {
        foreach (Page page in new List<Page>((IEnumerable<Page>) Values))
        {
          if (page.IsDirty)
            page.Refresh(fileStream, false);
          else if ((long) page.PageId != (long) page.StorageId)
            Remove(page.PageId);
        }
        Initialize();
      }
    }

    internal void ClearAssociativeButHeader(ulong storageId)
    {
    }

    internal void ClearAssociative(ulong storageId)
    {
      lock (SyncRoot)
      {
        foreach (Page page in pageCache.GetAllByStorage(storageId))
          RemovePage(page.PageId);
      }
    }

    internal void ResetPageSize(int newPageSize)
    {
      pageSize = newPageSize;
    }

    internal int ReadPage(DataStorage storage, ulong pageId, byte[] buffer, int offset, int length)
    {
      return OnHandleDataIO(storage.StorageId, buffer, offset, pageId, length, true, false);
    }

    internal int WritePage(DataStorage storage, ulong pageId, byte[] buffer, int offset, int length)
    {
      return OnHandleDataIO(storage.StorageId, buffer, offset, pageId, length, false, false);
    }

    internal bool ReadRow(DataStorage storage, Row row, int maxLength, bool force)
    {
      OnHandleDataIO(storage.StorageId, row.Buffer, 0, row.Position, maxLength, true, force);
      return true;
    }

    internal bool WriteRow(DataStorage storage, Row row, int maxLength)
    {
      OnHandleDataIO(storage.StorageId, row.Buffer, 0, row.Position, maxLength, false, true);
      return true;
    }

    public object SyncRoot
    {
      get
      {
        return syncObject;
      }
    }

    public new void Clear()
    {
      base.Clear();
      pageCache.Clear();
      Initialize();
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
        buffer = new byte[pageSize];
        cache = cacheSystem;
      }

      internal bool IsDirty
      {
        get
        {
          return isDirty;
        }
      }

      internal ulong PageId
      {
        get
        {
          return pageId;
        }
      }

      internal ulong StorageId
      {
        get
        {
          return storageId;
        }
      }

      internal int ReadFromCache(byte[] dataBuffer, ref int offset, ref ulong filePosition, int length)
      {
        int sourceIndex = (int) ((long) filePosition - (long) pageId);
        int num = buffer.Length - sourceIndex;
        int length1 = length <= num ? length : num;
        Array.Copy((Array) buffer, sourceIndex, (Array) dataBuffer, offset, length1);
        offset += length1;
        filePosition += (ulong) length1;
        return length1;
      }

      internal int WriteToCache(byte[] dataBuffer, ref int offset, ref ulong filePosition, int length)
      {
        int destinationIndex = (int) ((long) filePosition - (long) pageId);
        int num = buffer.Length - destinationIndex;
        int length1 = length <= num ? length : num;
        Array.Copy((Array) dataBuffer, offset, (Array) buffer, destinationIndex, length1);
        if (cache != null && !isDirty)
          cache.DirtyPage(this);
        isDirty = true;
        offset += length1;
        filePosition += (ulong) length1;
        return length1;
      }

      internal void Refresh(FileStream fileStream, bool toRead)
      {
        Refresh(fileStream, toRead, buffer.Length);
      }

      internal void Refresh(FileStream fileStream, bool toRead, int len)
      {
        len = len < buffer.Length ? len : buffer.Length;
        try
        {
          fileStream.Seek((long) pageId, SeekOrigin.Begin);
          if (toRead)
          {
            if (!IsNewPageVersion())
              return;
            fileStream.Read(buffer, 0, len);
          }
          else
            fileStream.Write(buffer, 0, len);
        }
        finally
        {
          isDirty = false;
          if (cache != null && (long) pageId != (long) storageId)
            cache.FreshPage(this);
        }
      }

      private bool IsNewPageVersion()
      {
        return true;
      }
    }

    internal class PageCache : WeakReferenceCache<ulong, Page>
    {
      internal PageCache()
        : base(50)
      {
      }

      internal IEnumerator<Page> EnumerateByStorage(ulong storageId)
      {
        IEnumerator<Page> enumerator = EnumerateEntireCache();
        while (enumerator.MoveNext())
        {
                    Page page = enumerator.Current;
          if (page != null && (long) page.StorageId == (long) storageId)
            yield return page;
        }
      }

      internal List<Page> GetAllByStorage(ulong storageId)
      {
        List<Page> pageList = new List<Page>();
        IEnumerator<Page> enumerator = EnumerateByStorage(storageId);
        while (enumerator.MoveNext())
        {
                    Page current = enumerator.Current;
          if (current != null)
            pageList.Add(current);
        }
        return pageList;
      }
    }
  }
}
