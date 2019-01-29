using System;
using System.Collections.Generic;
using System.IO;
using VistaDB.Engine.Core.IO;

namespace VistaDB.Engine.Core.Indexing
{
  internal class Band : List<Row>, IDisposable
  {
    private byte[] buffer;
    private Row patternRow;
    private int keyCount;
    private int peekIndex;
    private int pieceCount;
    private StorageHandle storageHandle;
    private StorageManager fileManager;
    private bool isolated;
    private Row precedenceRow;
    private int readOffset;
    private bool isDisposed;

    internal Band(Row patternRow, StorageManager fileManager, int maxCapacity, int pieces, bool isolated)
    {
      this.patternRow = patternRow;
      this.fileManager = fileManager;
      int num = 33554432 / Band.KeyApartment(patternRow);
      this.Capacity = maxCapacity > num ? num : maxCapacity / pieces;
      this.isolated = isolated;
    }

    internal int KeyCount
    {
      get
      {
        return this.keyCount;
      }
    }

    internal int Portions
    {
      get
      {
        return this.pieceCount;
      }
    }

    internal static int KeyApartment(Row patternKey)
    {
      return patternKey.GetMemoryApartment((Row) null);
    }

    private void WriteToFile()
    {
      int length = 0;
      Row precedenceRow = this.precedenceRow;
      for (int index = 0; index < this.Count; ++index)
      {
        Row row = this[index];
        length += row.GetMemoryApartment(precedenceRow);
        precedenceRow = row;
      }
      if (this.buffer == null || this.buffer.Length < length)
        this.buffer = new byte[length];
      int num = 0;
      for (int index = 0; index < this.Count; ++index)
      {
        Row row = this[index];
        num = row.FormatRowBuffer(this.buffer, num, this.precedenceRow);
        this.precedenceRow = row;
      }
      if (this.storageHandle == null)
        this.storageHandle = this.fileManager.CreateTemporaryStorage(StorageHandle.DEFAULT_SIZE_OF_PAGE, false, this.isolated);
      this.storageHandle.DirectWriteBuffer(this.buffer, num);
      this.Clear();
    }

    private void ReadFromFile()
    {
      this.Clear();
      this.peekIndex = 0;
      if (this.readOffset == 0)
      {
        int length = (int) this.storageHandle.DirectGetLength();
        if (this.buffer == null || this.buffer.Length < length)
          this.buffer = new byte[length];
        this.storageHandle.DirectReadBuffer(this.buffer, length);
        this.precedenceRow = (Row) null;
      }
      for (int index = 0; index < this.Capacity && this.readOffset < this.buffer.Length; ++index)
      {
        Row row = this.patternRow.CopyInstance();
        this.readOffset = row.UnformatRowBuffer(this.buffer, this.readOffset, this.precedenceRow);
        this.precedenceRow = row;
        this.Add(row);
      }
    }

    internal void FlushTailPortion(bool cleanUp)
    {
      this.WriteToFile();
      this.storageHandle.DirectSeek(0L, SeekOrigin.Begin);
      this.readOffset = 0;
      this.precedenceRow = (Row) null;
      if (!cleanUp)
        return;
      this.buffer = (byte[]) null;
    }

    internal Row PeekKey()
    {
      if (this.keyCount == 0)
        return (Row) null;
      if (this.Count == 0 || this.peekIndex == this.Count)
        this.ReadFromFile();
      return this[this.peekIndex];
    }

    internal Row PopKey()
    {
      if (this.keyCount == 0)
        return (Row) null;
      if (this.Count == 0 || this.peekIndex == this.Count)
        this.ReadFromFile();
      --this.keyCount;
      Row row = this[this.peekIndex];
      this[this.peekIndex] = (Row) null;
      ++this.peekIndex;
      return row;
    }

    internal void PushKey(Row row)
    {
      if (this.Count == this.Capacity)
      {
        this.WriteToFile();
        ++this.pieceCount;
      }
      this.Add(row);
      ++this.keyCount;
    }

    public void Dispose()
    {
      if (this.isDisposed)
        return;
      this.patternRow = (Row) null;
      this.buffer = (byte[]) null;
      this.Clear();
      if (this.fileManager != null)
      {
        if (this.storageHandle != null)
          this.fileManager.CloseStorage(this.storageHandle);
        this.fileManager = (StorageManager) null;
      }
      this.isDisposed = true;
      GC.SuppressFinalize((object) this);
    }
  }
}
