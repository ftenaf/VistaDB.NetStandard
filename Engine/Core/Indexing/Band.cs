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
      int num = 33554432 / KeyApartment(patternRow);
      Capacity = maxCapacity > num ? num : maxCapacity / pieces;
      this.isolated = isolated;
    }

    internal int KeyCount
    {
      get
      {
        return keyCount;
      }
    }

    internal int Portions
    {
      get
      {
        return pieceCount;
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
      for (int index = 0; index < Count; ++index)
      {
        Row row = this[index];
        length += row.GetMemoryApartment(precedenceRow);
        precedenceRow = row;
      }
      if (buffer == null || buffer.Length < length)
        buffer = new byte[length];
      int num = 0;
      for (int index = 0; index < Count; ++index)
      {
        Row row = this[index];
        num = row.FormatRowBuffer(buffer, num, this.precedenceRow);
        this.precedenceRow = row;
      }
      if (storageHandle == null)
        storageHandle = fileManager.CreateTemporaryStorage(StorageHandle.DEFAULT_SIZE_OF_PAGE, false, isolated);
      storageHandle.DirectWriteBuffer(buffer, num);
      Clear();
    }

    private void ReadFromFile()
    {
      Clear();
      peekIndex = 0;
      if (readOffset == 0)
      {
        int length = (int) storageHandle.DirectGetLength();
        if (buffer == null || buffer.Length < length)
          buffer = new byte[length];
        storageHandle.DirectReadBuffer(buffer, length);
        precedenceRow = (Row) null;
      }
      for (int index = 0; index < Capacity && readOffset < buffer.Length; ++index)
      {
        Row row = patternRow.CopyInstance();
        readOffset = row.UnformatRowBuffer(buffer, readOffset, precedenceRow);
        precedenceRow = row;
        Add(row);
      }
    }

    internal void FlushTailPortion(bool cleanUp)
    {
      WriteToFile();
      storageHandle.DirectSeek(0L, SeekOrigin.Begin);
      readOffset = 0;
      precedenceRow = (Row) null;
      if (!cleanUp)
        return;
      buffer = (byte[]) null;
    }

    internal Row PeekKey()
    {
      if (keyCount == 0)
        return (Row) null;
      if (Count == 0 || peekIndex == Count)
        ReadFromFile();
      return this[peekIndex];
    }

    internal Row PopKey()
    {
      if (keyCount == 0)
        return (Row) null;
      if (Count == 0 || peekIndex == Count)
        ReadFromFile();
      --keyCount;
      Row row = this[peekIndex];
      this[peekIndex] = (Row) null;
      ++peekIndex;
      return row;
    }

    internal void PushKey(Row row)
    {
      if (Count == Capacity)
      {
        WriteToFile();
        ++pieceCount;
      }
      Add(row);
      ++keyCount;
    }

    public void Dispose()
    {
      if (isDisposed)
        return;
      patternRow = (Row) null;
      buffer = (byte[]) null;
      Clear();
      if (fileManager != null)
      {
        if (storageHandle != null)
          fileManager.CloseStorage(storageHandle);
        fileManager = (StorageManager) null;
      }
      isDisposed = true;
      GC.SuppressFinalize((object) this);
    }
  }
}
