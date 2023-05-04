using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.IO;

namespace VistaDB.Engine.Core
{
  internal class ExtendedColumn : BinaryColumn
  {
    private static readonly int clusterReferenceSize = 8;
    private static readonly int dataReferenceSize = 4;
    private static readonly int dataCustSize = 1;
    private static readonly int clusterCounterSize = 2;
    private static readonly int maxClusterCount = 4;
    private static readonly int maxReferencing = maxClusterCount * clusterReferenceSize;
    private static readonly int maxPagesInCluster = short.MaxValue;
    private static readonly int maxBytesPerCluster = maxPagesInCluster * StorageHandle.DEFAULT_SIZE_OF_PAGE;
    private static readonly int maxSize = maxBytesPerCluster * maxClusterCount;
    private static Lzh lzhCoding = new Lzh(new Lzh.LzhStreaming(ReadCompressStreaming), new Lzh.LzhStreaming(WriteCompressStreaming));
    private static int instanceCount = 0;
    private ushort clusterLength = 1;
    private int currentInstance = ++instanceCount;
    private object lck = new object();
    private ushort clusterCount;
    private bool clustersFreed;
    private object extendedValue;
    private int extendedBufferLength;
    private int packedDifference;
    private bool extensionOptimized;
    private byte extraCust;
    private bool needFlush;
    private PostponeReadingMonitor postponingMonitor;

    protected ExtendedColumn(VistaDBType type)
      : base(type)
    {
    }

    protected ExtendedColumn(object extendedValue, VistaDBType type)
      : base(type)
    {
      this.extendedValue = extendedValue;
    }

    protected ExtendedColumn(ExtendedColumn col)
      : base(col)
    {
      extendedValue = col.extendedValue;
      clusterLength = col.clusterLength;
      clusterCount = col.clusterCount;
      clustersFreed = col.clustersFreed;
      extendedBufferLength = col.extendedBufferLength;
      packedDifference = col.packedDifference;
      extensionOptimized = col.extensionOptimized;
      extraCust = col.extraCust;
      Monitor = col.Monitor;
    }

    internal bool NeedFlush
    {
      get
      {
        return needFlush;
      }
    }

    public override bool ExtendedType
    {
      get
      {
        return true;
      }
    }

    internal override int GetBufferLength(Row.Column precedenceColumn)
    {
      return base.GetBufferLength(precedenceColumn) + InheritedSize;
    }

    public override bool Edited
    {
      get
      {
        if (!needFlush)
          return base.Edited;
        return true;
      }
      set
      {
        base.Edited = value;
        needFlush = value;
      }
    }

    protected override ushort InheritedSize
    {
      get
      {
        return (ushort) (dataReferenceSize + dataCustSize + dataCustSize + dataReferenceSize + clusterCounterSize + clusterCounterSize);
      }
    }

    protected override int MaxArraySize
    {
      get
      {
        return maxReferencing;
      }
    }

    public override int MaxLength
    {
      get
      {
        return maxSize;
      }
    }

    public override bool IsNull
    {
      get
      {
        if (Monitor == null)
          return base.IsNull;
        return false;
      }
    }

    protected virtual byte[] OnFormatExtendedBuffer()
    {
      return (byte[]) extendedValue;
    }

    protected virtual object OnUnformatExtendedBuffer(byte[] buffer, int dataLength)
    {
      if (dataLength != buffer.Length)
      {
        byte[] numArray = new byte[dataLength];
        Array.Copy(buffer, numArray, dataLength);
        buffer = numArray;
      }
      return buffer;
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      offset = base.ConvertToByteArray(buffer, offset, precedenceColumn);
      offset = VdbBitConverter.GetBytes((uint) extendedBufferLength, buffer, offset, dataReferenceSize);
      buffer[offset] = extraCust;
      offset += dataCustSize;
      buffer[offset] = extensionOptimized ? (byte) 1 : (byte) 0;
      offset += dataCustSize;
      offset = VdbBitConverter.GetBytes((uint) packedDifference, buffer, offset, dataReferenceSize);
      offset = VdbBitConverter.GetBytes(clusterLength, buffer, offset, clusterCounterSize);
      offset = VdbBitConverter.GetBytes(clusterCount, buffer, offset, clusterCounterSize);
      return offset;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      offset = base.ConvertFromByteArray(buffer, offset, precedenceColumn);
      extendedBufferLength = BitConverter.ToInt32(buffer, offset);
      offset += dataReferenceSize;
      extraCust = buffer[offset];
      offset += dataCustSize;
      extensionOptimized = buffer[offset] == 1;
      offset += dataCustSize;
      packedDifference = (int) BitConverter.ToUInt32(buffer, offset);
      offset += dataReferenceSize;
      clusterLength = BitConverter.ToUInt16(buffer, offset);
      offset += clusterCounterSize;
      clusterCount = BitConverter.ToUInt16(buffer, offset);
      offset += clusterCounterSize;
      return offset;
    }

    internal override void CreateFullCopy(Row.Column srcColumn)
    {
      base.CreateFullCopy(srcColumn);
      lock (lck)
      {
        ExtendedColumn extendedColumn = (ExtendedColumn) srcColumn;
        extendedValue = extendedColumn.extendedValue;
        extendedBufferLength = extendedColumn.extendedBufferLength;
        packedDifference = extendedColumn.packedDifference;
        extensionOptimized = extendedColumn.extensionOptimized;
        clusterLength = extendedColumn.clusterLength;
        clusterCount = extendedColumn.clusterCount;
        clustersFreed = extendedColumn.clustersFreed;
        needFlush = extendedColumn.needFlush;
        extraCust = extendedColumn.extraCust;
        Monitor = extendedColumn.Monitor;
      }
    }

    private static int ReadCompressStreaming(ref byte input, byte[] buffer, ref int offset)
    {
      if (offset >= buffer.Length)
        return 0;
      input = buffer[offset++];
      return 1;
    }

    private static int WriteCompressStreaming(ref byte input, byte[] buffer, ref int offset)
    {
      if (offset >= buffer.Length)
        return 0;
      buffer[offset++] = input;
      return 1;
    }

    private byte[] FormatExtendedBuffer(int pageSize, Encryption encryption, ref int toWriteLength, bool allowOptimization)
    {
      byte[] readBuffer = OnFormatExtendedBuffer();
      if (readBuffer == null)
      {
        clusterCount = 0;
        clusterLength = 1;
        clustersFreed = false;
        this.packedDifference = 0;
        extensionOptimized = false;
        toWriteLength = 0;
        return null;
      }
      int packedDifference = 0;
      toWriteLength = readBuffer.Length;
      if (Packed && toWriteLength > pageSize)
      {
        byte[] writeBuffer = new byte[toWriteLength + toWriteLength];
        try
        {
          int writtenBytes = 0;
          int num = lzhCoding.LZHPack(ref writtenBytes, readBuffer, 0, writeBuffer, 0);
          if (toWriteLength / pageSize > num / pageSize)
          {
            packedDifference = toWriteLength - num;
            toWriteLength = num;
            readBuffer = writeBuffer;
          }
        }
        catch
        {
        }
      }
      extensionOptimized = allowOptimization && toWriteLength <= pageSize / 4;
      if (extensionOptimized)
      {
        extendedBufferLength = 0;
        extraCust = 0;
        this.packedDifference = packedDifference;
        ResetMetaValue();
        if (toWriteLength != readBuffer.Length)
        {
          byte[] numArray = new byte[toWriteLength];
          Buffer.BlockCopy(readBuffer, 0, numArray, 0, toWriteLength);
          readBuffer = numArray;
        }
        base.Value = readBuffer;
        return null;
      }
      if (encryption != null)
      {
        extraCust = (byte) ((encryption.Step - toWriteLength % encryption.Step) % encryption.Step);
        int length = toWriteLength + extraCust;
        byte[] numArray = new byte[length];
        Buffer.BlockCopy(readBuffer, 0, numArray, 0, toWriteLength);
        toWriteLength = length;
        encryption.Encrypt(numArray, numArray, toWriteLength);
        readBuffer = numArray;
      }
      else
        extraCust = 0;
      PrepareExternalPageList(toWriteLength, packedDifference, pageSize);
      return readBuffer;
    }

    private void UnformatExtendedBuffer(byte[] buffer, Encryption encryption, bool updateMode)
    {
      int dataLength = buffer.Length;
      if (encryption != null && !extensionOptimized)
      {
        encryption.Decrypt(buffer, buffer, buffer.Length);
        dataLength = buffer.Length - extraCust;
      }
      if (Packed && packedDifference != 0 && !updateMode)
      {
        int originTextSize = dataLength + packedDifference;
        byte[] writeBuffer = new byte[originTextSize];
        try
        {
                    lzhCoding.LZHUnpack(originTextSize, buffer, 0, writeBuffer, 0);
          buffer = writeBuffer;
          dataLength = originTextSize;
        }
        catch
        {
        }
      }
      extendedValue = OnUnformatExtendedBuffer(buffer, dataLength);
    }

    private void SetMetaValues(int bufferLength, int pageSize)
    {
      bufferLength += (pageSize - bufferLength % pageSize) % pageSize;
      int num1 = bufferLength / pageSize;
      int num2 = num1 / maxClusterCount + 1;
      if (num2 > maxPagesInCluster)
        throw new VistaDBException(304, Name);
      clustersFreed = false;
      clusterLength = (ushort) num2;
      clusterCount = (ushort) ((uint) (num1 + (clusterLength - num1 % clusterLength) % clusterLength) / clusterLength);
      extensionOptimized = false;
    }

    private void PrepareExternalPageList(int bufferLength, int packedDifference, int pageSize)
    {
      extendedBufferLength = bufferLength;
      this.packedDifference = packedDifference;
      SetMetaValues(bufferLength, pageSize);
      base.Value = (new byte[clusterCount * clusterReferenceSize]);
    }

    private void SetCluster(int index, ulong clusterId)
    {
      VdbBitConverter.GetBytes(clusterId, (byte[]) base.Value, index * clusterReferenceSize, clusterReferenceSize);
    }

    private ulong GetCluster(int index)
    {
      return BitConverter.ToUInt64((byte[]) base.Value, index * clusterReferenceSize);
    }

    internal virtual int CompareLength
    {
      get
      {
        return 0;
      }
    }

    internal void ResetMetaValue()
    {
      clusterCount = 0;
      clusterLength = 1;
      clustersFreed = false;
    }

    internal void UnformatExtension(DataStorage storage, bool postpone, Row rowKey, bool updateMode)
    {
      lock (lck)
      {
        if (needFlush || IsNull)
          return;
        if (extensionOptimized && !updateMode)
        {
          UnformatExtendedBuffer((byte[]) base.Value, Encrypted ? storage.Encryption : null, updateMode);
        }
        else
        {
          if (extendedBufferLength == 0)
            return;
          if (postpone)
          {
            Monitor = new PostponeReadingMonitor(storage, rowKey);
          }
          else
          {
            byte[] buffer = new byte[extendedBufferLength];
            try
            {
              int extendedBufferLength = this.extendedBufferLength;
              int pageSize = storage.PageSize;
              int offset = 0;
              for (int index = 0; index < clusterCount; ++index)
              {
                ulong cluster = GetCluster(index);
                int num = 0;
                while (num < clusterLength && extendedBufferLength > 0)
                {
                  offset = storage.Handle.ReadPage(storage, cluster, buffer, offset, ref extendedBufferLength);
                  ++num;
                  cluster += (ulong) pageSize;
                }
              }
            }
            finally
            {
              UnformatExtendedBuffer(buffer, Encrypted ? storage.Encryption : null, updateMode);
            }
          }
        }
      }
    }

    internal void FormatExtension(DataStorage storage, bool allowOptimization)
    {
      if (!needFlush)
        return;
      int pageSize = storage.PageSize;
      FreeSpace(storage);
      int num1 = 0;
      byte[] buffer = FormatExtendedBuffer(pageSize, Encrypted ? storage.Encryption : null, ref num1, allowOptimization);
      try
      {
        if (buffer == null)
          return;
        int offset = 0;
        for (int index = 0; index < clusterCount; ++index)
        {
          ulong freeCluster = storage.GetFreeCluster(clusterLength);
          SetCluster(index, freeCluster);
          int num2 = 0;
          while (num2 < clusterLength && num1 > 0)
          {
            offset = storage.Handle.WritePage(storage, freeCluster, buffer, offset, ref num1);
            ++num2;
            freeCluster += (ulong) pageSize;
          }
        }
      }
      finally
      {
        needFlush = false;
      }
    }

    internal void FreeSpace(DataStorage parentStorage)
    {
      if (extensionOptimized || extendedBufferLength <= 0)
      {
        ResetMetaValue();
      }
      else
      {
        if (clustersFreed)
          return;
        int num = extendedBufferLength;
        for (int index = 0; index < clusterCount; ++index)
        {
          num -= this.clusterLength * parentStorage.PageSize;
          int clusterLength = this.clusterLength;
          if (num < 0)
          {
            num = -num;
            clusterLength -= num / parentStorage.PageSize;
          }
          parentStorage.SetFreeCluster(GetCluster(index), clusterLength);
        }
        clustersFreed = true;
      }
    }

    internal int TestReferenceLength(int expandedDataLen, int pageSize)
    {
      SetMetaValues(expandedDataLen, pageSize);
      int num = clusterCount * clusterReferenceSize + GetLengthCounterWidth(null) + InheritedSize;
      ResetMetaValue();
      return num;
    }

    private PostponeReadingMonitor Monitor
    {
      get
      {
        lock (lck)
          return postponingMonitor;
      }
      set
      {
        lock (lck)
          postponingMonitor = value;
      }
    }

    public override object Value
    {
      get
      {
        lock (lck)
        {
          if (Monitor != null)
          {
            object obj = Monitor.Read(this);
            if (postponingMonitor != null && postponingMonitor.StayIndirect)
            {
              extendedValue = null;
              return obj;
            }
            extendedValue = obj;
            Monitor = null;
          }
          return extendedValue;
        }
      }
      set
      {
        lock (lck)
        {
          extendedValue = value;
          Edited = true;
          ResetMetaValue();
          Monitor = null;
        }
      }
    }

    private class PostponeReadingMonitor
    {
            private DataStorage postponedUnformatStorage;
      private WeakReference cachedValue;
      private Row rowKey;

      internal bool StayIndirect
      {
        get
        {
          return cachedValue != null;
        }
      }

      internal PostponeReadingMonitor(DataStorage storage, Row key)
      {
        postponedUnformatStorage = storage;
        rowKey = key.CopyInstance();
        rowKey.RowVersion = Row.MinVersion;
      }

      internal object Read(ExtendedColumn column)
      {
        lock (this)
        {
          if (cachedValue != null)
          {
            object target = cachedValue.Target;
            if (cachedValue.IsAlive && target != null)
              return target;
          }
          postponedUnformatStorage.RereadExtendedColumn(column, rowKey);
          object extendedValue = column.extendedValue;
          cachedValue = null;
          return extendedValue;
        }
      }
    }
  }
}
