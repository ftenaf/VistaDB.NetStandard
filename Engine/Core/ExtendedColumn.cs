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
    private static readonly int maxReferencing = ExtendedColumn.maxClusterCount * ExtendedColumn.clusterReferenceSize;
    private static readonly int maxPagesInCluster = (int) short.MaxValue;
    private static readonly int maxBytesPerCluster = ExtendedColumn.maxPagesInCluster * StorageHandle.DEFAULT_SIZE_OF_PAGE;
    private static readonly int maxSize = ExtendedColumn.maxBytesPerCluster * ExtendedColumn.maxClusterCount;
    private static Lzh lzhCoding = new Lzh(new Lzh.LzhStreaming(ExtendedColumn.ReadCompressStreaming), new Lzh.LzhStreaming(ExtendedColumn.WriteCompressStreaming));
    private static int instanceCount = 0;
    private ushort clusterLength = 1;
    private int currentInstance = ++ExtendedColumn.instanceCount;
    private object lck = new object();
    private ushort clusterCount;
    private bool clustersFreed;
    private object extendedValue;
    private int extendedBufferLength;
    private int packedDifference;
    private bool extensionOptimized;
    private byte extraCust;
    private bool needFlush;
    private ExtendedColumn.PostponeReadingMonitor postponingMonitor;

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
      : base((BinaryColumn) col)
    {
      this.extendedValue = col.extendedValue;
      this.clusterLength = col.clusterLength;
      this.clusterCount = col.clusterCount;
      this.clustersFreed = col.clustersFreed;
      this.extendedBufferLength = col.extendedBufferLength;
      this.packedDifference = col.packedDifference;
      this.extensionOptimized = col.extensionOptimized;
      this.extraCust = col.extraCust;
      this.Monitor = col.Monitor;
    }

    internal bool NeedFlush
    {
      get
      {
        return this.needFlush;
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
      return base.GetBufferLength(precedenceColumn) + (int) this.InheritedSize;
    }

    public override bool Edited
    {
      get
      {
        if (!this.needFlush)
          return base.Edited;
        return true;
      }
      set
      {
        base.Edited = value;
        this.needFlush = value;
      }
    }

    protected override ushort InheritedSize
    {
      get
      {
        return (ushort) (ExtendedColumn.dataReferenceSize + ExtendedColumn.dataCustSize + ExtendedColumn.dataCustSize + ExtendedColumn.dataReferenceSize + ExtendedColumn.clusterCounterSize + ExtendedColumn.clusterCounterSize);
      }
    }

    protected override int MaxArraySize
    {
      get
      {
        return ExtendedColumn.maxReferencing;
      }
    }

    public override int MaxLength
    {
      get
      {
        return ExtendedColumn.maxSize;
      }
    }

    public override bool IsNull
    {
      get
      {
        if (this.Monitor == null)
          return base.IsNull;
        return false;
      }
    }

    protected virtual byte[] OnFormatExtendedBuffer()
    {
      return (byte[]) this.extendedValue;
    }

    protected virtual object OnUnformatExtendedBuffer(byte[] buffer, int dataLength)
    {
      if (dataLength != buffer.Length)
      {
        byte[] numArray = new byte[dataLength];
        Array.Copy((Array) buffer, (Array) numArray, dataLength);
        buffer = numArray;
      }
      return (object) buffer;
    }

    internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      offset = base.ConvertToByteArray(buffer, offset, precedenceColumn);
      offset = VdbBitConverter.GetBytes((uint) this.extendedBufferLength, buffer, offset, ExtendedColumn.dataReferenceSize);
      buffer[offset] = this.extraCust;
      offset += ExtendedColumn.dataCustSize;
      buffer[offset] = this.extensionOptimized ? (byte) 1 : (byte) 0;
      offset += ExtendedColumn.dataCustSize;
      offset = VdbBitConverter.GetBytes((uint) this.packedDifference, buffer, offset, ExtendedColumn.dataReferenceSize);
      offset = VdbBitConverter.GetBytes(this.clusterLength, buffer, offset, ExtendedColumn.clusterCounterSize);
      offset = VdbBitConverter.GetBytes(this.clusterCount, buffer, offset, ExtendedColumn.clusterCounterSize);
      return offset;
    }

    internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
    {
      offset = base.ConvertFromByteArray(buffer, offset, precedenceColumn);
      this.extendedBufferLength = BitConverter.ToInt32(buffer, offset);
      offset += ExtendedColumn.dataReferenceSize;
      this.extraCust = buffer[offset];
      offset += ExtendedColumn.dataCustSize;
      this.extensionOptimized = buffer[offset] == (byte) 1;
      offset += ExtendedColumn.dataCustSize;
      this.packedDifference = (int) BitConverter.ToUInt32(buffer, offset);
      offset += ExtendedColumn.dataReferenceSize;
      this.clusterLength = BitConverter.ToUInt16(buffer, offset);
      offset += ExtendedColumn.clusterCounterSize;
      this.clusterCount = BitConverter.ToUInt16(buffer, offset);
      offset += ExtendedColumn.clusterCounterSize;
      return offset;
    }

    internal override void CreateFullCopy(Row.Column srcColumn)
    {
      base.CreateFullCopy(srcColumn);
      lock (this.lck)
      {
        ExtendedColumn extendedColumn = (ExtendedColumn) srcColumn;
        this.extendedValue = extendedColumn.extendedValue;
        this.extendedBufferLength = extendedColumn.extendedBufferLength;
        this.packedDifference = extendedColumn.packedDifference;
        this.extensionOptimized = extendedColumn.extensionOptimized;
        this.clusterLength = extendedColumn.clusterLength;
        this.clusterCount = extendedColumn.clusterCount;
        this.clustersFreed = extendedColumn.clustersFreed;
        this.needFlush = extendedColumn.needFlush;
        this.extraCust = extendedColumn.extraCust;
        this.Monitor = extendedColumn.Monitor;
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
      byte[] readBuffer = this.OnFormatExtendedBuffer();
      if (readBuffer == null)
      {
        this.clusterCount = (ushort) 0;
        this.clusterLength = (ushort) 1;
        this.clustersFreed = false;
        this.packedDifference = 0;
        this.extensionOptimized = false;
        toWriteLength = 0;
        return (byte[]) null;
      }
      int packedDifference = 0;
      toWriteLength = readBuffer.Length;
      if (this.Packed && toWriteLength > pageSize)
      {
        byte[] writeBuffer = new byte[toWriteLength + toWriteLength];
        try
        {
          int writtenBytes = 0;
          int num = ExtendedColumn.lzhCoding.LZHPack(ref writtenBytes, readBuffer, 0, writeBuffer, 0);
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
      this.extensionOptimized = allowOptimization && toWriteLength <= pageSize / 4;
      if (this.extensionOptimized)
      {
        this.extendedBufferLength = 0;
        this.extraCust = (byte) 0;
        this.packedDifference = packedDifference;
        this.ResetMetaValue();
        if (toWriteLength != readBuffer.Length)
        {
          byte[] numArray = new byte[toWriteLength];
          Buffer.BlockCopy((Array) readBuffer, 0, (Array) numArray, 0, toWriteLength);
          readBuffer = numArray;
        }
        base.Value = (object) readBuffer;
        return (byte[]) null;
      }
      if (encryption != null)
      {
        this.extraCust = (byte) ((encryption.Step - toWriteLength % encryption.Step) % encryption.Step);
        int length = toWriteLength + (int) this.extraCust;
        byte[] numArray = new byte[length];
        Buffer.BlockCopy((Array) readBuffer, 0, (Array) numArray, 0, toWriteLength);
        toWriteLength = length;
        encryption.Encrypt(numArray, numArray, toWriteLength);
        readBuffer = numArray;
      }
      else
        this.extraCust = (byte) 0;
      this.PrepareExternalPageList(toWriteLength, packedDifference, pageSize);
      return readBuffer;
    }

    private void UnformatExtendedBuffer(byte[] buffer, Encryption encryption, bool updateMode)
    {
      int dataLength = buffer.Length;
      if (encryption != null && !this.extensionOptimized)
      {
        encryption.Decrypt(buffer, buffer, buffer.Length);
        dataLength = buffer.Length - (int) this.extraCust;
      }
      if (this.Packed && this.packedDifference != 0 && !updateMode)
      {
        int originTextSize = dataLength + this.packedDifference;
        byte[] writeBuffer = new byte[originTextSize];
        try
        {
          ExtendedColumn.lzhCoding.LZHUnpack(originTextSize, buffer, 0, writeBuffer, 0);
          buffer = writeBuffer;
          dataLength = originTextSize;
        }
        catch
        {
        }
      }
      this.extendedValue = this.OnUnformatExtendedBuffer(buffer, dataLength);
    }

    private void SetMetaValues(int bufferLength, int pageSize)
    {
      bufferLength += (pageSize - bufferLength % pageSize) % pageSize;
      int num1 = bufferLength / pageSize;
      int num2 = num1 / ExtendedColumn.maxClusterCount + 1;
      if (num2 > ExtendedColumn.maxPagesInCluster)
        throw new VistaDBException(304, this.Name);
      this.clustersFreed = false;
      this.clusterLength = (ushort) num2;
      this.clusterCount = (ushort) ((uint) (num1 + ((int) this.clusterLength - num1 % (int) this.clusterLength) % (int) this.clusterLength) / (uint) this.clusterLength);
      this.extensionOptimized = false;
    }

    private void PrepareExternalPageList(int bufferLength, int packedDifference, int pageSize)
    {
      this.extendedBufferLength = bufferLength;
      this.packedDifference = packedDifference;
      this.SetMetaValues(bufferLength, pageSize);
      base.Value = (object) new byte[(int) this.clusterCount * ExtendedColumn.clusterReferenceSize];
    }

    private void SetCluster(int index, ulong clusterId)
    {
      VdbBitConverter.GetBytes(clusterId, (byte[]) base.Value, index * ExtendedColumn.clusterReferenceSize, ExtendedColumn.clusterReferenceSize);
    }

    private ulong GetCluster(int index)
    {
      return BitConverter.ToUInt64((byte[]) base.Value, index * ExtendedColumn.clusterReferenceSize);
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
      this.clusterCount = (ushort) 0;
      this.clusterLength = (ushort) 1;
      this.clustersFreed = false;
    }

    internal void UnformatExtension(DataStorage storage, bool postpone, Row rowKey, bool updateMode)
    {
      lock (this.lck)
      {
        if (this.needFlush || this.IsNull)
          return;
        if (this.extensionOptimized && !updateMode)
        {
          this.UnformatExtendedBuffer((byte[]) base.Value, this.Encrypted ? storage.Encryption : (Encryption) null, updateMode);
        }
        else
        {
          if (this.extendedBufferLength == 0)
            return;
          if (postpone)
          {
            this.Monitor = new ExtendedColumn.PostponeReadingMonitor(storage, rowKey);
          }
          else
          {
            byte[] buffer = new byte[this.extendedBufferLength];
            try
            {
              int extendedBufferLength = this.extendedBufferLength;
              int pageSize = storage.PageSize;
              int offset = 0;
              for (int index = 0; index < (int) this.clusterCount; ++index)
              {
                ulong cluster = this.GetCluster(index);
                int num = 0;
                while (num < (int) this.clusterLength && extendedBufferLength > 0)
                {
                  offset = storage.Handle.ReadPage(storage, cluster, buffer, offset, ref extendedBufferLength);
                  ++num;
                  cluster += (ulong) pageSize;
                }
              }
            }
            finally
            {
              this.UnformatExtendedBuffer(buffer, this.Encrypted ? storage.Encryption : (Encryption) null, updateMode);
            }
          }
        }
      }
    }

    internal void FormatExtension(DataStorage storage, bool allowOptimization)
    {
      if (!this.needFlush)
        return;
      int pageSize = storage.PageSize;
      this.FreeSpace(storage);
      int num1 = 0;
      byte[] buffer = this.FormatExtendedBuffer(pageSize, this.Encrypted ? storage.Encryption : (Encryption) null, ref num1, allowOptimization);
      try
      {
        if (buffer == null)
          return;
        int offset = 0;
        for (int index = 0; index < (int) this.clusterCount; ++index)
        {
          ulong freeCluster = storage.GetFreeCluster((int) this.clusterLength);
          this.SetCluster(index, freeCluster);
          int num2 = 0;
          while (num2 < (int) this.clusterLength && num1 > 0)
          {
            offset = storage.Handle.WritePage(storage, freeCluster, buffer, offset, ref num1);
            ++num2;
            freeCluster += (ulong) pageSize;
          }
        }
      }
      finally
      {
        this.needFlush = false;
      }
    }

    internal void FreeSpace(DataStorage parentStorage)
    {
      if (this.extensionOptimized || this.extendedBufferLength <= 0)
      {
        this.ResetMetaValue();
      }
      else
      {
        if (this.clustersFreed)
          return;
        int num = this.extendedBufferLength;
        for (int index = 0; index < (int) this.clusterCount; ++index)
        {
          num -= (int) this.clusterLength * parentStorage.PageSize;
          int clusterLength = (int) this.clusterLength;
          if (num < 0)
          {
            num = -num;
            clusterLength -= num / parentStorage.PageSize;
          }
          parentStorage.SetFreeCluster(this.GetCluster(index), clusterLength);
        }
        this.clustersFreed = true;
      }
    }

    internal int TestReferenceLength(int expandedDataLen, int pageSize)
    {
      this.SetMetaValues(expandedDataLen, pageSize);
      int num = (int) this.clusterCount * ExtendedColumn.clusterReferenceSize + this.GetLengthCounterWidth((Row.Column) null) + (int) this.InheritedSize;
      this.ResetMetaValue();
      return num;
    }

    private ExtendedColumn.PostponeReadingMonitor Monitor
    {
      get
      {
        lock (this.lck)
          return this.postponingMonitor;
      }
      set
      {
        lock (this.lck)
          this.postponingMonitor = value;
      }
    }

    public override object Value
    {
      get
      {
        lock (this.lck)
        {
          if (this.Monitor != null)
          {
            object obj = this.Monitor.Read(this);
            if (this.postponingMonitor != null && this.postponingMonitor.StayIndirect)
            {
              this.extendedValue = (object) null;
              return obj;
            }
            this.extendedValue = obj;
            this.Monitor = (ExtendedColumn.PostponeReadingMonitor) null;
          }
          return this.extendedValue;
        }
      }
      set
      {
        lock (this.lck)
        {
          this.extendedValue = value;
          this.Edited = true;
          this.ResetMetaValue();
          this.Monitor = (ExtendedColumn.PostponeReadingMonitor) null;
        }
      }
    }

    private class PostponeReadingMonitor
    {
      private const int StringLocalReferenceLimit = 4096;
      private const int BinaryLocalReferenceLimit = 4096;
      private DataStorage postponedUnformatStorage;
      private WeakReference cachedValue;
      private Row rowKey;

      internal bool StayIndirect
      {
        get
        {
          return this.cachedValue != null;
        }
      }

      internal PostponeReadingMonitor(DataStorage storage, Row key)
      {
        this.postponedUnformatStorage = storage;
        this.rowKey = key.CopyInstance();
        this.rowKey.RowVersion = Row.MinVersion;
      }

      internal object Read(ExtendedColumn column)
      {
        lock (this)
        {
          if (this.cachedValue != null)
          {
            object target = this.cachedValue.Target;
            if (this.cachedValue.IsAlive && target != null)
              return target;
          }
          this.postponedUnformatStorage.RereadExtendedColumn(column, this.rowKey);
          object extendedValue = column.extendedValue;
          this.cachedValue = (WeakReference) null;
          return extendedValue;
        }
      }
    }
  }
}
