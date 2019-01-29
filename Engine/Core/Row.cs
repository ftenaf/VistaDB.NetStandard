using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class Row : List<Row.Column>, IRow, IVistaDBRow, IEnumerable
  {
    internal static uint MinRowId = 0;
    internal static uint MaxRowId = uint.MaxValue;
    internal static ulong EmptyReference = ulong.MaxValue;
    internal static uint MinVersion = 0;
    internal static uint MaxVersion = uint.MaxValue;
    protected static readonly int CounterSize = 4;
    private ulong position = Row.EmptyReference;
    private int timestampIndex = -1;
    private DateTime lastRefresh = DateTime.Now;
    private Row.MetaData metaInfo;
    private bool ascending;
    private byte[] buffer;
    private int formatLength;
    private int[] activeCompareMask;
    private bool partialRow;
    private RowExtension extensions;
    protected bool alignment;
    private Encryption encryption;
    private int lastVersion;

    internal static Row CreateInstance(uint rowId, bool ascending, Encryption encryption, int[] activeMask)
    {
      return new Row(rowId, Row.MinVersion, Row.EmptyReference, ascending, encryption, activeMask);
    }

    internal static Row CreateInstance(uint rowId, bool ascending, Encryption encryption, int[] activeMask, int maxColumnCount)
    {
      return new Row(rowId, Row.MinVersion, Row.EmptyReference, ascending, encryption, activeMask, maxColumnCount);
    }

    internal Row CopyInstance()
    {
      return new Row(this);
    }

    protected Row(uint rowId, uint rowVersion, ulong referencedPosition, bool ascending, Encryption encryption, int[] activeMask)
    {
      this.metaInfo = new Row.MetaData(rowId, rowVersion, referencedPosition);
      this.ascending = ascending;
      this.encryption = encryption;
      this.activeCompareMask = activeMask;
    }

    protected Row(uint rowId, uint rowVersion, ulong referencedPosition, bool ascending, Encryption encryption, int[] activeMask, int maxColCount)
      : base(maxColCount)
    {
      this.metaInfo = new Row.MetaData(rowId, rowVersion, referencedPosition);
      this.ascending = ascending;
      this.encryption = encryption;
      this.activeCompareMask = activeMask;
    }

    private Row(Row row)
      : this(row.RowId, row.RowVersion, row.RefPosition, row.ascending, row.encryption, row.activeCompareMask, row.Count)
    {
      foreach (Row.Column column in (List<Row.Column>) row)
        this.AppendColumn((IColumn) column.Duplicate(false));
      this.lastRefresh = row.lastRefresh;
      this.lastVersion = row.lastVersion;
    }

    private bool NoParticipation(Row.Column column)
    {
      if (column.IsNull)
        return true;
      if (this.partialRow && this.activeCompareMask != null)
        return this.activeCompareMask[column.RowIndex] == 0;
      return false;
    }

    private int GetHeadLength(Row precedenceRow)
    {
      int num1 = Row.CounterSize + this.metaInfo.GetBufferLength(precedenceRow == null ? (Row.Column) null : (Row.Column) precedenceRow.metaInfo);
      int num2 = this.Count - 1;
      int num3 = num2 + (8 - num2 % 8);
      return num1 + num3 / 8;
    }

    private int GetEncryptedMemoryApartment(Row precedenceRow)
    {
      int headLength = this.GetHeadLength(precedenceRow);
      int step = this.encryption.Step;
      int num = -1;
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        if (!this.NoParticipation(column))
        {
          if (column.Encrypted)
          {
            if (num < 0)
              num = headLength;
          }
          else if (num >= 0)
          {
            headLength += (step - (headLength - num) % step) % step;
            num = -1;
          }
          headLength += column.GetBufferLength(this.GetPrecedenceColumn(precedenceRow, column.RowIndex));
        }
      }
      if (num >= 0)
        headLength += (step - (headLength - num) % step) % step;
      return headLength + Row.CounterSize;
    }

    internal bool Ascending
    {
      get
      {
        return this.ascending;
      }
    }

    internal byte[] Buffer
    {
      get
      {
        return this.buffer;
      }
    }

    internal bool HasTimestamp
    {
      get
      {
        return this.timestampIndex >= 0;
      }
    }

    internal Row.Column TimeStampColumn
    {
      get
      {
        if (!this.HasTimestamp)
          return (Row.Column) null;
        return this[this.timestampIndex];
      }
    }

    internal uint RowId
    {
      get
      {
        return (uint) (int) this.metaInfo.rowID.Value;
      }
      set
      {
        this.metaInfo.rowID.Value = (object) (int) value;
      }
    }

    internal uint RowVersion
    {
      get
      {
        return (uint) (int) this.metaInfo.Value;
      }
      set
      {
        this.metaInfo.Value = (object) (int) value;
      }
    }

    internal bool OutdatedStatus
    {
      get
      {
        return ((int) this.RowVersion & int.MinValue) != 0;
      }
      set
      {
        if (value)
          this.RowVersion |= 2147483648U;
        else
          this.RowVersion &= (uint) int.MaxValue;
      }
    }

    internal uint TransactionId
    {
      get
      {
        return this.RowVersion & (uint) int.MaxValue;
      }
    }

    internal ulong Position
    {
      get
      {
        return this.position;
      }
      set
      {
        this.position = value;
      }
    }

    internal int FormatLength
    {
      get
      {
        return this.formatLength;
      }
      set
      {
        if (this.Buffer == null || this.Buffer.Length < value)
          this.AllocateBuffer(value);
        this.formatLength = value;
      }
    }

    internal ulong RefPosition
    {
      get
      {
        return this.metaInfo.referencedPosition;
      }
      set
      {
        this.metaInfo.referencedPosition = value;
      }
    }

    internal bool Alignment
    {
      get
      {
        return this.alignment;
      }
    }

    internal bool PartialRow
    {
      set
      {
        this.partialRow = value;
      }
    }

    internal int[] ComparingMask
    {
      get
      {
        return this.activeCompareMask;
      }
    }

    internal bool EditedExtensions
    {
      get
      {
        if (this.Extensions == null)
          return false;
        foreach (Row.Column extension in (List<IColumn>) this.Extensions)
        {
          if (extension.Edited)
            return true;
        }
        return false;
      }
    }

    internal bool HasNulls
    {
      get
      {
        foreach (Row.Column column in (List<Row.Column>) this)
        {
          if (column.IsNull)
            return true;
        }
        return false;
      }
    }

    internal DateTime LastRefresh
    {
      get
      {
        return this.lastRefresh;
      }
    }

    internal int LastVersion
    {
      get
      {
        return this.lastVersion;
      }
    }

    internal virtual RowExtension Extensions
    {
      get
      {
        return this.extensions;
      }
    }

    private void IncrementVersion()
    {
      this.lastRefresh = DateTime.Now;
      ++this.lastVersion;
    }

    private int CompareMaskedColumns(Row row)
    {
      int num1 = 0;
      int count = this.Count;
      for (int index1 = 0; index1 < count && num1 == 0; ++index1)
      {
        int num2 = this.activeCompareMask[index1];
        if (num2 != 0)
        {
          bool flag = num2 < 0;
          if (flag)
            num2 = -num2;
          int index2 = num2 - 1;
          num1 = !flag ? this[index2].MinusColumn(row[index2]) : row[index2].MinusColumn(this[index2]);
        }
      }
      if (!this.ascending)
        return -num1;
      return num1;
    }

    private int CompareUnmaskedColumns(Row row)
    {
      int num = 0;
      for (int index = 0; index < this.Count && num == 0; ++index)
        num = this[index].MinusColumn(row[index]);
      if (!this.ascending)
        return -num;
      return num;
    }

    private int CompareTpHidden(Row row)
    {
      int num = this.activeCompareMask == null ? this.CompareUnmaskedColumns(row) : this.CompareMaskedColumns(row);
      if (num == 0)
      {
        uint rowId1 = this.RowId;
        uint rowId2 = row.RowId;
        num = rowId1 > rowId2 ? 1 : (rowId1 < rowId2 ? -1 : 0);
        if (num == 0)
        {
          uint transactionId1 = this.TransactionId;
          uint transactionId2 = row.TransactionId;
          num = transactionId1 > transactionId2 ? 1 : (transactionId1 < transactionId2 ? -1 : 0);
          if (num == 0)
          {
            bool outdatedStatus1 = this.OutdatedStatus;
            bool outdatedStatus2 = row.OutdatedStatus;
            num = !outdatedStatus1 || outdatedStatus2 ? (outdatedStatus1 || !outdatedStatus2 ? 0 : -1) : 1;
          }
        }
      }
      return num;
    }

    private int CompareTpVisible(Row row)
    {
      int num = this.activeCompareMask == null ? this.CompareUnmaskedColumns(row) : this.CompareMaskedColumns(row);
      if (num == 0)
      {
        uint rowId1 = this.RowId;
        uint rowId2 = row.RowId;
        num = rowId1 > rowId2 ? 1 : (rowId1 < rowId2 ? -1 : 0);
      }
      return num;
    }

    private int ConvertNullsToBuffer(byte[] buffer, int offset)
    {
      --offset;
      for (int index = 0; index < this.Count; ++index)
      {
        int num = index % 8;
        if (num == 0)
          buffer[++offset] = (byte) 0;
        if (this[index].IsNull)
          buffer[offset] = (byte) ((uint) buffer[offset] | (uint) (byte) (1 << num));
      }
      return ++offset;
    }

    private int ConvertNullsFromBuffer(byte[] buffer, int offset)
    {
      --offset;
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        int num = column.RowIndex % 8;
        if (num == 0)
          ++offset;
        column.Value = ((int) buffer[offset] & (int) (byte) (1 << num)) == 0 ? column.DummyNull : (object) null;
      }
      return ++offset;
    }

    private new void Add(Row.Column column)
    {
      base.Add(column);
    }

    private int FormatHeader()
    {
      return this.FormatHeader(this.buffer, 0, (Row) null);
    }

    private int FormatHeader(byte[] buffer, int offset, Row precedenceRow)
    {
      int dstOffset = offset;
      offset += Row.CounterSize;
      offset = this.metaInfo.ConvertToByteArray(buffer, offset, precedenceRow == null ? (Row.Column) null : (Row.Column) precedenceRow.metaInfo);
      int num = (this.Count - 1) / 8;
      offset = this.ConvertNullsToBuffer(buffer, offset);
      System.Buffer.BlockCopy((Array) BitConverter.GetBytes(offset - dstOffset), 0, (Array) buffer, dstOffset, Row.CounterSize);
      this.IncrementVersion();
      return offset;
    }

    private void UnformatHeader()
    {
      this.UnformatHeader(this.buffer, 0, true, (Row) null);
    }

    private int UnformatHeader(byte[] buffer, int offset, bool bypassNulls, Row precedenceRow)
    {
      int num = BitConverter.ToInt32(buffer, offset) + offset;
      offset += Row.CounterSize;
      offset = this.metaInfo.ConvertFromByteArray(buffer, offset, precedenceRow == null ? (Row.Column) null : (Row.Column) precedenceRow.metaInfo);
      if (bypassNulls)
        return offset;
      offset = this.ConvertNullsFromBuffer(buffer, offset);
      this.IncrementVersion();
      return num;
    }

    private int FormatEncryptedColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      int step = this.encryption.Step;
      int offset1 = -1;
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        try
        {
          if (!this.NoParticipation(column))
          {
            if (column.Encrypted)
            {
              if (offset1 < 0)
                offset1 = offset;
            }
            else if (offset1 >= 0)
            {
              offset += (step - (offset - offset1) % step) % step;
              this.encryption.Encrypt(buffer, offset1, offset - offset1);
              offset1 = -1;
            }
            offset = column.ConvertToByteArray(buffer, offset, this.GetPrecedenceColumn(precedenceRow, column.RowIndex));
          }
        }
        finally
        {
          column.Edited = false;
        }
      }
      if (offset1 >= 0)
      {
        offset += (step - (offset - offset1) % step) % step;
        this.encryption.Encrypt(buffer, offset1, offset - offset1);
      }
      return offset;
    }

    private int UnformatEncryptedColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      int step = this.encryption.Step;
      int num1 = 0;
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        try
        {
          if (!this.NoParticipation(column))
          {
            Row.Column precedenceColumn = this.GetPrecedenceColumn(precedenceRow, column.RowIndex);
            if (column.Encrypted)
            {
              int num2 = column.GetLengthCounterWidth(precedenceColumn);
              bool flag = num2 == 0;
              if (flag)
                num2 = column.GetBufferLength(precedenceColumn);
              int num3 = 0;
              do
              {
                int num4 = num1 - num2;
                if (num4 < 0)
                {
                  int offset1 = offset + num3 + num1;
                  int num5 = -num4;
                  num1 = (step - num5 % step) % step;
                  this.encryption.Decrypt(buffer, offset1, num5 + num1);
                }
                else
                  num1 = num4;
                if (!flag)
                {
                  num3 = num2;
                  num2 = column.ReadVarLength(buffer, offset, precedenceColumn);
                }
                flag = !flag;
              }
              while (flag);
            }
            else
            {
              offset += num1;
              num1 = 0;
            }
            offset = column.ConvertFromByteArray(buffer, offset, precedenceColumn);
          }
        }
        finally
        {
          column.Edited = false;
        }
      }
      return offset + num1;
    }

    private Row.Column GetPrecedenceColumn(Row precedenceRow, int index)
    {
      if (precedenceRow == null)
        return (Row.Column) null;
      Row.Column column = precedenceRow[index];
      if (!column.IsNull)
        return column;
      return (Row.Column) null;
    }

    protected void AllocateBuffer(int length)
    {
      this.buffer = new byte[length];
    }

    internal int FormatRowBuffer(byte[] buffer, int offset, Row precedenceRow)
    {
      return this.FormatColumns(buffer, this.FormatHeader(buffer, offset, precedenceRow), precedenceRow);
    }

    private int FormatRowBuffer(Row precedenceRow)
    {
      return this.FormatRowBuffer(this.buffer, 0, precedenceRow);
    }

    internal int UnformatRowBuffer(byte[] buffer, int offset, Row precedenceRow)
    {
      return this.UnformatColumns(buffer, this.UnformatHeader(buffer, offset, false, precedenceRow), precedenceRow);
    }

    protected int UnformatRowBuffer()
    {
      return this.UnformatRowBuffer(this.buffer, 0, (Row) null);
    }

    internal void SyncPartialRow()
    {
      if (this.activeCompareMask == null)
        return;
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        if (this.activeCompareMask[column.RowIndex] == 0)
          column.Edited = false;
      }
    }

    internal void Copy(Row sourceRow)
    {
      this.CopyData(sourceRow);
      this.CopyMetaData(sourceRow);
    }

    private void CopyData(Row sourceRow)
    {
      if (this == sourceRow)
        return;
      for (int index = 0; index < this.Count; ++index)
        this[index].CreateFullCopy(sourceRow[index]);
    }

    internal void CopyMetaData(Row sourceRow)
    {
      if (this == sourceRow)
        return;
      this.RowId = sourceRow.RowId;
      this.RowVersion = sourceRow.RowVersion;
      this.RefPosition = sourceRow.RefPosition;
      this.ascending = sourceRow.ascending;
    }

    internal Row.Column LookForColumn(char[] buffer, int offset, bool containSpaces)
    {
      bool firstPosition = false;
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        int length = column.Name.Length;
        if (length <= buffer.Length - offset && Database.DatabaseObject.EqualNames(new string(buffer, offset, length), column.Name))
        {
          int index = offset + length;
          if (index >= buffer.Length || !DirectConnection.IsCorrectNameSymbol(buffer[index], ref firstPosition, containSpaces))
            return column;
        }
      }
      return (Row.Column) null;
    }

    internal Row.Column LookForColumn(string name)
    {
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        if (Database.DatabaseObject.EqualNames(name, column.Name))
          return column;
      }
      return (Row.Column) null;
    }

    public void InitTop()
    {
      foreach (Row.Column column in (List<Row.Column>) this)
        column.InitTop();
      this.RowId = Row.MinRowId;
      this.RowVersion = Row.MinVersion;
      this.RefPosition = Row.EmptyReference;
      this.Position = Row.EmptyReference;
    }

    public void InitBottom()
    {
      foreach (Row.Column column in (List<Row.Column>) this)
        column.InitBottom();
      this.RowId = Row.MaxRowId;
      this.RowVersion = Row.MaxVersion;
      this.RefPosition = Row.EmptyReference;
      this.Position = Row.EmptyReference;
    }

    internal bool EqualColumns(Row row, bool masked)
    {
      if (!masked)
        return this.CompareUnmaskedColumns(row) == 0;
      return this.CompareMaskedColumns(row) == 0;
    }

    internal void ClearEditStatus()
    {
      foreach (Row.Column column in (List<Row.Column>) this)
        column.Edited = false;
    }

    internal void ReorderByIndex()
    {
      foreach (Row.Column column in this.GetRange(0, this.Count))
      {
        this[column.RowIndex] = column;
        if (column.Type == VistaDBType.Timestamp)
          this.timestampIndex = column.RowIndex;
      }
    }

    internal void InstantiateComparingMask()
    {
      this.activeCompareMask = new int[this.Count];
    }

    internal int FormatColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      offset += Row.CounterSize;
      int num = offset;
      if (this.encryption != null)
      {
        offset = this.FormatEncryptedColumns(buffer, offset, precedenceRow);
      }
      else
      {
        foreach (Row.Column column in (List<Row.Column>) this)
        {
          try
          {
            if (!this.NoParticipation(column))
              offset = column.ConvertToByteArray(buffer, offset, this.GetPrecedenceColumn(precedenceRow, column.RowIndex));
          }
          finally
          {
            column.Edited = false;
          }
        }
      }
      VdbBitConverter.GetBytes((uint) (offset - num), buffer, num - Row.CounterSize, Row.CounterSize);
      return offset;
    }

    internal int UnformatColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      BitConverter.ToInt32(buffer, offset);
      offset += Row.CounterSize;
      if (this.encryption != null)
        return this.UnformatEncryptedColumns(buffer, offset, precedenceRow);
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        try
        {
          if (!this.NoParticipation(column))
            offset = column.ConvertFromByteArray(buffer, offset, this.GetPrecedenceColumn(precedenceRow, column.RowIndex));
        }
        finally
        {
          column.Edited = false;
        }
      }
      return offset;
    }

    internal void Read(DataStorage storage, Row.RowScope scope, bool oldSpecification)
    {
      bool force = scope == Row.RowScope.Head;
      try
      {
        storage.Handle.ReadRow(storage, this, force ? this.GetHeadLength((Row) null) : this.formatLength, force);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 110, storage.Name);
      }
      if (force)
      {
        this.UnformatHeader();
      }
      else
      {
        this.UnformatRowBuffer();
        this.ReadExtensions(storage, false);
      }
    }

    internal void Write(DataStorage storage, Row.RowScope scope)
    {
      try
      {
        int num = scope == Row.RowScope.Head ? this.FormatHeader() : this.WriteExtensions(storage, false, false) + this.FormatRowBuffer((Row) null);
        storage.Handle.WriteRow(storage, this, !this.alignment || scope == Row.RowScope.Head ? num : this.formatLength);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 111, storage.Name);
      }
    }

    internal int WriteExtensions(DataStorage storage, bool resetMeta, bool allowOptimization)
    {
      if (this.Extensions == null)
        return 0;
      foreach (ExtendedColumn extension in (List<IColumn>) this.Extensions)
      {
        if (resetMeta)
          extension.ResetMetaValue();
        extension.FormatExtension(storage, allowOptimization);
      }
      return 0;
    }

    internal void ReadExtensions(DataStorage storage, bool postpone)
    {
      if (this.Extensions == null)
        return;
      Database wrapperDatabase = storage.WrapperDatabase;
      bool updateMode = wrapperDatabase != null && wrapperDatabase.UpgradeExtensionsMode;
      foreach (ExtendedColumn extension in (List<IColumn>) this.Extensions)
        extension.UnformatExtension(storage, postpone, this, updateMode);
    }

    internal bool FreeExtensionSpace(DataStorage storage)
    {
      if (this.Extensions == null)
        return true;
      foreach (ExtendedColumn extension in (List<IColumn>) this.Extensions)
        extension.FreeSpace(storage);
      return true;
    }

    internal void SetTimestamp(ulong val)
    {
      this.SetTimestamp(val, this.timestampIndex);
    }

    internal void SetTimestamp(ulong val, int index)
    {
      Row.Column column = this[index];
      column.Value = (object) (long) val;
      column.Edited = false;
    }

    internal void SetOriginator(Guid val, int index, bool useAutoValue)
    {
      Row.Column column = this[index];
      if (useAutoValue)
        column.Value = (object) val;
      column.Edited = false;
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      return this.CompareUnmaskedColumns((Row) obj) == 0;
    }

    public new void Clear()
    {
      if (this.extensions != null)
        this.extensions.Clear();
      this.timestampIndex = -1;
      base.Clear();
    }

    public override string ToString()
    {
      StringBuilder stringBuilder = new StringBuilder(this.Count * 10);
      stringBuilder.Append("row#");
      stringBuilder.Append(this.RowId.ToString());
      stringBuilder.Append(" ");
      foreach (IColumn column in (List<Row.Column>) this)
        stringBuilder.Append(":" + (column.Name == null ? column.ToString() : column.Name + "(" + column.ToString() + ")"));
      return stringBuilder.ToString();
    }

    public static int operator -(Row a, Row b)
    {
      return a.CompareTpHidden(b);
    }

    public int AppendColumn(IColumn column)
    {
      if (column == null)
        return -1;
      this.Add((Row.Column) column);
      ((Row.Column) column).RowIndex = this.Count - 1;
      if (column.ExtendedType)
      {
        if (this.extensions == null)
          this.extensions = new RowExtension(this);
        this.extensions.Add(column);
      }
      if (column.Type == VistaDBType.Timestamp && !column.IsSystem)
        this.timestampIndex = column.RowIndex;
      return column.RowIndex;
    }

    public virtual int GetMemoryApartment(Row precedenceRow)
    {
      if (this.encryption != null)
        return this.GetEncryptedMemoryApartment(precedenceRow);
      int headLength = this.GetHeadLength(precedenceRow);
      foreach (Row.Column column in (List<Row.Column>) this)
      {
        if (!this.NoParticipation(column))
          headLength += column.GetBufferLength(this.GetPrecedenceColumn(precedenceRow, column.RowIndex));
      }
      return headLength + Row.CounterSize;
    }

    IColumn IRow.this[int index]
    {
      get
      {
        return (IColumn) this[index];
      }
    }

    uint IRow.RowId
    {
      set
      {
        this.RowId = value;
      }
      get
      {
        return this.RowId;
      }
    }

    IRow IRow.CopyInstance()
    {
      return (IRow) this.CopyInstance();
    }

    IVistaDBColumn IVistaDBRow.this[int index]
    {
      get
      {
        return (IVistaDBColumn) this[index];
      }
    }

    IVistaDBColumn IVistaDBRow.this[string name]
    {
      get
      {
        return (IVistaDBColumn) this.LookForColumn(name);
      }
    }

    long IVistaDBRow.RowId
    {
      get
      {
        return (long) this.RowId;
      }
    }

    int IVistaDBRow.Compare(IVistaDBRow row)
    {
      return this.CompareUnmaskedColumns((Row) row);
    }

    int IVistaDBRow.CompareKey(IVistaDBRow key)
    {
      return this.CompareTpVisible((Row) key);
    }

    void IVistaDBRow.ClearModified()
    {
      this.ClearEditStatus();
    }

    internal enum RowScope
    {
      All,
      Head,
    }

    internal abstract class Column : IColumn, IValue, IVistaDBColumnAttributes, IVistaDBColumn, IVistaDBValue
    {
      private int rowIndex = -1;
      private static Dictionary<Type, VistaDBType> systemTypeMap = new Dictionary<Type, VistaDBType>(32);
      private static VistaDBType[] typeMap;
      private static Row.Column.ArithmeticRank[] rankMap;
      private VistaDBType type;
      protected object val;
      private int bufferLength;
      private bool edited;
      private bool descending;
      private Row.Column.Attributes attributes;

      static Column()
      {
        Row.Column.systemTypeMap.Add(typeof (string), VistaDBType.NText);
        Row.Column.systemTypeMap.Add(typeof (Decimal), VistaDBType.Decimal);
        Row.Column.systemTypeMap.Add(typeof (DateTime), VistaDBType.DateTime);
        Row.Column.systemTypeMap.Add(typeof (Guid), VistaDBType.UniqueIdentifier);
        Row.Column.systemTypeMap.Add(typeof (bool), VistaDBType.Bit);
        Row.Column.systemTypeMap.Add(typeof (byte[]), VistaDBType.Image);
        Row.Column.systemTypeMap.Add(typeof (float), VistaDBType.Real);
        Row.Column.systemTypeMap.Add(typeof (double), VistaDBType.Float);
        Row.Column.systemTypeMap.Add(typeof (byte), VistaDBType.TinyInt);
        Row.Column.systemTypeMap.Add(typeof (short), VistaDBType.SmallInt);
        Row.Column.systemTypeMap.Add(typeof (int), VistaDBType.Int);
        Row.Column.systemTypeMap.Add(typeof (long), VistaDBType.BigInt);
        Row.Column.typeMap = new VistaDBType[32];
        foreach (int index in Enum.GetValues(typeof (VistaDBType)))
        {
          switch (index)
          {
            case -1:
              continue;
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
              Row.Column.typeMap[index] = VistaDBType.NChar;
              continue;
            case 14:
            case 15:
            case 16:
              Row.Column.typeMap[index] = VistaDBType.Decimal;
              continue;
            case 18:
            case 19:
            case 23:
              Row.Column.typeMap[index] = VistaDBType.DateTime;
              continue;
            case 20:
            case 21:
              Row.Column.typeMap[index] = VistaDBType.VarBinary;
              continue;
            case 24:
              Row.Column.typeMap[index] = VistaDBType.BigInt;
              continue;
            default:
              Row.Column.typeMap[index] = (VistaDBType) index;
              continue;
          }
        }
        Row.Column.rankMap = new Row.Column.ArithmeticRank[32];
        foreach (int index in Enum.GetValues(typeof (VistaDBType)))
        {
          switch (index)
          {
            case -1:
              continue;
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.String;
              continue;
            case 8:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Byte;
              continue;
            case 9:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Int16;
              continue;
            case 10:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Int32;
              continue;
            case 11:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Int64;
              continue;
            case 12:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Single;
              continue;
            case 13:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Double;
              continue;
            case 14:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Decimal;
              continue;
            case 15:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Money;
              continue;
            case 16:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.SmallMoney;
              continue;
            case 17:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Bit;
              continue;
            case 18:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.SmallDateTime;
              continue;
            case 19:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.DateTime;
              continue;
            case 22:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.UniqueIdentifier;
              continue;
            case 23:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.SmallDateTime;
              continue;
            default:
              Row.Column.rankMap[index] = Row.Column.ArithmeticRank.Unsupported;
              continue;
          }
        }
      }

      internal Column(object val, VistaDBType type, int bufferLength)
      {
        this.val = val;
        this.type = type;
        this.bufferLength = bufferLength;
      }

      internal Column(Row.Column column)
        : this(column.val, column.type, column.bufferLength)
      {
        this.attributes = column.attributes;
        this.descending = column.descending;
      }

      internal static VistaDBType GetInternalType(VistaDBType type)
      {
        return Row.Column.typeMap[(int) type];
      }

      internal static Row.Column.ArithmeticRank Rank(VistaDBType type)
      {
        return Row.Column.rankMap[(int) type];
      }

      internal static VistaDBType ParseType(string type)
      {
        if (string.IsNullOrEmpty(type))
          return VistaDBType.Unknown;
        object obj = Enum.Parse(typeof (VistaDBType), type, true);
        if (obj == null)
          return VistaDBType.Uninitialized;
        try
        {
          return (VistaDBType) obj;
        }
        catch
        {
          return VistaDBType.Uninitialized;
        }
      }

      internal static string FixName(string name)
      {
        if (name == null || name.Length == 0)
          throw new VistaDBException(280);
        name = name.TrimEnd(char.MinValue, ' ');
        if (name.Length == 0)
          throw new VistaDBException(280);
        if (name[0] == '[')
          name = name.Substring(1);
        if (name.Length == 0)
          throw new VistaDBException(280);
        if (name[name.Length - 1] == ']')
          name = name.Substring(0, name.Length - 1);
        name = name.TrimEnd(char.MinValue, ' ');
        if (name.Length == 0)
          throw new VistaDBException(280);
        return name;
      }

      internal bool Descending
      {
        get
        {
          return this.descending;
        }
        set
        {
          this.descending = value;
        }
      }

      internal int UniqueID
      {
        get
        {
          return this.attributes.ID;
        }
        set
        {
          this.attributes.ID = value;
        }
      }

      internal Row.Column.ArithmeticRank ArithmeticalRank
      {
        get
        {
          return Row.Column.Rank(this.Type);
        }
      }

      internal static VistaDBType VistaDBTypeBySystemType(Type type)
      {
        if (!Row.Column.systemTypeMap.ContainsKey(type))
          return VistaDBType.Unknown;
        return Row.Column.systemTypeMap[type];
      }

      internal virtual object DummyNull
      {
        get
        {
          return (object) null;
        }
      }

      internal virtual int CodePage
      {
        get
        {
          return 0;
        }
      }

      internal virtual bool IsSync
      {
        get
        {
          return false;
        }
      }

      protected void ResetType(VistaDBType type)
      {
        this.type = type;
      }

      internal virtual void AssignAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed)
      {
        if (this.attributes == null)
        {
          this.attributes = new Row.Column.Attributes(name, allowNull, readOnly, encrypted, packed);
        }
        else
        {
          this.attributes.AllowNull = allowNull;
          this.attributes.ReadOnly = readOnly;
          this.attributes.Packed = packed;
          this.attributes.Encrypted = encrypted;
          this.attributes.Name = name;
        }
      }

      internal virtual void AssignAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed, string caption, string description)
      {
        this.AssignAttributes(name, allowNull, readOnly, encrypted, packed);
        this.attributes.Caption = caption;
        this.attributes.Description = description;
      }

      internal Row.Column Duplicate(bool padRight)
      {
        Row.Column column = this.OnDuplicate(padRight);
        column.Edited = this.Edited;
        column.RowIndex = this.RowIndex;
        column.descending = this.descending;
        return column;
      }

      internal void InitTop()
      {
        this.Value = this.descending ? this.MaxValue : this.MinValue;
        this.Edited = false;
      }

      internal void InitBottom()
      {
        this.Value = this.descending ? this.MinValue : this.MaxValue;
        this.Edited = false;
      }

      internal int MinusColumn(Row.Column b)
      {
        long num1 = this.IsNull ? (b.IsNull ? 0L : -1L) : (b.IsNull ? 1L : this.Collate(b));
        int num2 = num1 > 0L ? 1 : (num1 < 0L ? -1 : 0);
        if (!this.descending)
          return num2;
        return -num2;
      }

      internal int MinusColumnTrimmed(Row.Column b)
      {
        long num1 = this.IsNull ? (b.IsNull ? 0L : -1L) : (b.IsNull ? 1L : this.CollateTrimmed(b));
        int num2 = num1 > 0L ? 1 : (num1 < 0L ? -1 : 0);
        if (!this.descending)
          return num2;
        return -num2;
      }

      internal virtual void CreateFullCopy(Row.Column srcColumn)
      {
        this.val = srcColumn.val;
        this.edited = srcColumn.edited;
        this.attributes = srcColumn.attributes;
        this.bufferLength = srcColumn.bufferLength;
        this.descending = srcColumn.descending;
      }

      protected abstract Row.Column OnDuplicate(bool padRight);

      internal abstract int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn);

      internal abstract int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceColumn);

      internal virtual int ReadVarLength(byte[] buffer, int offset, Row.Column precedenceColumn)
      {
        return 0;
      }

      internal virtual int GetBufferLength(Row.Column precedenceColumn)
      {
        if (!this.IsNull)
          return this.bufferLength + this.GetLengthCounterWidth(precedenceColumn);
        return 0;
      }

      internal virtual int GetLengthCounterWidth(Row.Column precedenceColumn)
      {
        return 0;
      }

      protected virtual long Collate(Row.Column col)
      {
        return 0;
      }

      protected virtual long CollateTrimmed(Row.Column col)
      {
        return this.Collate(col);
      }

      protected virtual object NonTrimedValue
      {
        set
        {
          this.Value = value;
        }
      }

      internal virtual string PaddedStringValue
      {
        get
        {
          if (!this.IsNull)
            return this.Value.ToString();
          return string.Empty;
        }
        set
        {
        }
      }

      internal virtual object PaddedValue
      {
        get
        {
          return this.Value;
        }
      }

      internal virtual object DoGetTrimmedValue()
      {
        return this.Value;
      }

      protected virtual Row.Column DoUnaryMinus()
      {
        return this;
      }

      protected virtual Row.Column DoMinus(Row.Column column)
      {
        return this;
      }

      protected virtual Row.Column DoPlus(Row.Column column)
      {
        return this;
      }

      protected virtual Row.Column DoMultiplyBy(Row.Column b)
      {
        return this;
      }

      protected virtual Row.Column DoDivideBy(Row.Column denominator)
      {
        return this;
      }

      protected virtual Row.Column DoGetDividedBy(Row.Column numerator)
      {
        return this;
      }

      protected virtual Row.Column DoModBy(Row.Column denominator)
      {
        return this;
      }

      protected virtual Row.Column DoGetModBy(Row.Column numerator)
      {
        return this;
      }

      protected virtual Row.Column DoBitwiseNot()
      {
        return this;
      }

      protected virtual Row.Column DoBitwiseAnd(Row.Column denominator)
      {
        return this;
      }

      protected virtual Row.Column DoBitwiseOr(Row.Column denominator)
      {
        return this;
      }

      protected virtual Row.Column DoBitwiseXor(Row.Column denominator)
      {
        return this;
      }

      public override bool Equals(object obj)
      {
        if ((object) (obj as Row.Column) != null)
          return this == (Row.Column) obj;
        return false;
      }

      public override int GetHashCode()
      {
        return base.GetHashCode();
      }

      public override string ToString()
      {
        if (!this.IsNull)
          return this.Value.ToString();
        return "<null>";
      }

      public static Row.Column operator -(Row.Column a)
      {
        if (!a.IsNull)
          return a.DoUnaryMinus();
        return a;
      }

      public static Row.Column operator -(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoMinus(a);
        return a.DoMinus(b);
      }

      public static Row.Column operator +(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoPlus(a);
        return a.DoPlus(b);
      }

      public static Row.Column operator *(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoMultiplyBy(a);
        return a.DoMultiplyBy(b);
      }

      public static Row.Column operator /(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoGetDividedBy(a);
        return a.DoDivideBy(b);
      }

      public static Row.Column operator %(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoGetModBy(a);
        return a.DoModBy(b);
      }

      public static Row.Column operator ~(Row.Column a)
      {
        if (!a.IsNull)
          return a.DoBitwiseNot();
        return a;
      }

      public static Row.Column operator &(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoBitwiseAnd(a);
        return a.DoBitwiseAnd(b);
      }

      public static Row.Column operator |(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoBitwiseOr(a);
        return a.DoBitwiseOr(b);
      }

      public static Row.Column operator ^(Row.Column a, Row.Column b)
      {
        if (a.IsNull || b.IsNull)
        {
          Row.Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoBitwiseXor(a);
        return a.DoBitwiseXor(b);
      }

      public static bool operator ==(Row.Column a, Row.Column b)
      {
        if (object.Equals((object) a, (object) b))
          return true;
        if (!object.Equals((object) a, (object) null) && !object.Equals((object) b, (object) null))
          return a.MinusColumn(b) == 0;
        return false;
      }

      public static bool operator !=(Row.Column a, Row.Column b)
      {
        return !(a == b);
      }

      public int RowIndex
      {
        get
        {
          return this.rowIndex;
        }
        set
        {
          this.rowIndex = value;
        }
      }

      public virtual bool Edited
      {
        get
        {
          return this.edited;
        }
        set
        {
          this.edited = value;
        }
      }

      public virtual VistaDBType InternalType
      {
        get
        {
          return this.Type;
        }
      }

      public virtual Type SystemType
      {
        get
        {
          return typeof (object);
        }
      }

      object IValue.Value
      {
        get
        {
          return this.PaddedValue;
        }
        set
        {
          this.Value = value;
        }
      }

      object IValue.TrimmedValue
      {
        get
        {
          return this.DoGetTrimmedValue();
        }
      }

      bool IColumn.Descending
      {
        get
        {
          return this.descending;
        }
      }

      int IColumn.CompareRank(IColumn b)
      {
        return this.ArithmeticalRank - ((Row.Column) b).ArithmeticalRank;
      }

      IColumn IColumn.Clone()
      {
        return (IColumn) this.Duplicate(false);
      }

      public string Name
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.Name;
          return (string) null;
        }
      }

      public VistaDBType Type
      {
        get
        {
          return this.type;
        }
      }

      public virtual object Value
      {
        get
        {
          return this.val;
        }
        set
        {
          this.edited = true;
          this.val = value;
        }
      }

      public virtual bool IsNull
      {
        get
        {
          return this.Value == null;
        }
      }

      public virtual int MaxLength
      {
        get
        {
          return this.bufferLength;
        }
      }

      public virtual object MinValue
      {
        get
        {
          return (object) null;
        }
      }

      public virtual object MaxValue
      {
        get
        {
          return this.MinValue;
        }
      }

      public bool Encrypted
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.Encrypted;
          return false;
        }
      }

      public bool Packed
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.Packed;
          return false;
        }
      }

      public bool AllowNull
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.AllowNull;
          return true;
        }
      }

      public bool ReadOnly
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.ReadOnly;
          return false;
        }
      }

      string IVistaDBColumnAttributes.Caption
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.Caption;
          return (string) null;
        }
        set
        {
          if (this.attributes == null)
            return;
          this.attributes.Caption = value;
        }
      }

      string IVistaDBColumnAttributes.Description
      {
        get
        {
          if (this.attributes != null)
            return this.attributes.Description;
          return (string) null;
        }
        set
        {
          if (this.attributes == null)
            return;
          this.attributes.Description = value;
        }
      }

      int IVistaDBColumnAttributes.CodePage
      {
        get
        {
          return this.CodePage;
        }
      }

      public virtual bool ExtendedType
      {
        get
        {
          return false;
        }
      }

      public virtual bool FixedType
      {
        get
        {
          return true;
        }
      }

      public bool IsSystem
      {
        get
        {
          return this.IsSync;
        }
      }

      int IVistaDBColumn.Compare(IVistaDBColumn column)
      {
        return this.MinusColumnTrimmed((Row.Column) column);
      }

      IVistaDBColumnAttributesDifference IVistaDBColumnAttributes.Compare(IVistaDBColumnAttributes columnAttr)
      {
        return (IVistaDBColumnAttributesDifference) new Row.Column.AttributeDifference(!Database.DatabaseObject.EqualNames(this.Name, columnAttr.Name), this.Type != columnAttr.Type, !this.ExtendedType && !this.FixedType && this.MaxLength != columnAttr.MaxLength, this.RowIndex != columnAttr.RowIndex, this.Encrypted != columnAttr.Encrypted, this.Packed != columnAttr.Packed, this.CodePage != columnAttr.CodePage, string.Compare(((IVistaDBColumnAttributes) this).Description, columnAttr.Description, StringComparison.Ordinal) != 0, string.Compare(((IVistaDBColumnAttributes) this).Caption, columnAttr.Caption, StringComparison.Ordinal) != 0, this.AllowNull != columnAttr.AllowNull, this.ReadOnly != columnAttr.ReadOnly);
      }

      bool IVistaDBColumn.Modified
      {
        get
        {
          return this.Edited;
        }
      }

      int IVistaDBColumnAttributes.UniqueId
      {
        get
        {
          return this.UniqueID;
        }
      }

      internal enum ArithmeticRank
      {
        Unsupported = -1,
        String = 0,
        Bit = 1,
        Byte = 2,
        Int16 = 3,
        Int32 = 4,
        Int64 = 5,
        SmallMoney = 6,
        Money = 7,
        Decimal = 8,
        Single = 9,
        Double = 10, // 0x0000000A
        SmallDateTime = 11, // 0x0000000B
        DateTime = 12, // 0x0000000C
        UniqueIdentifier = 13, // 0x0000000D
      }

      private class Attributes
      {
        internal string Name;
        internal bool AllowNull;
        internal bool ReadOnly;
        internal bool Encrypted;
        internal bool Packed;
        private string caption;
        private string description;
        internal int ID;

        internal Attributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed)
        {
          this.Name = name;
          this.AllowNull = allowNull;
          this.ReadOnly = readOnly;
          this.Encrypted = encrypted;
          this.Packed = packed;
        }

        internal string Caption
        {
          get
          {
            return this.caption;
          }
          set
          {
            this.caption = value == null || value.Length == 0 ? (string) null : value;
          }
        }

        internal string Description
        {
          get
          {
            return this.description;
          }
          set
          {
            this.description = value == null || value.Length == 0 ? (string) null : value;
          }
        }
      }

      private class AttributeDifference : IVistaDBColumnAttributesDifference
      {
        private bool renamed;
        private bool typeChanged;
        private bool lengthChanged;
        private bool orderChagned;
        private bool encryptionChanged;
        private bool packedChanged;
        private bool codePageChanged;
        private bool isNewDescription;
        private bool isNewCaption;
        private bool isNullChanged;
        private bool isReadonlyChanged;

        internal AttributeDifference(bool renamed, bool typeChanged, bool lengthChanged, bool orderChanged, bool encryptionChanged, bool packedChanged, bool codePageChanged, bool isNewDescription, bool isNewCaption, bool isNullChanged, bool isReadonlyChanged)
        {
          this.renamed = renamed;
          this.typeChanged = typeChanged;
          this.lengthChanged = lengthChanged;
          this.orderChagned = orderChanged;
          this.encryptionChanged = encryptionChanged;
          this.packedChanged = packedChanged;
          this.codePageChanged = codePageChanged;
          this.isNewDescription = isNewDescription;
          this.isNewCaption = isNewCaption;
          this.isNullChanged = isNullChanged;
          this.isReadonlyChanged = isReadonlyChanged;
        }

        bool IVistaDBColumnAttributesDifference.IsRenamed
        {
          get
          {
            return this.renamed;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsTypeDiffers
        {
          get
          {
            return this.typeChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsMaxLengthDiffers
        {
          get
          {
            return this.lengthChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsOrderDiffers
        {
          get
          {
            return this.orderChagned;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsEncryptedDiffers
        {
          get
          {
            return this.encryptionChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsPackedDiffers
        {
          get
          {
            return this.packedChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsCodePageDiffers
        {
          get
          {
            return this.codePageChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsDescriptionDiffers
        {
          get
          {
            return this.isNewDescription;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsCaptionDiffers
        {
          get
          {
            return this.isNewCaption;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsNullDiffers
        {
          get
          {
            return this.isNullChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsReadOnlyDiffers
        {
          get
          {
            return this.isReadonlyChanged;
          }
        }
      }
    }

    private class MetaData : IntColumn
    {
      private static readonly int rowReferenceSize = 8;
      internal IntColumn rowID = new IntColumn(0);
      internal ulong referencedPosition = Row.EmptyReference;

      internal MetaData(uint rowId, uint version, ulong referencedPosition)
        : base((int) version)
      {
        this.rowID.Value = (object) (int) rowId;
        this.referencedPosition = referencedPosition;
      }

      internal override int GetBufferLength(Row.Column precedenceColumn)
      {
        return this.rowID.GetBufferLength(precedenceColumn == (Row.Column) null ? (Row.Column) null : (Row.Column) ((Row.MetaData) precedenceColumn).rowID) + ((long) this.referencedPosition == (long) Row.EmptyReference ? 1 : Row.MetaData.rowReferenceSize) + base.GetBufferLength(precedenceColumn);
      }

      internal override int ConvertToByteArray(byte[] buffer, int offset, Row.Column precedenceColumn)
      {
        offset = this.rowID.ConvertToByteArray(buffer, offset, precedenceColumn == (Row.Column) null ? (Row.Column) null : (Row.Column) ((Row.MetaData) precedenceColumn).rowID);
        if ((long) this.referencedPosition == (long) Row.EmptyReference)
        {
          buffer[offset] = (byte) 1;
          ++offset;
        }
        else
          offset = VdbBitConverter.GetBytes(this.referencedPosition, buffer, offset, Row.MetaData.rowReferenceSize);
        return base.ConvertToByteArray(buffer, offset, precedenceColumn);
      }

      internal override int ConvertFromByteArray(byte[] buffer, int offset, Row.Column precedenceMetainfo)
      {
        offset = this.rowID.ConvertFromByteArray(buffer, offset, precedenceMetainfo == (Row.Column) null ? (Row.Column) null : (Row.Column) ((Row.MetaData) precedenceMetainfo).rowID);
        if (buffer[offset] == (byte) 0)
        {
          this.referencedPosition = BitConverter.ToUInt64(buffer, offset);
          offset += Row.MetaData.rowReferenceSize;
        }
        else
        {
          this.referencedPosition = Row.EmptyReference;
          ++offset;
        }
        return base.ConvertFromByteArray(buffer, offset, precedenceMetainfo);
      }
    }
  }
}
