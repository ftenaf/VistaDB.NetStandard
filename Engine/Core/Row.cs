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
    private ulong position = EmptyReference;
    private int timestampIndex = -1;
    private DateTime lastRefresh = DateTime.Now;
    private MetaData metaInfo;
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
      return new Row(rowId, MinVersion, EmptyReference, ascending, encryption, activeMask);
    }

    internal static Row CreateInstance(uint rowId, bool ascending, Encryption encryption, int[] activeMask, int maxColumnCount)
    {
      return new Row(rowId, MinVersion, EmptyReference, ascending, encryption, activeMask, maxColumnCount);
    }

    internal Row CopyInstance()
    {
      return new Row(this);
    }

    protected Row(uint rowId, uint rowVersion, ulong referencedPosition, bool ascending, Encryption encryption, int[] activeMask)
    {
      metaInfo = new MetaData(rowId, rowVersion, referencedPosition);
      this.ascending = ascending;
      this.encryption = encryption;
      activeCompareMask = activeMask;
    }

    protected Row(uint rowId, uint rowVersion, ulong referencedPosition, bool ascending, Encryption encryption, int[] activeMask, int maxColCount)
      : base(maxColCount)
    {
      metaInfo = new MetaData(rowId, rowVersion, referencedPosition);
      this.ascending = ascending;
      this.encryption = encryption;
      activeCompareMask = activeMask;
    }

    private Row(Row row)
      : this(row.RowId, row.RowVersion, row.RefPosition, row.ascending, row.encryption, row.activeCompareMask, row.Count)
    {
      foreach (Column column in (List<Column>) row)
        AppendColumn((IColumn) column.Duplicate(false));
      lastRefresh = row.lastRefresh;
      lastVersion = row.lastVersion;
    }

    private bool NoParticipation(Column column)
    {
      if (column.IsNull)
        return true;
      if (partialRow && activeCompareMask != null)
        return activeCompareMask[column.RowIndex] == 0;
      return false;
    }

    private int GetHeadLength(Row precedenceRow)
    {
      int num1 = CounterSize + metaInfo.GetBufferLength(precedenceRow == null ? (Column) null : (Column) precedenceRow.metaInfo);
      int num2 = Count - 1;
      int num3 = num2 + (8 - num2 % 8);
      return num1 + num3 / 8;
    }

    private int GetEncryptedMemoryApartment(Row precedenceRow)
    {
      int headLength = GetHeadLength(precedenceRow);
      int step = encryption.Step;
      int num = -1;
      foreach (Column column in (List<Column>) this)
      {
        if (!NoParticipation(column))
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
          headLength += column.GetBufferLength(GetPrecedenceColumn(precedenceRow, column.RowIndex));
        }
      }
      if (num >= 0)
        headLength += (step - (headLength - num) % step) % step;
      return headLength + CounterSize;
    }

    internal bool Ascending
    {
      get
      {
        return ascending;
      }
    }

    internal byte[] Buffer
    {
      get
      {
        return buffer;
      }
    }

    internal bool HasTimestamp
    {
      get
      {
        return timestampIndex >= 0;
      }
    }

    internal Column TimeStampColumn
    {
      get
      {
        if (!HasTimestamp)
          return (Column) null;
        return this[timestampIndex];
      }
    }

    internal uint RowId
    {
      get
      {
        return (uint) (int) metaInfo.rowID.Value;
      }
      set
      {
        metaInfo.rowID.Value = (object) (int) value;
      }
    }

    internal uint RowVersion
    {
      get
      {
        return (uint) (int) metaInfo.Value;
      }
      set
      {
        metaInfo.Value = (object) (int) value;
      }
    }

    internal bool OutdatedStatus
    {
      get
      {
        return ((int) RowVersion & int.MinValue) != 0;
      }
      set
      {
        if (value)
          RowVersion |= 2147483648U;
        else
          RowVersion &= (uint) int.MaxValue;
      }
    }

    internal uint TransactionId
    {
      get
      {
        return RowVersion & (uint) int.MaxValue;
      }
    }

    internal ulong Position
    {
      get
      {
        return position;
      }
      set
      {
        position = value;
      }
    }

    internal int FormatLength
    {
      get
      {
        return formatLength;
      }
      set
      {
        if (Buffer == null || Buffer.Length < value)
          AllocateBuffer(value);
        formatLength = value;
      }
    }

    internal ulong RefPosition
    {
      get
      {
        return metaInfo.referencedPosition;
      }
      set
      {
        metaInfo.referencedPosition = value;
      }
    }

    internal bool Alignment
    {
      get
      {
        return alignment;
      }
    }

    internal bool PartialRow
    {
      set
      {
        partialRow = value;
      }
    }

    internal int[] ComparingMask
    {
      get
      {
        return activeCompareMask;
      }
    }

    internal bool EditedExtensions
    {
      get
      {
        if (Extensions == null)
          return false;
        foreach (Column extension in (List<IColumn>) Extensions)
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
        foreach (Column column in (List<Column>) this)
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
        return lastRefresh;
      }
    }

    internal int LastVersion
    {
      get
      {
        return lastVersion;
      }
    }

    internal virtual RowExtension Extensions
    {
      get
      {
        return extensions;
      }
    }

    private void IncrementVersion()
    {
      lastRefresh = DateTime.Now;
      ++lastVersion;
    }

    private int CompareMaskedColumns(Row row)
    {
      int num1 = 0;
      int count = Count;
      for (int index1 = 0; index1 < count && num1 == 0; ++index1)
      {
        int num2 = activeCompareMask[index1];
        if (num2 != 0)
        {
          bool flag = num2 < 0;
          if (flag)
            num2 = -num2;
          int index2 = num2 - 1;
          num1 = !flag ? this[index2].MinusColumn(row[index2]) : row[index2].MinusColumn(this[index2]);
        }
      }
      if (!ascending)
        return -num1;
      return num1;
    }

    private int CompareUnmaskedColumns(Row row)
    {
      int num = 0;
      for (int index = 0; index < Count && num == 0; ++index)
        num = this[index].MinusColumn(row[index]);
      if (!ascending)
        return -num;
      return num;
    }

    private int CompareTpHidden(Row row)
    {
      int num = activeCompareMask == null ? CompareUnmaskedColumns(row) : CompareMaskedColumns(row);
      if (num == 0)
      {
        uint rowId1 = RowId;
        uint rowId2 = row.RowId;
        num = rowId1 > rowId2 ? 1 : (rowId1 < rowId2 ? -1 : 0);
        if (num == 0)
        {
          uint transactionId1 = TransactionId;
          uint transactionId2 = row.TransactionId;
          num = transactionId1 > transactionId2 ? 1 : (transactionId1 < transactionId2 ? -1 : 0);
          if (num == 0)
          {
            bool outdatedStatus1 = OutdatedStatus;
            bool outdatedStatus2 = row.OutdatedStatus;
            num = !outdatedStatus1 || outdatedStatus2 ? (outdatedStatus1 || !outdatedStatus2 ? 0 : -1) : 1;
          }
        }
      }
      return num;
    }

    private int CompareTpVisible(Row row)
    {
      int num = activeCompareMask == null ? CompareUnmaskedColumns(row) : CompareMaskedColumns(row);
      if (num == 0)
      {
        uint rowId1 = RowId;
        uint rowId2 = row.RowId;
        num = rowId1 > rowId2 ? 1 : (rowId1 < rowId2 ? -1 : 0);
      }
      return num;
    }

    private int ConvertNullsToBuffer(byte[] buffer, int offset)
    {
      --offset;
      for (int index = 0; index < Count; ++index)
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
      foreach (Column column in (List<Column>) this)
      {
        int num = column.RowIndex % 8;
        if (num == 0)
          ++offset;
        column.Value = ((int) buffer[offset] & (int) (byte) (1 << num)) == 0 ? column.DummyNull : (object) null;
      }
      return ++offset;
    }

    private new void Add(Column column)
    {
      base.Add(column);
    }

    private int FormatHeader()
    {
      return FormatHeader(buffer, 0, (Row) null);
    }

    private int FormatHeader(byte[] buffer, int offset, Row precedenceRow)
    {
      int dstOffset = offset;
      offset += CounterSize;
      offset = metaInfo.ConvertToByteArray(buffer, offset, precedenceRow == null ? (Column) null : (Column) precedenceRow.metaInfo);
      int num = (Count - 1) / 8;
      offset = ConvertNullsToBuffer(buffer, offset);
      System.Buffer.BlockCopy((Array) BitConverter.GetBytes(offset - dstOffset), 0, (Array) buffer, dstOffset, CounterSize);
      IncrementVersion();
      return offset;
    }

    private void UnformatHeader()
    {
      UnformatHeader(buffer, 0, true, (Row) null);
    }

    private int UnformatHeader(byte[] buffer, int offset, bool bypassNulls, Row precedenceRow)
    {
      int num = BitConverter.ToInt32(buffer, offset) + offset;
      offset += CounterSize;
      offset = metaInfo.ConvertFromByteArray(buffer, offset, precedenceRow == null ? (Column) null : (Column) precedenceRow.metaInfo);
      if (bypassNulls)
        return offset;
      offset = ConvertNullsFromBuffer(buffer, offset);
      IncrementVersion();
      return num;
    }

    private int FormatEncryptedColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      int step = encryption.Step;
      int offset1 = -1;
      foreach (Column column in (List<Column>) this)
      {
        try
        {
          if (!NoParticipation(column))
          {
            if (column.Encrypted)
            {
              if (offset1 < 0)
                offset1 = offset;
            }
            else if (offset1 >= 0)
            {
              offset += (step - (offset - offset1) % step) % step;
              encryption.Encrypt(buffer, offset1, offset - offset1);
              offset1 = -1;
            }
            offset = column.ConvertToByteArray(buffer, offset, GetPrecedenceColumn(precedenceRow, column.RowIndex));
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
        encryption.Encrypt(buffer, offset1, offset - offset1);
      }
      return offset;
    }

    private int UnformatEncryptedColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      int step = encryption.Step;
      int num1 = 0;
      foreach (Column column in (List<Column>) this)
      {
        try
        {
          if (!NoParticipation(column))
          {
                        Column precedenceColumn = GetPrecedenceColumn(precedenceRow, column.RowIndex);
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
                  encryption.Decrypt(buffer, offset1, num5 + num1);
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

    private Column GetPrecedenceColumn(Row precedenceRow, int index)
    {
      if (precedenceRow == null)
        return (Column) null;
            Column column = precedenceRow[index];
      if (!column.IsNull)
        return column;
      return (Column) null;
    }

    protected void AllocateBuffer(int length)
    {
      buffer = new byte[length];
    }

    internal int FormatRowBuffer(byte[] buffer, int offset, Row precedenceRow)
    {
      return FormatColumns(buffer, FormatHeader(buffer, offset, precedenceRow), precedenceRow);
    }

    private int FormatRowBuffer(Row precedenceRow)
    {
      return FormatRowBuffer(buffer, 0, precedenceRow);
    }

    internal int UnformatRowBuffer(byte[] buffer, int offset, Row precedenceRow)
    {
      return UnformatColumns(buffer, UnformatHeader(buffer, offset, false, precedenceRow), precedenceRow);
    }

    protected int UnformatRowBuffer()
    {
      return UnformatRowBuffer(buffer, 0, (Row) null);
    }

    internal void SyncPartialRow()
    {
      if (activeCompareMask == null)
        return;
      foreach (Column column in (List<Column>) this)
      {
        if (activeCompareMask[column.RowIndex] == 0)
          column.Edited = false;
      }
    }

    internal void Copy(Row sourceRow)
    {
      CopyData(sourceRow);
      CopyMetaData(sourceRow);
    }

    private void CopyData(Row sourceRow)
    {
      if (this == sourceRow)
        return;
      for (int index = 0; index < Count; ++index)
        this[index].CreateFullCopy(sourceRow[index]);
    }

    internal void CopyMetaData(Row sourceRow)
    {
      if (this == sourceRow)
        return;
      RowId = sourceRow.RowId;
      RowVersion = sourceRow.RowVersion;
      RefPosition = sourceRow.RefPosition;
      ascending = sourceRow.ascending;
    }

    internal Column LookForColumn(char[] buffer, int offset, bool containSpaces)
    {
      bool firstPosition = false;
      foreach (Column column in (List<Column>) this)
      {
        int length = column.Name.Length;
        if (length <= buffer.Length - offset && Database.DatabaseObject.EqualNames(new string(buffer, offset, length), column.Name))
        {
          int index = offset + length;
          if (index >= buffer.Length || !DirectConnection.IsCorrectNameSymbol(buffer[index], ref firstPosition, containSpaces))
            return column;
        }
      }
      return (Column) null;
    }

    internal Column LookForColumn(string name)
    {
      foreach (Column column in (List<Column>) this)
      {
        if (Database.DatabaseObject.EqualNames(name, column.Name))
          return column;
      }
      return (Column) null;
    }

    public void InitTop()
    {
      foreach (Column column in (List<Column>) this)
        column.InitTop();
      RowId = MinRowId;
      RowVersion = MinVersion;
      RefPosition = EmptyReference;
      Position = EmptyReference;
    }

    public void InitBottom()
    {
      foreach (Column column in (List<Column>) this)
        column.InitBottom();
      RowId = MaxRowId;
      RowVersion = MaxVersion;
      RefPosition = EmptyReference;
      Position = EmptyReference;
    }

    internal bool EqualColumns(Row row, bool masked)
    {
      if (!masked)
        return CompareUnmaskedColumns(row) == 0;
      return CompareMaskedColumns(row) == 0;
    }

    internal void ClearEditStatus()
    {
      foreach (Column column in (List<Column>) this)
        column.Edited = false;
    }

    internal void ReorderByIndex()
    {
      foreach (Column column in GetRange(0, Count))
      {
        this[column.RowIndex] = column;
        if (column.Type == VistaDBType.Timestamp)
          timestampIndex = column.RowIndex;
      }
    }

    internal void InstantiateComparingMask()
    {
      activeCompareMask = new int[Count];
    }

    internal int FormatColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      offset += CounterSize;
      int num = offset;
      if (encryption != null)
      {
        offset = FormatEncryptedColumns(buffer, offset, precedenceRow);
      }
      else
      {
        foreach (Column column in (List<Column>) this)
        {
          try
          {
            if (!NoParticipation(column))
              offset = column.ConvertToByteArray(buffer, offset, GetPrecedenceColumn(precedenceRow, column.RowIndex));
          }
          finally
          {
            column.Edited = false;
          }
        }
      }
      VdbBitConverter.GetBytes((uint) (offset - num), buffer, num - CounterSize, CounterSize);
      return offset;
    }

    internal int UnformatColumns(byte[] buffer, int offset, Row precedenceRow)
    {
      BitConverter.ToInt32(buffer, offset);
      offset += CounterSize;
      if (encryption != null)
        return UnformatEncryptedColumns(buffer, offset, precedenceRow);
      foreach (Column column in (List<Column>) this)
      {
        try
        {
          if (!NoParticipation(column))
            offset = column.ConvertFromByteArray(buffer, offset, GetPrecedenceColumn(precedenceRow, column.RowIndex));
        }
        finally
        {
          column.Edited = false;
        }
      }
      return offset;
    }

    internal void Read(DataStorage storage, RowScope scope, bool oldSpecification)
    {
      bool force = scope == RowScope.Head;
      try
      {
        storage.Handle.ReadRow(storage, this, force ? GetHeadLength((Row) null) : formatLength, force);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 110, storage.Name);
      }
      if (force)
      {
        UnformatHeader();
      }
      else
      {
        UnformatRowBuffer();
        ReadExtensions(storage, false);
      }
    }

    internal void Write(DataStorage storage, RowScope scope)
    {
      try
      {
        int num = scope == RowScope.Head ? FormatHeader() : WriteExtensions(storage, false, false) + FormatRowBuffer((Row) null);
        storage.Handle.WriteRow(storage, this, !alignment || scope == RowScope.Head ? num : formatLength);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 111, storage.Name);
      }
    }

    internal int WriteExtensions(DataStorage storage, bool resetMeta, bool allowOptimization)
    {
      if (Extensions == null)
        return 0;
      foreach (ExtendedColumn extension in (List<IColumn>) Extensions)
      {
        if (resetMeta)
          extension.ResetMetaValue();
        extension.FormatExtension(storage, allowOptimization);
      }
      return 0;
    }

    internal void ReadExtensions(DataStorage storage, bool postpone)
    {
      if (Extensions == null)
        return;
      Database wrapperDatabase = storage.WrapperDatabase;
      bool updateMode = wrapperDatabase != null && wrapperDatabase.UpgradeExtensionsMode;
      foreach (ExtendedColumn extension in (List<IColumn>) Extensions)
        extension.UnformatExtension(storage, postpone, this, updateMode);
    }

    internal bool FreeExtensionSpace(DataStorage storage)
    {
      if (Extensions == null)
        return true;
      foreach (ExtendedColumn extension in (List<IColumn>) Extensions)
        extension.FreeSpace(storage);
      return true;
    }

    internal void SetTimestamp(ulong val)
    {
      SetTimestamp(val, timestampIndex);
    }

    internal void SetTimestamp(ulong val, int index)
    {
            Column column = this[index];
      column.Value = (object) (long) val;
      column.Edited = false;
    }

    internal void SetOriginator(Guid val, int index, bool useAutoValue)
    {
            Column column = this[index];
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
      return CompareUnmaskedColumns((Row) obj) == 0;
    }

    public new void Clear()
    {
      if (extensions != null)
        extensions.Clear();
      timestampIndex = -1;
      base.Clear();
    }

    public override string ToString()
    {
      StringBuilder stringBuilder = new StringBuilder(Count * 10);
      stringBuilder.Append("row#");
      stringBuilder.Append(RowId.ToString());
      stringBuilder.Append(" ");
      foreach (IColumn column in (List<Column>) this)
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
      Add((Column) column);
      ((Column) column).RowIndex = Count - 1;
      if (column.ExtendedType)
      {
        if (extensions == null)
          extensions = new RowExtension(this);
        extensions.Add(column);
      }
      if (column.Type == VistaDBType.Timestamp && !column.IsSystem)
        timestampIndex = column.RowIndex;
      return column.RowIndex;
    }

    public virtual int GetMemoryApartment(Row precedenceRow)
    {
      if (encryption != null)
        return GetEncryptedMemoryApartment(precedenceRow);
      int headLength = GetHeadLength(precedenceRow);
      foreach (Column column in (List<Column>) this)
      {
        if (!NoParticipation(column))
          headLength += column.GetBufferLength(GetPrecedenceColumn(precedenceRow, column.RowIndex));
      }
      return headLength + CounterSize;
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
        RowId = value;
      }
      get
      {
        return RowId;
      }
    }

    IRow IRow.CopyInstance()
    {
      return (IRow) CopyInstance();
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
        return (IVistaDBColumn) LookForColumn(name);
      }
    }

    long IVistaDBRow.RowId
    {
      get
      {
        return (long) RowId;
      }
    }

    int IVistaDBRow.Compare(IVistaDBRow row)
    {
      return CompareUnmaskedColumns((Row) row);
    }

    int IVistaDBRow.CompareKey(IVistaDBRow key)
    {
      return CompareTpVisible((Row) key);
    }

    void IVistaDBRow.ClearModified()
    {
      ClearEditStatus();
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
      private static ArithmeticRank[] rankMap;
      private VistaDBType type;
      protected object val;
      private int bufferLength;
      private bool edited;
      private bool descending;
      private Attributes attributes;

      static Column()
      {
                systemTypeMap.Add(typeof (string), VistaDBType.NText);
                systemTypeMap.Add(typeof (Decimal), VistaDBType.Decimal);
                systemTypeMap.Add(typeof (DateTime), VistaDBType.DateTime);
                systemTypeMap.Add(typeof (Guid), VistaDBType.UniqueIdentifier);
                systemTypeMap.Add(typeof (bool), VistaDBType.Bit);
                systemTypeMap.Add(typeof (byte[]), VistaDBType.Image);
                systemTypeMap.Add(typeof (float), VistaDBType.Real);
                systemTypeMap.Add(typeof (double), VistaDBType.Float);
                systemTypeMap.Add(typeof (byte), VistaDBType.TinyInt);
                systemTypeMap.Add(typeof (short), VistaDBType.SmallInt);
                systemTypeMap.Add(typeof (int), VistaDBType.Int);
                systemTypeMap.Add(typeof (long), VistaDBType.BigInt);
                typeMap = new VistaDBType[32];
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
                            typeMap[index] = VistaDBType.NChar;
              continue;
            case 14:
            case 15:
            case 16:
                            typeMap[index] = VistaDBType.Decimal;
              continue;
            case 18:
            case 19:
            case 23:
                            typeMap[index] = VistaDBType.DateTime;
              continue;
            case 20:
            case 21:
                            typeMap[index] = VistaDBType.VarBinary;
              continue;
            case 24:
                            typeMap[index] = VistaDBType.BigInt;
              continue;
            default:
                            typeMap[index] = (VistaDBType) index;
              continue;
          }
        }
                rankMap = new ArithmeticRank[32];
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
                            rankMap[index] = ArithmeticRank.String;
              continue;
            case 8:
                            rankMap[index] = ArithmeticRank.Byte;
              continue;
            case 9:
                            rankMap[index] = ArithmeticRank.Int16;
              continue;
            case 10:
                            rankMap[index] = ArithmeticRank.Int32;
              continue;
            case 11:
                            rankMap[index] = ArithmeticRank.Int64;
              continue;
            case 12:
                            rankMap[index] = ArithmeticRank.Single;
              continue;
            case 13:
                            rankMap[index] = ArithmeticRank.Double;
              continue;
            case 14:
                            rankMap[index] = ArithmeticRank.Decimal;
              continue;
            case 15:
                            rankMap[index] = ArithmeticRank.Money;
              continue;
            case 16:
                            rankMap[index] = ArithmeticRank.SmallMoney;
              continue;
            case 17:
                            rankMap[index] = ArithmeticRank.Bit;
              continue;
            case 18:
                            rankMap[index] = ArithmeticRank.SmallDateTime;
              continue;
            case 19:
                            rankMap[index] = ArithmeticRank.DateTime;
              continue;
            case 22:
                            rankMap[index] = ArithmeticRank.UniqueIdentifier;
              continue;
            case 23:
                            rankMap[index] = ArithmeticRank.SmallDateTime;
              continue;
            default:
                            rankMap[index] = ArithmeticRank.Unsupported;
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

      internal Column(Column column)
        : this(column.val, column.type, column.bufferLength)
      {
        attributes = column.attributes;
        descending = column.descending;
      }

      internal static VistaDBType GetInternalType(VistaDBType type)
      {
        return typeMap[(int) type];
      }

      internal static ArithmeticRank Rank(VistaDBType type)
      {
        return rankMap[(int) type];
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
          return descending;
        }
        set
        {
          descending = value;
        }
      }

      internal int UniqueID
      {
        get
        {
          return attributes.ID;
        }
        set
        {
          attributes.ID = value;
        }
      }

      internal ArithmeticRank ArithmeticalRank
      {
        get
        {
          return Rank(Type);
        }
      }

      internal static VistaDBType VistaDBTypeBySystemType(Type type)
      {
        if (!systemTypeMap.ContainsKey(type))
          return VistaDBType.Unknown;
        return systemTypeMap[type];
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
        if (attributes == null)
        {
          attributes = new Attributes(name, allowNull, readOnly, encrypted, packed);
        }
        else
        {
          attributes.AllowNull = allowNull;
          attributes.ReadOnly = readOnly;
          attributes.Packed = packed;
          attributes.Encrypted = encrypted;
          attributes.Name = name;
        }
      }

      internal virtual void AssignAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed, string caption, string description)
      {
        AssignAttributes(name, allowNull, readOnly, encrypted, packed);
        attributes.Caption = caption;
        attributes.Description = description;
      }

      internal Column Duplicate(bool padRight)
      {
                Column column = OnDuplicate(padRight);
        column.Edited = Edited;
        column.RowIndex = RowIndex;
        column.descending = descending;
        return column;
      }

      internal void InitTop()
      {
        Value = descending ? MaxValue : MinValue;
        Edited = false;
      }

      internal void InitBottom()
      {
        Value = descending ? MinValue : MaxValue;
        Edited = false;
      }

      internal int MinusColumn(Column b)
      {
        long num1 = IsNull ? (b.IsNull ? 0L : -1L) : (b.IsNull ? 1L : Collate(b));
        int num2 = num1 > 0L ? 1 : (num1 < 0L ? -1 : 0);
        if (!descending)
          return num2;
        return -num2;
      }

      internal int MinusColumnTrimmed(Column b)
      {
        long num1 = IsNull ? (b.IsNull ? 0L : -1L) : (b.IsNull ? 1L : CollateTrimmed(b));
        int num2 = num1 > 0L ? 1 : (num1 < 0L ? -1 : 0);
        if (!descending)
          return num2;
        return -num2;
      }

      internal virtual void CreateFullCopy(Column srcColumn)
      {
        val = srcColumn.val;
        edited = srcColumn.edited;
        attributes = srcColumn.attributes;
        bufferLength = srcColumn.bufferLength;
        descending = srcColumn.descending;
      }

      protected abstract Column OnDuplicate(bool padRight);

      internal abstract int ConvertToByteArray(byte[] buffer, int offset, Column precedenceColumn);

      internal abstract int ConvertFromByteArray(byte[] buffer, int offset, Column precedenceColumn);

      internal virtual int ReadVarLength(byte[] buffer, int offset, Column precedenceColumn)
      {
        return 0;
      }

      internal virtual int GetBufferLength(Column precedenceColumn)
      {
        if (!IsNull)
          return bufferLength + GetLengthCounterWidth(precedenceColumn);
        return 0;
      }

      internal virtual int GetLengthCounterWidth(Column precedenceColumn)
      {
        return 0;
      }

      protected virtual long Collate(Column col)
      {
        return 0;
      }

      protected virtual long CollateTrimmed(Column col)
      {
        return Collate(col);
      }

      protected virtual object NonTrimedValue
      {
        set
        {
          Value = value;
        }
      }

      internal virtual string PaddedStringValue
      {
        get
        {
          if (!IsNull)
            return Value.ToString();
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
          return Value;
        }
      }

      internal virtual object DoGetTrimmedValue()
      {
        return Value;
      }

      protected virtual Column DoUnaryMinus()
      {
        return this;
      }

      protected virtual Column DoMinus(Column column)
      {
        return this;
      }

      protected virtual Column DoPlus(Column column)
      {
        return this;
      }

      protected virtual Column DoMultiplyBy(Column b)
      {
        return this;
      }

      protected virtual Column DoDivideBy(Column denominator)
      {
        return this;
      }

      protected virtual Column DoGetDividedBy(Column numerator)
      {
        return this;
      }

      protected virtual Column DoModBy(Column denominator)
      {
        return this;
      }

      protected virtual Column DoGetModBy(Column numerator)
      {
        return this;
      }

      protected virtual Column DoBitwiseNot()
      {
        return this;
      }

      protected virtual Column DoBitwiseAnd(Column denominator)
      {
        return this;
      }

      protected virtual Column DoBitwiseOr(Column denominator)
      {
        return this;
      }

      protected virtual Column DoBitwiseXor(Column denominator)
      {
        return this;
      }

      public override bool Equals(object obj)
      {
        if ((object) (obj as Column) != null)
          return this == (Column) obj;
        return false;
      }

      public override int GetHashCode()
      {
        return base.GetHashCode();
      }

      public override string ToString()
      {
        if (!IsNull)
          return Value.ToString();
        return "<null>";
      }

      public static Column operator -(Column a)
      {
        if (!a.IsNull)
          return a.DoUnaryMinus();
        return a;
      }

      public static Column operator -(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoMinus(a);
        return a.DoMinus(b);
      }

      public static Column operator +(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoPlus(a);
        return a.DoPlus(b);
      }

      public static Column operator *(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoMultiplyBy(a);
        return a.DoMultiplyBy(b);
      }

      public static Column operator /(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoGetDividedBy(a);
        return a.DoDivideBy(b);
      }

      public static Column operator %(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoGetModBy(a);
        return a.DoModBy(b);
      }

      public static Column operator ~(Column a)
      {
        if (!a.IsNull)
          return a.DoBitwiseNot();
        return a;
      }

      public static Column operator &(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoBitwiseAnd(a);
        return a.DoBitwiseAnd(b);
      }

      public static Column operator |(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoBitwiseOr(a);
        return a.DoBitwiseOr(b);
      }

      public static Column operator ^(Column a, Column b)
      {
        if (a.IsNull || b.IsNull)
        {
                    Column column = a.ArithmeticalRank >= b.ArithmeticalRank ? a : b;
          column.Value = (object) null;
          return column;
        }
        if (a.ArithmeticalRank < b.ArithmeticalRank)
          return b.DoBitwiseXor(a);
        return a.DoBitwiseXor(b);
      }

      public static bool operator ==(Column a, Column b)
      {
        if (Equals((object) a, (object) b))
          return true;
        if (!Equals((object) a, (object) null) && !Equals((object) b, (object) null))
          return a.MinusColumn(b) == 0;
        return false;
      }

      public static bool operator !=(Column a, Column b)
      {
        return !(a == b);
      }

      public int RowIndex
      {
        get
        {
          return rowIndex;
        }
        set
        {
          rowIndex = value;
        }
      }

      public virtual bool Edited
      {
        get
        {
          return edited;
        }
        set
        {
          edited = value;
        }
      }

      public virtual VistaDBType InternalType
      {
        get
        {
          return Type;
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
          return PaddedValue;
        }
        set
        {
          Value = value;
        }
      }

      object IValue.TrimmedValue
      {
        get
        {
          return DoGetTrimmedValue();
        }
      }

      bool IColumn.Descending
      {
        get
        {
          return descending;
        }
      }

      int IColumn.CompareRank(IColumn b)
      {
        return ArithmeticalRank - ((Column) b).ArithmeticalRank;
      }

      IColumn IColumn.Clone()
      {
        return (IColumn) Duplicate(false);
      }

      public string Name
      {
        get
        {
          if (attributes != null)
            return attributes.Name;
          return (string) null;
        }
      }

      public VistaDBType Type
      {
        get
        {
          return type;
        }
      }

      public virtual object Value
      {
        get
        {
          return val;
        }
        set
        {
          edited = true;
          val = value;
        }
      }

      public virtual bool IsNull
      {
        get
        {
          return Value == null;
        }
      }

      public virtual int MaxLength
      {
        get
        {
          return bufferLength;
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
          return MinValue;
        }
      }

      public bool Encrypted
      {
        get
        {
          if (attributes != null)
            return attributes.Encrypted;
          return false;
        }
      }

      public bool Packed
      {
        get
        {
          if (attributes != null)
            return attributes.Packed;
          return false;
        }
      }

      public bool AllowNull
      {
        get
        {
          if (attributes != null)
            return attributes.AllowNull;
          return true;
        }
      }

      public bool ReadOnly
      {
        get
        {
          if (attributes != null)
            return attributes.ReadOnly;
          return false;
        }
      }

      string IVistaDBColumnAttributes.Caption
      {
        get
        {
          if (attributes != null)
            return attributes.Caption;
          return (string) null;
        }
        set
        {
          if (attributes == null)
            return;
          attributes.Caption = value;
        }
      }

      string IVistaDBColumnAttributes.Description
      {
        get
        {
          if (attributes != null)
            return attributes.Description;
          return (string) null;
        }
        set
        {
          if (attributes == null)
            return;
          attributes.Description = value;
        }
      }

      int IVistaDBColumnAttributes.CodePage
      {
        get
        {
          return CodePage;
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
          return IsSync;
        }
      }

      int IVistaDBColumn.Compare(IVistaDBColumn column)
      {
        return MinusColumnTrimmed((Column) column);
      }

      IVistaDBColumnAttributesDifference IVistaDBColumnAttributes.Compare(IVistaDBColumnAttributes columnAttr)
      {
        return (IVistaDBColumnAttributesDifference) new AttributeDifference(!Database.DatabaseObject.EqualNames(Name, columnAttr.Name), Type != columnAttr.Type, !ExtendedType && !FixedType && MaxLength != columnAttr.MaxLength, RowIndex != columnAttr.RowIndex, Encrypted != columnAttr.Encrypted, Packed != columnAttr.Packed, CodePage != columnAttr.CodePage, string.Compare(((IVistaDBColumnAttributes) this).Description, columnAttr.Description, StringComparison.Ordinal) != 0, string.Compare(((IVistaDBColumnAttributes) this).Description, columnAttr.Description, StringComparison.Ordinal) != 0, AllowNull != columnAttr.AllowNull, ReadOnly != columnAttr.ReadOnly);
      }

      bool IVistaDBColumn.Modified
      {
        get
        {
          return Edited;
        }
      }

      int IVistaDBColumnAttributes.UniqueId
      {
        get
        {
          return UniqueID;
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
          Name = name;
          AllowNull = allowNull;
          ReadOnly = readOnly;
          Encrypted = encrypted;
          Packed = packed;
        }

        internal string Caption
        {
          get
          {
            return caption;
          }
          set
          {
            caption = value == null || value.Length == 0 ? (string) null : value;
          }
        }

        internal string Description
        {
          get
          {
            return description;
          }
          set
          {
            description = value == null || value.Length == 0 ? (string) null : value;
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
          orderChagned = orderChanged;
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
            return renamed;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsTypeDiffers
        {
          get
          {
            return typeChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsMaxLengthDiffers
        {
          get
          {
            return lengthChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsOrderDiffers
        {
          get
          {
            return orderChagned;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsEncryptedDiffers
        {
          get
          {
            return encryptionChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsPackedDiffers
        {
          get
          {
            return packedChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsCodePageDiffers
        {
          get
          {
            return codePageChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsDescriptionDiffers
        {
          get
          {
            return isNewDescription;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsCaptionDiffers
        {
          get
          {
            return isNewCaption;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsNullDiffers
        {
          get
          {
            return isNullChanged;
          }
        }

        bool IVistaDBColumnAttributesDifference.IsReadOnlyDiffers
        {
          get
          {
            return isReadonlyChanged;
          }
        }
      }
    }

    private class MetaData : IntColumn
    {
      private static readonly int rowReferenceSize = 8;
      internal IntColumn rowID = new IntColumn(0);
      internal ulong referencedPosition = EmptyReference;

      internal MetaData(uint rowId, uint version, ulong referencedPosition)
        : base((int) version)
      {
        rowID.Value = (object) (int) rowId;
        this.referencedPosition = referencedPosition;
      }

      internal override int GetBufferLength(Column precedenceColumn)
      {
        return rowID.GetBufferLength(precedenceColumn == (Column) null ? (Column) null : (Column) ((MetaData) precedenceColumn).rowID) + ((long) referencedPosition == (long)EmptyReference ? 1 : rowReferenceSize) + base.GetBufferLength(precedenceColumn);
      }

      internal override int ConvertToByteArray(byte[] buffer, int offset, Column precedenceColumn)
      {
        offset = rowID.ConvertToByteArray(buffer, offset, precedenceColumn == (Column) null ? (Column) null : (Column) ((MetaData) precedenceColumn).rowID);
        if ((long) referencedPosition == (long)EmptyReference)
        {
          buffer[offset] = (byte) 1;
          ++offset;
        }
        else
          offset = VdbBitConverter.GetBytes(referencedPosition, buffer, offset, rowReferenceSize);
        return base.ConvertToByteArray(buffer, offset, precedenceColumn);
      }

      internal override int ConvertFromByteArray(byte[] buffer, int offset, Column precedenceMetainfo)
      {
        offset = rowID.ConvertFromByteArray(buffer, offset, precedenceMetainfo == (Column) null ? (Column) null : (Column) ((MetaData) precedenceMetainfo).rowID);
        if (buffer[offset] == (byte) 0)
        {
          referencedPosition = BitConverter.ToUInt64(buffer, offset);
          offset += rowReferenceSize;
        }
        else
        {
          referencedPosition = EmptyReference;
          ++offset;
        }
        return base.ConvertFromByteArray(buffer, offset, precedenceMetainfo);
      }
    }
  }
}
