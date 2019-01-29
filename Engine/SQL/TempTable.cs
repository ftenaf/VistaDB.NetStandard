using System.Collections.Generic;
using System.Globalization;
using VistaDB.Engine.Core;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class TempTable : IQueryResult
  {
    protected List<Row> rows;
    protected int rowCount;
    protected Row patternRow;
    protected int curRowIndex;
    protected Row curRow;
    protected IDatabase database;
    protected bool eof;
    protected QueryResultKey[] currentOrder;
    protected bool sparseIndex;
    protected Row patternKey;

    internal TempTable(IDatabase database)
      : this(database, 100)
    {
    }

    internal TempTable(IDatabase database, int capacity)
    {
      this.rows = new List<Row>(capacity);
      this.rowCount = 0;
      this.patternRow = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
      this.curRowIndex = -1;
      this.curRow = (Row) null;
      this.database = database;
      this.eof = true;
      this.currentOrder = (QueryResultKey[]) null;
      this.sparseIndex = false;
      this.patternKey = (Row) null;
    }

    public void AddColumn(string name, VistaDBType dataType)
    {
      this.AddColumn(name, dataType, true);
    }

    public void AddColumn(string name, VistaDBType dataType, int maxLength)
    {
      this.AddColumn(name, dataType, true, maxLength);
    }

    public void AddColumn(string name, VistaDBType dataType, bool allowNull)
    {
      CultureInfo culture;
      bool flag;
      if (this.database != null)
      {
        culture = this.database.Culture;
        flag = this.database.CaseSensitive;
      }
      else
      {
        culture = CultureInfo.InvariantCulture;
        flag = false;
      }
      Row.Column rowColumn = DataStorage.CreateRowColumn(dataType, !flag, culture);
      rowColumn.AssignAttributes(name, allowNull, false, false, false);
      this.patternRow.AppendColumn((IColumn) rowColumn);
    }

    public void AddColumn(string name, VistaDBType dataType, bool allowNull, int maxLength)
    {
      CultureInfo culture;
      bool flag;
      if (this.database != null)
      {
        culture = this.database.Culture;
        flag = this.database.CaseSensitive;
      }
      else
      {
        culture = CultureInfo.InvariantCulture;
        flag = false;
      }
      Row.Column rowColumn = DataStorage.CreateRowColumn(dataType, maxLength, !flag, culture);
      rowColumn.AssignAttributes(name, allowNull, false, false, false);
      this.patternRow.AppendColumn((IColumn) rowColumn);
    }

    public void FinalizeCreate()
    {
      this.patternRow.InstantiateComparingMask();
    }

    private bool CheckSameRow()
    {
      Row row1 = this.rows[this.curRowIndex];
      if (this.patternKey == this.patternRow)
        return row1.Equals((object) this.curRow);
      if (this.curRowIndex == 1)
      {
        int index = 0;
        for (int length = this.currentOrder.Length; index < length; ++index)
          this.patternKey[index].Value = this.curRow[this.currentOrder[index].ColumnIndex].Value;
      }
      Row row2 = this.patternKey.CopyInstance();
      int index1 = 0;
      for (int length = this.currentOrder.Length; index1 < length; ++index1)
        row2[index1].Value = row1[this.currentOrder[index1].ColumnIndex].Value;
      if (row2.Equals((object) this.patternKey))
        return true;
      this.patternKey = row2;
      return false;
    }

    public void Insert()
    {
      this.curRow = this.patternRow.CopyInstance();
      this.rows.Add(this.curRow);
      this.curRowIndex = this.rowCount++;
      this.eof = true;
    }

    public void Delete()
    {
      if (this.rowCount == 0)
        return;
      this.rows.RemoveAt(this.curRowIndex);
      --this.rowCount;
      this.eof = this.rowCount == 0;
      if (this.eof)
      {
        this.curRow = (Row) null;
      }
      else
      {
        if (this.curRowIndex == this.rowCount)
          --this.curRowIndex;
        this.curRow = this.rows[this.curRowIndex];
      }
    }

    public void PutColumn(IColumn column, int index)
    {
      if (this.rowCount == 0)
        return;
      if (column.InternalType != this.curRow[index].InternalType)
        this.database.Conversion.Convert((IValue) column, (IValue) this.curRow[index]);
      else
        this.curRow[index].Value = ((IValue) column).Value;
    }

    public void Post()
    {
    }

    public IRow GetCurrentKeyClone()
    {
      if (this.sparseIndex)
        return (IRow) this.patternKey;
      if (this.patternKey == this.patternRow)
        return (IRow) this.curRow;
      Row row = this.patternKey.CopyInstance();
      int index = 0;
      for (int length = this.currentOrder.Length; index < length; ++index)
        row[index].Value = this.curRow[this.currentOrder[index].ColumnIndex].Value;
      return (IRow) row;
    }

    public IRow GetCurrentRowClone()
    {
      if (this.curRow != null)
        return (IRow) this.curRow.CopyInstance();
      return (IRow) null;
    }

    public void Truncate(long rowLimit)
    {
      if ((long) this.rowCount <= rowLimit)
        return;
      this.rows.RemoveRange((int) rowLimit, this.rowCount - (int) rowLimit);
      this.rowCount = (int) rowLimit;
    }

    public void Sort(QueryResultKey[] sortOrder, bool distinct, bool sparse)
    {
      this.sparseIndex = sparse;
      int[] comparingMask = this.patternRow.ComparingMask;
      if (sortOrder == null)
      {
        int index = 0;
        for (int count = this.patternRow.Count; index < count; ++index)
          comparingMask[index] = index + 1;
        this.patternKey = this.patternRow;
      }
      else
      {
        int index1 = 0;
        for (int length = sortOrder.Length; index1 < length; ++index1)
        {
          comparingMask[index1] = sortOrder[index1].ColumnIndex + 1;
          if (sortOrder[index1].Descending)
            comparingMask[index1] = -comparingMask[index1];
        }
        int length1 = sortOrder.Length;
        for (int count = this.patternRow.Count; length1 < count; ++length1)
          comparingMask[length1] = 0;
        this.patternKey = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
        int index2 = 0;
        for (int length2 = sortOrder.Length; index2 < length2; ++index2)
          this.patternKey.AppendColumn(this.database.CreateEmptyColumn(this.patternRow[sortOrder[index2].ColumnIndex].Type));
      }
      this.currentOrder = sortOrder;
      int expectedKeyLen = 0;
      SortSpool sortSpool = new SortSpool(false, (uint) this.rowCount, ref expectedKeyLen, this.patternRow, new StorageManager(), true);
      for (int index = 0; index < this.rowCount; ++index)
        sortSpool.PushKey(this.rows[index], false);
      sortSpool.Sort();
      if (distinct)
      {
        int index1 = 0;
        for (int index2 = 0; index2 < this.rowCount; ++index2)
        {
          Row row = sortSpool.PopKey();
          if (index2 == 0 || !row.Equals((object) this.rows[index1 - 1]))
          {
            this.rows[index1] = row;
            ++index1;
          }
        }
        if (this.rowCount == index1)
          return;
        this.rowCount = index1;
        this.rows.RemoveRange(this.rowCount, this.rows.Count - this.rowCount);
      }
      else
      {
        for (int index = 0; index < this.rowCount; ++index)
          this.rows[index] = sortSpool.PopKey();
      }
    }

    public Row CurrentRow
    {
      get
      {
        return this.curRow;
      }
      set
      {
        if (this.rowCount == 0)
          return;
        this.rows[this.curRowIndex] = value.CopyInstance();
        this.curRow = this.rows[this.curRowIndex];
      }
    }

    public void FirstRow()
    {
      this.curRowIndex = 0;
      this.eof = this.rowCount == 0;
      if (!this.eof)
        this.curRow = this.rows[this.curRowIndex];
      else
        this.curRow = (Row) null;
    }

    public void NextRow()
    {
      if (this.rowCount == 0 || this.curRowIndex == this.rowCount - 1)
      {
        this.eof = true;
      }
      else
      {
        if (this.sparseIndex)
        {
          int curRowIndex = this.curRowIndex;
          ++this.curRowIndex;
          while (this.CheckSameRow())
          {
            if (this.curRowIndex == this.rowCount - 1)
            {
              this.curRowIndex = curRowIndex;
              this.eof = true;
              return;
            }
            ++this.curRowIndex;
          }
        }
        else
          ++this.curRowIndex;
        this.curRow = this.rows[this.curRowIndex];
      }
    }

    public void Close()
    {
    }

    public object GetValue(int index, VistaDBType dataType)
    {
      if (this.rowCount == 0)
        return (object) null;
      IColumn column = (IColumn) this.curRow[index];
      if (dataType == VistaDBType.Unknown || column.InternalType == dataType)
        return ((IValue) column).Value;
      IColumn emptyColumn = this.database.CreateEmptyColumn(dataType);
      this.database.Conversion.Convert((IValue) column, (IValue) emptyColumn);
      return ((IValue) emptyColumn).Value;
    }

    public IColumn GetColumn(int index)
    {
      if (this.rowCount != 0)
        return (IColumn) this.curRow[index];
      return (IColumn) null;
    }

    public bool IsNull(int index)
    {
      if (this.rowCount != 0)
        return this.curRow[index].IsNull;
      return true;
    }

    public int GetColumnCount()
    {
      return this.patternRow.Count;
    }

    public bool EndOfTable
    {
      get
      {
        return this.eof;
      }
    }

    public long RowCount
    {
      get
      {
        return (long) this.rowCount;
      }
    }
  }
}
