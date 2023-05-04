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
      rows = new List<Row>(capacity);
      rowCount = 0;
      patternRow = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
      curRowIndex = -1;
      curRow = (Row) null;
      this.database = database;
      eof = true;
      currentOrder = (QueryResultKey[]) null;
      sparseIndex = false;
      patternKey = (Row) null;
    }

    public void AddColumn(string name, VistaDBType dataType)
    {
      AddColumn(name, dataType, true);
    }

    public void AddColumn(string name, VistaDBType dataType, int maxLength)
    {
      AddColumn(name, dataType, true, maxLength);
    }

    public void AddColumn(string name, VistaDBType dataType, bool allowNull)
    {
      CultureInfo culture;
      bool flag;
      if (database != null)
      {
        culture = database.Culture;
        flag = database.CaseSensitive;
      }
      else
      {
        culture = CultureInfo.InvariantCulture;
        flag = false;
      }
      Row.Column rowColumn = DataStorage.CreateRowColumn(dataType, !flag, culture);
      rowColumn.AssignAttributes(name, allowNull, false, false, false);
      patternRow.AppendColumn((IColumn) rowColumn);
    }

    public void AddColumn(string name, VistaDBType dataType, bool allowNull, int maxLength)
    {
      CultureInfo culture;
      bool flag;
      if (database != null)
      {
        culture = database.Culture;
        flag = database.CaseSensitive;
      }
      else
      {
        culture = CultureInfo.InvariantCulture;
        flag = false;
      }
      Row.Column rowColumn = DataStorage.CreateRowColumn(dataType, maxLength, !flag, culture);
      rowColumn.AssignAttributes(name, allowNull, false, false, false);
      patternRow.AppendColumn((IColumn) rowColumn);
    }

    public void FinalizeCreate()
    {
      patternRow.InstantiateComparingMask();
    }

    private bool CheckSameRow()
    {
      Row row1 = rows[curRowIndex];
      if (patternKey == patternRow)
        return row1.Equals((object) curRow);
      if (curRowIndex == 1)
      {
        int index = 0;
        for (int length = currentOrder.Length; index < length; ++index)
          patternKey[index].Value = curRow[currentOrder[index].ColumnIndex].Value;
      }
      Row row2 = patternKey.CopyInstance();
      int index1 = 0;
      for (int length = currentOrder.Length; index1 < length; ++index1)
        row2[index1].Value = row1[currentOrder[index1].ColumnIndex].Value;
      if (row2.Equals((object) patternKey))
        return true;
      patternKey = row2;
      return false;
    }

    public void Insert()
    {
      curRow = patternRow.CopyInstance();
      rows.Add(curRow);
      curRowIndex = rowCount++;
      eof = true;
    }

    public void Delete()
    {
      if (rowCount == 0)
        return;
      rows.RemoveAt(curRowIndex);
      --rowCount;
      eof = rowCount == 0;
      if (eof)
      {
        curRow = (Row) null;
      }
      else
      {
        if (curRowIndex == rowCount)
          --curRowIndex;
        curRow = rows[curRowIndex];
      }
    }

    public void PutColumn(IColumn column, int index)
    {
      if (rowCount == 0)
        return;
      if (column.InternalType != curRow[index].InternalType)
        database.Conversion.Convert((IValue) column, (IValue) curRow[index]);
      else
        curRow[index].Value = ((IValue) column).Value;
    }

    public void Post()
    {
    }

    public IRow GetCurrentKeyClone()
    {
      if (sparseIndex)
        return (IRow) patternKey;
      if (patternKey == patternRow)
        return (IRow) curRow;
      Row row = patternKey.CopyInstance();
      int index = 0;
      for (int length = currentOrder.Length; index < length; ++index)
        row[index].Value = curRow[currentOrder[index].ColumnIndex].Value;
      return (IRow) row;
    }

    public IRow GetCurrentRowClone()
    {
      if (curRow != null)
        return (IRow) curRow.CopyInstance();
      return (IRow) null;
    }

    public void Truncate(long rowLimit)
    {
      if ((long) rowCount <= rowLimit)
        return;
      rows.RemoveRange((int) rowLimit, rowCount - (int) rowLimit);
      rowCount = (int) rowLimit;
    }

    public void Sort(QueryResultKey[] sortOrder, bool distinct, bool sparse)
    {
      sparseIndex = sparse;
      int[] comparingMask = patternRow.ComparingMask;
      if (sortOrder == null)
      {
        int index = 0;
        for (int count = patternRow.Count; index < count; ++index)
          comparingMask[index] = index + 1;
        patternKey = patternRow;
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
        for (int count = patternRow.Count; length1 < count; ++length1)
          comparingMask[length1] = 0;
        patternKey = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
        int index2 = 0;
        for (int length2 = sortOrder.Length; index2 < length2; ++index2)
          patternKey.AppendColumn(database.CreateEmptyColumn(patternRow[sortOrder[index2].ColumnIndex].Type));
      }
      currentOrder = sortOrder;
      int expectedKeyLen = 0;
      SortSpool sortSpool = new SortSpool(false, (uint) rowCount, ref expectedKeyLen, patternRow, new StorageManager(), true);
      for (int index = 0; index < rowCount; ++index)
        sortSpool.PushKey(rows[index], false);
      sortSpool.Sort();
      if (distinct)
      {
        int index1 = 0;
        for (int index2 = 0; index2 < rowCount; ++index2)
        {
          Row row = sortSpool.PopKey();
          if (index2 == 0 || !row.Equals((object) rows[index1 - 1]))
          {
            rows[index1] = row;
            ++index1;
          }
        }
        if (rowCount == index1)
          return;
        rowCount = index1;
        rows.RemoveRange(rowCount, rows.Count - rowCount);
      }
      else
      {
        for (int index = 0; index < rowCount; ++index)
          rows[index] = sortSpool.PopKey();
      }
    }

    public Row CurrentRow
    {
      get
      {
        return curRow;
      }
      set
      {
        if (rowCount == 0)
          return;
        rows[curRowIndex] = value.CopyInstance();
        curRow = rows[curRowIndex];
      }
    }

    public void FirstRow()
    {
      curRowIndex = 0;
      eof = rowCount == 0;
      if (!eof)
        curRow = rows[curRowIndex];
      else
        curRow = (Row) null;
    }

    public void NextRow()
    {
      if (rowCount == 0 || curRowIndex == rowCount - 1)
      {
        eof = true;
      }
      else
      {
        if (sparseIndex)
        {
          int curRowIndex = this.curRowIndex;
          ++this.curRowIndex;
          while (CheckSameRow())
          {
            if (this.curRowIndex == rowCount - 1)
            {
              this.curRowIndex = curRowIndex;
              eof = true;
              return;
            }
            ++this.curRowIndex;
          }
        }
        else
          ++curRowIndex;
        curRow = rows[curRowIndex];
      }
    }

    public void Close()
    {
    }

    public object GetValue(int index, VistaDBType dataType)
    {
      if (rowCount == 0)
        return (object) null;
      IColumn column = (IColumn) curRow[index];
      if (dataType == VistaDBType.Unknown || column.InternalType == dataType)
        return ((IValue) column).Value;
      IColumn emptyColumn = database.CreateEmptyColumn(dataType);
      database.Conversion.Convert((IValue) column, (IValue) emptyColumn);
      return ((IValue) emptyColumn).Value;
    }

    public IColumn GetColumn(int index)
    {
      if (rowCount != 0)
        return (IColumn) curRow[index];
      return (IColumn) null;
    }

    public bool IsNull(int index)
    {
      if (rowCount != 0)
        return curRow[index].IsNull;
      return true;
    }

    public int GetColumnCount()
    {
      return patternRow.Count;
    }

    public bool EndOfTable
    {
      get
      {
        return eof;
      }
    }

    public long RowCount
    {
      get
      {
        return (long) rowCount;
      }
    }
  }
}
