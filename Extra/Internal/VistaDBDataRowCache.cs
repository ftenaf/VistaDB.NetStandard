using System;
using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;

namespace VistaDB.Extra.Internal
{
  internal class VistaDBDataRowCache : List<IVistaDBRow>
  {
    private long inserterRowNumber = -1;
    private object SynchObj = new object();
        private long minRowIndex;
    private long maxRowIndex;
    private long tableRowCount;
    private long tableNewRowCount;
    private IVistaDBTable table;
    private IVistaDBDatabase db;
    private string tableName;
    private bool exclusive;
    private bool readOnly;
    private VistaDBRowHashTable hashTable;
    private bool tableFiltered;
    private bool tableScoped;
    private bool optimisticLock;
    private bool inserting;
    private bool checkBOT;
    private bool checkEOT;
    private string filterExpression;
    private string scopeLowExpression;
    private string scopeHighExpression;
    private IVistaDBRow insertedRow;
    private VistaDBEventDelegateHandler tableDelegateHandler;

    private VistaDBDataRowCache(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly, int capacity, bool optimistic)
      : base(capacity)
    {
      optimisticLock = optimistic;
      tableFiltered = false;
      tableScoped = false;
      checkBOT = true;
      checkEOT = true;
      hashTable = new VistaDBRowHashTable();
      table = InitializeTable(db, tblName, exclusive, readOnly);
      insertedRow = table.CurrentRow;
      tableDelegateHandler = new VistaDBEventDelegateHandler();
    }

    internal VistaDBDataRowCache(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly)
      : this(db, tblName, exclusive, readOnly, 1, true)
    {
      ResetCache();
      tableRowCount = tableNewRowCount;
    }

    internal VistaDBDataRowCache(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly, int capacity, string indexName, bool optimisticLock)
      : this(db, tblName, exclusive, readOnly, capacity, optimisticLock)
    {
      try
      {
        table.ActiveIndex = indexName;
      }
      catch (VistaDBException ex)
      {
        if (ex.ErrorId != (int) sbyte.MaxValue)
          throw ex;
      }
      finally
      {
        ResetCache();
        tableRowCount = tableNewRowCount;
      }
    }

    private IVistaDBTable InitializeTable(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly)
    {
      try
      {
        this.db = db;
        tableName = tblName;
        this.exclusive = exclusive;
        this.readOnly = readOnly;
        return db.OpenTable(tblName, exclusive, readOnly);
      }
      catch (Exception ex)
      {
        throw new VistaDBDataTableException(ex, 2023);
      }
    }

    private void ResetCache()
    {
      try
      {
        Clear();
        tableNewRowCount = -1L;
        inserterRowNumber = -1L;
        inserting = false;
        table.First();
        CalculateNewTblCount();
        minRowIndex = -1L;
        maxRowIndex = -1L;
        if (tableNewRowCount == 0L)
          return;
        minRowIndex = 0L;
        for (maxRowIndex = -1L; maxRowIndex < (long) (Capacity - 1) && !table.EndOfTable; ++maxRowIndex)
        {
          Add(table.CurrentKey);
          hashTable.Add(this[(int) (maxRowIndex + 1L)], table.CurrentRow);
          table.Next();
        }
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2024);
      }
    }

    private void CalculateMinMaxRows()
    {
      long num = 0;
      table.CurrentKey = this[0];
      while (!table.StartOfTable)
      {
        ++num;
        table.Prev();
      }
      minRowIndex = num - 1L;
      maxRowIndex = minRowIndex + (long) Count - 1L;
    }

    private bool ReReadCache(int index, IVistaDBRow oldKey, IVistaDBRow newKey, TypeOfOperation state)
    {
      try
      {
        long num1 = minRowIndex + (long) index + 1L;
        IVistaDBRow currentKey = table.CurrentKey;
        table.CurrentKey = table.CurrentKey;
        if (currentKey.CompareKey(table.CurrentKey) != 0)
        {
          RemoveAt(index);
          --tableNewRowCount;
          if (maxRowIndex == 0L)
            minRowIndex = -1L;
          --maxRowIndex;
        }
        table.CurrentKey = oldKey;
        if (table.ActiveIndex != null && newKey.CompareKey(oldKey) < 0)
          ++num1;
        for (int index1 = index; index1 < Capacity; ++index1)
        {
          if (table.EndOfTable)
          {
            Clear();
            table.Last();
            maxRowIndex = num1 + (long) index1 - (long) index - 2L;
            if (state != TypeOfOperation.Delete)
              tableNewRowCount = maxRowIndex + 1L;
            for (int index2 = 0; index2 < Capacity && !table.StartOfTable; ++index2)
            {
              Insert(0, table.CurrentKey);
              hashTable.Add(this[0], table.CurrentRow);
              table.Prev();
            }
            if (Count == 0)
            {
              ResetCache();
              return false;
            }
            minRowIndex = maxRowIndex - (long) Count + 1L;
            return true;
          }
          if (index1 < Count)
          {
            RemoveAt(index1);
          }
          else
          {
            ++maxRowIndex;
            if (minRowIndex < 0L)
              minRowIndex = 0L;
          }
          Insert(index1, table.CurrentKey);
          hashTable.Add(this[index1], table.CurrentRow);
          table.Next();
        }
        table.CurrentKey = oldKey;
        for (int index1 = index; index1 >= 0; --index1)
        {
          if (table.StartOfTable)
          {
            Clear();
            table.First();
            int num2;
            for (num2 = 0; num2 < Capacity && !table.EndOfTable; ++num2)
            {
              Add(table.CurrentKey);
              hashTable.Add(this[Count], table.CurrentRow);
              table.Next();
            }
            minRowIndex = 0L;
            maxRowIndex = minRowIndex + (long) num2 - 1L;
            tableNewRowCount = maxRowIndex + 1L;
            return false;
          }
          if (this[index1] != null)
            RemoveAt(index1);
          Insert(index1, table.CurrentKey);
          hashTable.Add(this[index1], table.CurrentRow);
          table.Prev();
        }
        if (table.ActiveIndex != null && newKey.CompareKey(this[0]) < 0)
        {
          ++minRowIndex;
          maxRowIndex = minRowIndex + (long) Count - 1L;
        }
        return true;
      }
      catch (Exception)
            {
        return true;
      }
    }

    private int CheckRowPosition(int index)
    {
      if (this[index].CompareKey(table.CurrentKey) == 0)
        return 0;
      ResetCache();
      return -1;
    }

    private int IsFirstRow(int cachedRowPos)
    {
      table.CurrentKey = this[cachedRowPos];
      table.Prev();
      if (table.StartOfTable)
      {
        table.First();
        return 0;
      }
      int num;
      for (num = 0; num < 100 && !table.StartOfTable; ++num)
      {
        table.Prev();
        ++tableNewRowCount;
      }
      return num;
    }

    private int IsLastRow(int cachedRowPos)
    {
      checkBOT = true;
      table.CurrentKey = this[cachedRowPos];
      table.Next();
      if (table.EndOfTable)
      {
        table.Last();
        return 0;
      }
      int num;
      for (num = 0; num < 100 && !table.EndOfTable; ++num)
      {
        table.Next();
        ++tableNewRowCount;
      }
      return num;
    }

    private int LeftUpdate(long offset, long index)
    {
      if (offset > 100L && checkBOT && index == 0L)
      {
        table.First();
        int num = table.EndOfTable ? 1 : 0;
        Clear();
        IVistaDBRow currentKey = table.CurrentKey;
        Add(currentKey);
        hashTable.Add(currentKey, table.CurrentRow);
        maxRowIndex = 0L;
        minRowIndex = 0L;
        return 0;
      }
      if (offset < (long) Capacity)
      {
        int num = Capacity - Count;
        table.CurrentKey = this[0];
        if (CheckRowPosition(0) != 0)
          return tableNewRowCount <= 0L ? -1 : 0;
        if (offset > (long) num)
        {
          for (long index1 = 0; index1 < offset - (long) num; ++index1)
            RemoveAt(Count - 1);
          table.CurrentKey = this[0];
          if (CheckRowPosition(0) != 0)
            return tableNewRowCount <= 0L ? -1 : 0;
        }
        for (long index1 = offset; index1 > 0L; --index1)
        {
          if (table.StartOfTable)
          {
            minRowIndex = 0L;
            tableNewRowCount = tableRowCount - offset + index1 - 1L;
            maxRowIndex = minRowIndex + (long) Count - 1L;
            return 0;
          }
          table.Prev();
          Insert(0, table.CurrentKey);
          hashTable.Add(this[0], table.CurrentRow);
        }
        minRowIndex -= offset;
        maxRowIndex = minRowIndex + (long) Count - 1L;
      }
      else
      {
        table.CurrentKey = this[0];
        if (CheckRowPosition(0) != 0)
          return tableNewRowCount <= 0L ? -1 : 0;
        long num;
        for (num = 0L; num < offset && !table.StartOfTable; ++num)
          table.Prev();
        if (table.StartOfTable)
        {
          table.First();
          tableNewRowCount = tableRowCount - offset + num - 1L;
          minRowIndex = 0L;
        }
        else
          minRowIndex -= offset;
        Clear();
        for (long index1 = 0; index1 < (long) Capacity; ++index1)
        {
          if (table.EndOfTable)
          {
            tableNewRowCount = minRowIndex + index1;
            maxRowIndex = minRowIndex + (long) Count - 1L;
            return Count - 1;
          }
          Add(table.CurrentKey);
          hashTable.Add(this[Count - 1], table.CurrentRow);
          table.Next();
        }
      }
      int num1 = 0;
      if (index == 0L && checkBOT)
        num1 = IsFirstRow(0);
      maxRowIndex = minRowIndex + (long) Count - 1L + (long) num1;
      minRowIndex += (long) num1;
      return 0;
    }

    private int RightUpdate(long offset)
    {
      bool flag = maxRowIndex + offset == tableNewRowCount - 1L;
      if (offset > 100L && flag && checkEOT)
      {
        table.Last();
        int num = table.StartOfTable ? 1 : 0;
        Clear();
        IVistaDBRow currentKey = table.CurrentKey;
        Add(currentKey);
        hashTable.Add(currentKey, table.CurrentRow);
        maxRowIndex = tableNewRowCount - 1L;
        minRowIndex = maxRowIndex;
        return 0;
      }
      if (offset < (long) Capacity)
      {
        int num = Capacity - Count;
        if (offset > (long) num)
        {
          for (long index = 0; index < offset; ++index)
            RemoveAt(0);
        }
        if (offset == 0L)
        {
          table.First();
          if (table.EndOfTable)
            return -1;
          Add(table.CurrentKey);
          hashTable.Add(this[Count - 1], table.CurrentRow);
          minRowIndex = 0L;
          maxRowIndex = 0L;
          return 0;
        }
        table.CurrentKey = this[Count - 1];
        if (CheckRowPosition(Count - 1) != 0)
          return tableNewRowCount <= 0L ? -1 : 0;
        for (long index = 0; index < offset; ++index)
        {
          if (table.EndOfTable)
          {
            tableNewRowCount = maxRowIndex + index;
            maxRowIndex = tableNewRowCount;
            minRowIndex = maxRowIndex - (long) Count + 1L;
            return Count - 1;
          }
          table.Next();
          Add(table.CurrentKey);
          hashTable.Add(this[Count - 1], table.CurrentRow);
        }
        maxRowIndex += offset;
        minRowIndex = maxRowIndex - (long) Count + 1L;
      }
      else
      {
        table.CurrentKey = this[Count - 1];
        if (CheckRowPosition(Count - 1) != 0)
          return tableNewRowCount <= 0L ? -1 : 0;
        long num;
        for (num = 0L; num < offset && !table.EndOfTable; ++num)
          table.Next();
        if (table.EndOfTable)
        {
          table.Last();
          tableNewRowCount = maxRowIndex + num;
          maxRowIndex = tableNewRowCount - 1L;
        }
        else
          maxRowIndex += offset;
        Clear();
        for (long capacity = (long) Capacity; capacity > 0L; --capacity)
        {
          if (table.StartOfTable)
          {
            minRowIndex = 0L;
            maxRowIndex = minRowIndex + (long) Count - 1L;
            return 0;
          }
          Insert(0, table.CurrentKey);
          hashTable.Add(this[0], table.CurrentRow);
          table.Prev();
        }
        minRowIndex = maxRowIndex - (long) Count + 1L;
      }
      if (checkEOT && flag)
        IsLastRow(Count - 1);
      return Count - 1;
    }

    private bool TryToReopenTable()
    {
      try
      {
        IVistaDBRow currentRow = table.CurrentRow;
        string activeIndex = table.ActiveIndex;
        table.Close();
        table = db.OpenTable(tableName, exclusive, readOnly);
        if (activeIndex != null)
          table.ActiveIndex = activeIndex;
        if (filterExpression != null)
          table.SetFilter(filterExpression, true);
        if (scopeHighExpression != null)
          table.SetScope(scopeLowExpression, scopeHighExpression);
        table.CurrentRow = currentRow;
        return true;
      }
      catch (Exception)
            {
        return false;
      }
    }

    private int PositionCache(long index)
    {
      lock (SynchObj)
      {
        try
        {
          if (table.IsClosed)
            TryToReopenTable();
          if (index < minRowIndex)
            return LeftUpdate(minRowIndex - index, index);
          if (index > maxRowIndex)
            return RightUpdate(maxRowIndex < 0L ? 0L : index - maxRowIndex);
          if (index == tableNewRowCount - 1L)
            IsLastRow((int) (index - minRowIndex));
          if (index - minRowIndex < 0L || index - minRowIndex >= (long) Count)
            throw new IndexOutOfRangeException("Internal cache exception");
          return (int) (index - minRowIndex);
        }
        catch (VistaDBException ex)
        {
          throw ex;
        }
      }
    }

    private void CalculateNewTblCount()
    {
      if (!tableFiltered && !tableScoped)
        tableNewRowCount = table.RowCount;
      else if (tableRowCount > 10000L)
      {
        tableNewRowCount = 100000L;
      }
      else
      {
        if (table == null)
          throw new VistaDBException(2041);
        IVistaDBRow currentKey = table.CurrentKey;
        try
        {
          table.First();
          int num = 0;
          while (!table.EndOfTable)
          {
            ++num;
            table.Next();
          }
          tableNewRowCount = (long) num;
        }
        finally
        {
          table.CurrentKey = currentKey;
        }
      }
    }

    internal IVistaDBRow GetDataRow(int index)
    {
      try
      {
        if ((long) index == inserterRowNumber)
          return insertedRow;
        int index1 = PositionCache((long) index);
        if (index1 >= 0 && (long) index < tableNewRowCount)
          return hashTable[this[index1]];
        IVistaDBRow currentRow = table.CurrentRow;
        foreach (IVistaDBValue vistaDbValue in (IEnumerable) currentRow)
          vistaDbValue.Value = (object) null;
        return currentRow;
      }
      catch (VistaDBException ex)
      {
        if (ex.Contains(140L) && TryToReopenTable())
          return GetDataRow(index);
        throw ex;
      }
      catch (Exception)
            {
        throw;
      }
    }

    internal long GetCurrentRowID(long index)
    {
      try
      {
        int index1 = PositionCache(index);
        if (index1 < 0 || index1 >= Count)
          return -1;
        IVistaDBRow vistaDbRow = hashTable[this[index1]];
        return vistaDbRow == null ? -1L : vistaDbRow.RowId;
      }
      catch (Exception)
            {
        return -1;
      }
    }

    internal void ResetInsertedRow()
    {
      inserterRowNumber = -1L;
    }

    internal bool FillInsertedRow(long rowPos)
    {
      if (inserting || inserterRowNumber == rowPos)
        return false;
      int index1 = PositionCache(rowPos);
      if (index1 < 0)
        return false;
      IVistaDBRow vistaDbRow = hashTable[this[index1]];
      for (int index2 = 0; index2 < vistaDbRow.Count; ++index2)
        insertedRow[index2].Value = vistaDbRow[index2].Value;
      insertedRow.ClearModified();
      inserterRowNumber = rowPos;
      return true;
    }

    internal void SetDataToColumn(int keyIndex, int colIndex, object val)
    {
      if (!optimisticLock && !inserting)
        table.Lock(this[PositionCache((long) keyIndex)].RowId);
      insertedRow[colIndex].Value = val is DBNull ? (object) null : val;
      inserterRowNumber = (long) keyIndex;
    }

    internal int CheckRowCount()
    {
      if (tableRowCount == tableNewRowCount)
        return 0;
      tableRowCount = tableNewRowCount;
      return -1;
    }

    internal void CloseTable()
    {
      if (table == null)
        return;
      table.Close();
      Clear();
      minRowIndex = -1L;
      maxRowIndex = -1L;
      tableNewRowCount = 0L;
    }

    internal string GetTableActiveIndex()
    {
      if (table == null)
        throw new InvalidOperationException("Table isn't opened");
      return table.ActiveIndex;
    }

    internal void DeleteRow(long index)
    {
      SynchronizeTableData((int) index, TypeOfOperation.Delete);
      --tableRowCount;
      --tableNewRowCount;
    }

    internal void InsertRow()
    {
      try
      {
        int num;
        if (Count > 0)
        {
          IVistaDBRow vistaDbRow = this[PositionCache(tableRowCount - 1L)];
          table.CurrentKey = vistaDbRow;
          num = -1;
          if (vistaDbRow.CompareKey(table.CurrentKey) != 0)
          {
            ResetCache();
            tableRowCount = tableNewRowCount;
            throw new VistaDBDataTableException(2011);
          }
        }
        else
        {
          table.First();
          num = 0;
        }
        while (!table.EndOfTable)
        {
          table.Next();
          ++num;
        }
        table.Last();
        if (num != 0)
        {
          tableRowCount += (long) num;
          tableNewRowCount += (long) num;
          throw new VistaDBDataTableException(2012);
        }
        inserterRowNumber = tableRowCount;
        inserting = true;
        foreach (IVistaDBValue vistaDbValue in (IEnumerable) insertedRow)
          vistaDbValue.Value = (object) null;
        insertedRow.ClearModified();
        ++tableRowCount;
        ++tableNewRowCount;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2010);
      }
    }

    internal void SetScope(string lowExp, string highExp)
    {
      try
      {
        table.SetScope(lowExp, highExp);
        scopeHighExpression = highExp;
        scopeLowExpression = lowExp;
        tableScoped = true;
        ResetCache();
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2013);
      }
    }

    internal void ResetScope()
    {
      try
      {
        table.ResetScope();
        tableScoped = false;
        scopeHighExpression = (string) null;
        scopeLowExpression = (string) null;
        ResetCache();
        tableRowCount = tableNewRowCount;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2014);
      }
    }

    internal void SetFilter(string exp, bool optimize)
    {
      try
      {
        table.SetFilter(exp, optimize);
        filterExpression = exp;
        tableFiltered = exp != null;
        ResetCache();
        tableRowCount = tableNewRowCount;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2015);
      }
    }

    internal bool CancelInsert()
    {
      if (inserting)
      {
        inserting = false;
        if (tableNewRowCount > 0L)
        {
          --tableRowCount;
          --tableNewRowCount;
        }
      }
      return true;
    }

    internal int SynchronizeTableData(int index, TypeOfOperation typeOp)
    {
      lock (SynchObj)
      {
        switch (typeOp)
        {
          case TypeOfOperation.Update:
            int index1 = PositionCache((long) index);
            IVistaDBRow vistaDbRow1 = this[index1];
            try
            {
              table.CurrentKey = vistaDbRow1;
              if (table.CurrentKey.CompareKey(vistaDbRow1) != 0)
                throw new VistaDBDataTableException(2016);
              table.CurrentRow = insertedRow;
              table.Post();
              return 1;
            }
            catch (VistaDBException ex)
            {
              throw new VistaDBDataTableException((Exception) ex, 2019);
            }
            finally
            {
              if (!optimisticLock)
                table.Unlock(vistaDbRow1.RowId);
              inserterRowNumber = -1L;
              ReReadCache(index1, vistaDbRow1, table.CurrentKey, typeOp);
            }
          case TypeOfOperation.Insert | TypeOfOperation.Update:
            bool flag = false;
            try
            {
              table.Insert();
              table.CurrentRow = insertedRow;
              table.Post();
              IVistaDBRow currentKey = table.CurrentKey;
              table.CurrentKey = table.CurrentKey;
              if (table.EndOfTable || table.StartOfTable || table.CurrentKey.CompareKey(currentKey) != 0)
              {
                flag = true;
                throw new VistaDBDataTableException(2020);
              }
              return 2;
            }
            catch (VistaDBException ex)
            {
              flag = true;
              if (ex.ErrorId == 2020)
                throw ex;
              throw new VistaDBDataTableException((Exception) ex, 2021);
            }
            finally
            {
              maxRowIndex = flag ? (long) (index - 1) : (long) index;
              table.Last();
              Clear();
              int num = 0;
              for (int capacity = Capacity; num < capacity; ++num)
              {
                if (table.StartOfTable)
                {
                  minRowIndex = 0L;
                  maxRowIndex = minRowIndex + (long) Count - 1L;
                  break;
                }
                Insert(0, table.CurrentKey);
                hashTable.Add(this[0], table.CurrentRow);
                table.Prev();
                minRowIndex = maxRowIndex - (long) Count + 1L;
              }
            }
          case TypeOfOperation.Delete:
            int index2 = PositionCache((long) index);
            IVistaDBRow vistaDbRow2 = this[index2];
            table.CurrentKey = vistaDbRow2;
            if (table.CurrentKey.CompareKey(vistaDbRow2) != 0)
              throw new VistaDBDataTableException(2016);
            try
            {
              table.Delete();
              return 3;
            }
            catch (VistaDBException ex)
            {
              throw new VistaDBDataTableException((Exception) ex, 2022);
            }
            finally
            {
              ReReadCache(index2, vistaDbRow2, table.CurrentKey, typeOp);
              inserterRowNumber = -1L;
            }
          default:
            return 0;
        }
      }
    }

    internal bool Find(string keyExpr, string idxName, bool partialMaching, bool softPos)
    {
      try
      {
        if (!table.Find(keyExpr, idxName, partialMaching, softPos))
          return false;
        Clear();
        inserterRowNumber = -1L;
        for (int index = 0; index < Capacity && !table.EndOfTable; ++index)
        {
          Add(table.CurrentKey);
          hashTable.Add(this[Count - 1], table.CurrentRow);
          table.Next();
        }
        minRowIndex = 0L;
        maxRowIndex = (long) (Count - 1);
        checkBOT = false;
        tableNewRowCount = (long) Count;
        return true;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2017);
      }
      catch (Exception)
            {
      }
      return false;
    }

    internal long ChangeActiveIndex(long rowIndex, string activeIndex)
    {
      lock (SynchObj)
      {
        try
        {
          checkBOT = false;
          checkEOT = false;
          int index1 = PositionCache(rowIndex);
          if (index1 > -1)
            table.CurrentKey = this[index1];
          table.ActiveIndex = activeIndex;
          Clear();
          if (index1 < 0)
            return -1;
          Add(table.CurrentKey);
          hashTable.Add(this[0], table.CurrentRow);
          int num = 0;
          for (int index2 = index1 - 1; index2 >= 0; --index2)
          {
            table.Prev();
            if (table.StartOfTable)
            {
              minRowIndex = 0L;
              maxRowIndex = (long) (Count - 1);
              return (long) num;
            }
            Insert(0, table.CurrentKey);
            hashTable.Add(this[0], table.CurrentRow);
            ++num;
          }
          table.CurrentKey = this[Count - 1];
          for (int index2 = index1 + 1; index2 < Capacity; ++index2)
          {
            table.Next();
            if (table.EndOfTable)
            {
              CalculateMinMaxRows();
              if (Count == 0)
                throw new Exception("ERROR");
              return minRowIndex + (long) num;
            }
            Add(table.CurrentKey);
            hashTable.Add(this[index2], table.CurrentRow);
          }
          CalculateMinMaxRows();
          if (Count == 0)
            throw new Exception("ERROR");
          return (long) num + minRowIndex;
        }
        catch (VistaDBException ex)
        {
          throw new VistaDBDataTableException((Exception) ex, 2018);
        }
        finally
        {
          inserterRowNumber = -1L;
          checkBOT = true;
          checkEOT = true;
        }
      }
    }

    internal bool TableFiltered
    {
      get
      {
        return tableFiltered;
      }
      set
      {
        tableFiltered = value;
      }
    }

    internal bool IsInserting
    {
      get
      {
        return inserting;
      }
      set
      {
        inserting = value;
      }
    }

    internal long TableRowCount
    {
      get
      {
        return tableRowCount;
      }
    }

    internal bool OptimisticLock
    {
      get
      {
        return optimisticLock;
      }
      set
      {
        optimisticLock = value;
      }
    }

    public new void Clear()
    {
      hashTable.Clear();
      base.Clear();
    }

    public new void RemoveAt(int index)
    {
      hashTable.Remove(this[index]);
      base.RemoveAt(index);
    }

    internal IVistaDBRow InsertedRow
    {
      get
      {
        return insertedRow;
      }
    }
  }
}
