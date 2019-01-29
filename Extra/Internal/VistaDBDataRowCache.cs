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
    private const int ReReadRowsCount = 100;
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
      this.optimisticLock = optimistic;
      this.tableFiltered = false;
      this.tableScoped = false;
      this.checkBOT = true;
      this.checkEOT = true;
      this.hashTable = new VistaDBRowHashTable();
      this.table = this.InitializeTable(db, tblName, exclusive, readOnly);
      this.insertedRow = this.table.CurrentRow;
      this.tableDelegateHandler = new VistaDBEventDelegateHandler();
    }

    internal VistaDBDataRowCache(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly)
      : this(db, tblName, exclusive, readOnly, 1, true)
    {
      this.ResetCache();
      this.tableRowCount = this.tableNewRowCount;
    }

    internal VistaDBDataRowCache(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly, int capacity, string indexName, bool optimisticLock)
      : this(db, tblName, exclusive, readOnly, capacity, optimisticLock)
    {
      try
      {
        this.table.ActiveIndex = indexName;
      }
      catch (VistaDBException ex)
      {
        if (ex.ErrorId != (int) sbyte.MaxValue)
          throw ex;
      }
      finally
      {
        this.ResetCache();
        this.tableRowCount = this.tableNewRowCount;
      }
    }

    private IVistaDBTable InitializeTable(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly)
    {
      try
      {
        this.db = db;
        this.tableName = tblName;
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
        this.Clear();
        this.tableNewRowCount = -1L;
        this.inserterRowNumber = -1L;
        this.inserting = false;
        this.table.First();
        this.CalculateNewTblCount();
        this.minRowIndex = -1L;
        this.maxRowIndex = -1L;
        if (this.tableNewRowCount == 0L)
          return;
        this.minRowIndex = 0L;
        for (this.maxRowIndex = -1L; this.maxRowIndex < (long) (this.Capacity - 1) && !this.table.EndOfTable; ++this.maxRowIndex)
        {
          this.Add(this.table.CurrentKey);
          this.hashTable.Add(this[(int) (this.maxRowIndex + 1L)], this.table.CurrentRow);
          this.table.Next();
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
      this.table.CurrentKey = this[0];
      while (!this.table.StartOfTable)
      {
        ++num;
        this.table.Prev();
      }
      this.minRowIndex = num - 1L;
      this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
    }

    private bool ReReadCache(int index, IVistaDBRow oldKey, IVistaDBRow newKey, TypeOfOperation state)
    {
      try
      {
        long num1 = this.minRowIndex + (long) index + 1L;
        IVistaDBRow currentKey = this.table.CurrentKey;
        this.table.CurrentKey = this.table.CurrentKey;
        if (currentKey.CompareKey(this.table.CurrentKey) != 0)
        {
          this.RemoveAt(index);
          --this.tableNewRowCount;
          if (this.maxRowIndex == 0L)
            this.minRowIndex = -1L;
          --this.maxRowIndex;
        }
        this.table.CurrentKey = oldKey;
        if (this.table.ActiveIndex != null && newKey.CompareKey(oldKey) < 0)
          ++num1;
        for (int index1 = index; index1 < this.Capacity; ++index1)
        {
          if (this.table.EndOfTable)
          {
            this.Clear();
            this.table.Last();
            this.maxRowIndex = num1 + (long) index1 - (long) index - 2L;
            if (state != TypeOfOperation.Delete)
              this.tableNewRowCount = this.maxRowIndex + 1L;
            for (int index2 = 0; index2 < this.Capacity && !this.table.StartOfTable; ++index2)
            {
              this.Insert(0, this.table.CurrentKey);
              this.hashTable.Add(this[0], this.table.CurrentRow);
              this.table.Prev();
            }
            if (this.Count == 0)
            {
              this.ResetCache();
              return false;
            }
            this.minRowIndex = this.maxRowIndex - (long) this.Count + 1L;
            return true;
          }
          if (index1 < this.Count)
          {
            this.RemoveAt(index1);
          }
          else
          {
            ++this.maxRowIndex;
            if (this.minRowIndex < 0L)
              this.minRowIndex = 0L;
          }
          this.Insert(index1, this.table.CurrentKey);
          this.hashTable.Add(this[index1], this.table.CurrentRow);
          this.table.Next();
        }
        this.table.CurrentKey = oldKey;
        for (int index1 = index; index1 >= 0; --index1)
        {
          if (this.table.StartOfTable)
          {
            this.Clear();
            this.table.First();
            int num2;
            for (num2 = 0; num2 < this.Capacity && !this.table.EndOfTable; ++num2)
            {
              this.Add(this.table.CurrentKey);
              this.hashTable.Add(this[this.Count], this.table.CurrentRow);
              this.table.Next();
            }
            this.minRowIndex = 0L;
            this.maxRowIndex = this.minRowIndex + (long) num2 - 1L;
            this.tableNewRowCount = this.maxRowIndex + 1L;
            return false;
          }
          if (this[index1] != null)
            this.RemoveAt(index1);
          this.Insert(index1, this.table.CurrentKey);
          this.hashTable.Add(this[index1], this.table.CurrentRow);
          this.table.Prev();
        }
        if (this.table.ActiveIndex != null && newKey.CompareKey(this[0]) < 0)
        {
          ++this.minRowIndex;
          this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
        }
        return true;
      }
      catch (Exception ex)
      {
        return true;
      }
    }

    private int CheckRowPosition(int index)
    {
      if (this[index].CompareKey(this.table.CurrentKey) == 0)
        return 0;
      this.ResetCache();
      return -1;
    }

    private int IsFirstRow(int cachedRowPos)
    {
      this.table.CurrentKey = this[cachedRowPos];
      this.table.Prev();
      if (this.table.StartOfTable)
      {
        this.table.First();
        return 0;
      }
      int num;
      for (num = 0; num < 100 && !this.table.StartOfTable; ++num)
      {
        this.table.Prev();
        ++this.tableNewRowCount;
      }
      return num;
    }

    private int IsLastRow(int cachedRowPos)
    {
      this.checkBOT = true;
      this.table.CurrentKey = this[cachedRowPos];
      this.table.Next();
      if (this.table.EndOfTable)
      {
        this.table.Last();
        return 0;
      }
      int num;
      for (num = 0; num < 100 && !this.table.EndOfTable; ++num)
      {
        this.table.Next();
        ++this.tableNewRowCount;
      }
      return num;
    }

    private int LeftUpdate(long offset, long index)
    {
      if (offset > 100L && this.checkBOT && index == 0L)
      {
        this.table.First();
        int num = this.table.EndOfTable ? 1 : 0;
        this.Clear();
        IVistaDBRow currentKey = this.table.CurrentKey;
        this.Add(currentKey);
        this.hashTable.Add(currentKey, this.table.CurrentRow);
        this.maxRowIndex = 0L;
        this.minRowIndex = 0L;
        return 0;
      }
      if (offset < (long) this.Capacity)
      {
        int num = this.Capacity - this.Count;
        this.table.CurrentKey = this[0];
        if (this.CheckRowPosition(0) != 0)
          return this.tableNewRowCount <= 0L ? -1 : 0;
        if (offset > (long) num)
        {
          for (long index1 = 0; index1 < offset - (long) num; ++index1)
            this.RemoveAt(this.Count - 1);
          this.table.CurrentKey = this[0];
          if (this.CheckRowPosition(0) != 0)
            return this.tableNewRowCount <= 0L ? -1 : 0;
        }
        for (long index1 = offset; index1 > 0L; --index1)
        {
          if (this.table.StartOfTable)
          {
            this.minRowIndex = 0L;
            this.tableNewRowCount = this.tableRowCount - offset + index1 - 1L;
            this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
            return 0;
          }
          this.table.Prev();
          this.Insert(0, this.table.CurrentKey);
          this.hashTable.Add(this[0], this.table.CurrentRow);
        }
        this.minRowIndex -= offset;
        this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
      }
      else
      {
        this.table.CurrentKey = this[0];
        if (this.CheckRowPosition(0) != 0)
          return this.tableNewRowCount <= 0L ? -1 : 0;
        long num;
        for (num = 0L; num < offset && !this.table.StartOfTable; ++num)
          this.table.Prev();
        if (this.table.StartOfTable)
        {
          this.table.First();
          this.tableNewRowCount = this.tableRowCount - offset + num - 1L;
          this.minRowIndex = 0L;
        }
        else
          this.minRowIndex -= offset;
        this.Clear();
        for (long index1 = 0; index1 < (long) this.Capacity; ++index1)
        {
          if (this.table.EndOfTable)
          {
            this.tableNewRowCount = this.minRowIndex + index1;
            this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
            return this.Count - 1;
          }
          this.Add(this.table.CurrentKey);
          this.hashTable.Add(this[this.Count - 1], this.table.CurrentRow);
          this.table.Next();
        }
      }
      int num1 = 0;
      if (index == 0L && this.checkBOT)
        num1 = this.IsFirstRow(0);
      this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L + (long) num1;
      this.minRowIndex += (long) num1;
      return 0;
    }

    private int RightUpdate(long offset)
    {
      bool flag = this.maxRowIndex + offset == this.tableNewRowCount - 1L;
      if (offset > 100L && flag && this.checkEOT)
      {
        this.table.Last();
        int num = this.table.StartOfTable ? 1 : 0;
        this.Clear();
        IVistaDBRow currentKey = this.table.CurrentKey;
        this.Add(currentKey);
        this.hashTable.Add(currentKey, this.table.CurrentRow);
        this.maxRowIndex = this.tableNewRowCount - 1L;
        this.minRowIndex = this.maxRowIndex;
        return 0;
      }
      if (offset < (long) this.Capacity)
      {
        int num = this.Capacity - this.Count;
        if (offset > (long) num)
        {
          for (long index = 0; index < offset; ++index)
            this.RemoveAt(0);
        }
        if (offset == 0L)
        {
          this.table.First();
          if (this.table.EndOfTable)
            return -1;
          this.Add(this.table.CurrentKey);
          this.hashTable.Add(this[this.Count - 1], this.table.CurrentRow);
          this.minRowIndex = 0L;
          this.maxRowIndex = 0L;
          return 0;
        }
        this.table.CurrentKey = this[this.Count - 1];
        if (this.CheckRowPosition(this.Count - 1) != 0)
          return this.tableNewRowCount <= 0L ? -1 : 0;
        for (long index = 0; index < offset; ++index)
        {
          if (this.table.EndOfTable)
          {
            this.tableNewRowCount = this.maxRowIndex + index;
            this.maxRowIndex = this.tableNewRowCount;
            this.minRowIndex = this.maxRowIndex - (long) this.Count + 1L;
            return this.Count - 1;
          }
          this.table.Next();
          this.Add(this.table.CurrentKey);
          this.hashTable.Add(this[this.Count - 1], this.table.CurrentRow);
        }
        this.maxRowIndex += offset;
        this.minRowIndex = this.maxRowIndex - (long) this.Count + 1L;
      }
      else
      {
        this.table.CurrentKey = this[this.Count - 1];
        if (this.CheckRowPosition(this.Count - 1) != 0)
          return this.tableNewRowCount <= 0L ? -1 : 0;
        long num;
        for (num = 0L; num < offset && !this.table.EndOfTable; ++num)
          this.table.Next();
        if (this.table.EndOfTable)
        {
          this.table.Last();
          this.tableNewRowCount = this.maxRowIndex + num;
          this.maxRowIndex = this.tableNewRowCount - 1L;
        }
        else
          this.maxRowIndex += offset;
        this.Clear();
        for (long capacity = (long) this.Capacity; capacity > 0L; --capacity)
        {
          if (this.table.StartOfTable)
          {
            this.minRowIndex = 0L;
            this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
            return 0;
          }
          this.Insert(0, this.table.CurrentKey);
          this.hashTable.Add(this[0], this.table.CurrentRow);
          this.table.Prev();
        }
        this.minRowIndex = this.maxRowIndex - (long) this.Count + 1L;
      }
      if (this.checkEOT && flag)
        this.IsLastRow(this.Count - 1);
      return this.Count - 1;
    }

    private bool TryToReopenTable()
    {
      try
      {
        IVistaDBRow currentRow = this.table.CurrentRow;
        string activeIndex = this.table.ActiveIndex;
        this.table.Close();
        this.table = this.db.OpenTable(this.tableName, this.exclusive, this.readOnly);
        if (activeIndex != null)
          this.table.ActiveIndex = activeIndex;
        if (this.filterExpression != null)
          this.table.SetFilter(this.filterExpression, true);
        if (this.scopeHighExpression != null)
          this.table.SetScope(this.scopeLowExpression, this.scopeHighExpression);
        this.table.CurrentRow = currentRow;
        return true;
      }
      catch (Exception ex)
      {
        return false;
      }
    }

    private int PositionCache(long index)
    {
      lock (this.SynchObj)
      {
        try
        {
          if (this.table.IsClosed)
            this.TryToReopenTable();
          if (index < this.minRowIndex)
            return this.LeftUpdate(this.minRowIndex - index, index);
          if (index > this.maxRowIndex)
            return this.RightUpdate(this.maxRowIndex < 0L ? 0L : index - this.maxRowIndex);
          if (index == this.tableNewRowCount - 1L)
            this.IsLastRow((int) (index - this.minRowIndex));
          if (index - this.minRowIndex < 0L || index - this.minRowIndex >= (long) this.Count)
            throw new IndexOutOfRangeException("Internal cache exception");
          return (int) (index - this.minRowIndex);
        }
        catch (VistaDBException ex)
        {
          throw ex;
        }
      }
    }

    private void CalculateNewTblCount()
    {
      if (!this.tableFiltered && !this.tableScoped)
        this.tableNewRowCount = this.table.RowCount;
      else if (this.tableRowCount > 10000L)
      {
        this.tableNewRowCount = 100000L;
      }
      else
      {
        if (this.table == null)
          throw new VistaDBException(2041);
        IVistaDBRow currentKey = this.table.CurrentKey;
        try
        {
          this.table.First();
          int num = 0;
          while (!this.table.EndOfTable)
          {
            ++num;
            this.table.Next();
          }
          this.tableNewRowCount = (long) num;
        }
        finally
        {
          this.table.CurrentKey = currentKey;
        }
      }
    }

    internal IVistaDBRow GetDataRow(int index)
    {
      try
      {
        if ((long) index == this.inserterRowNumber)
          return this.insertedRow;
        int index1 = this.PositionCache((long) index);
        if (index1 >= 0 && (long) index < this.tableNewRowCount)
          return this.hashTable[this[index1]];
        IVistaDBRow currentRow = this.table.CurrentRow;
        foreach (IVistaDBValue vistaDbValue in (IEnumerable) currentRow)
          vistaDbValue.Value = (object) null;
        return currentRow;
      }
      catch (VistaDBException ex)
      {
        if (ex.Contains(140L) && this.TryToReopenTable())
          return this.GetDataRow(index);
        throw ex;
      }
      catch (Exception ex)
      {
        throw;
      }
    }

    internal long GetCurrentRowID(long index)
    {
      try
      {
        int index1 = this.PositionCache(index);
        if (index1 < 0 || index1 >= this.Count)
          return -1;
        IVistaDBRow vistaDbRow = this.hashTable[this[index1]];
        return vistaDbRow == null ? -1L : vistaDbRow.RowId;
      }
      catch (Exception ex)
      {
        return -1;
      }
    }

    internal void ResetInsertedRow()
    {
      this.inserterRowNumber = -1L;
    }

    internal bool FillInsertedRow(long rowPos)
    {
      if (this.inserting || this.inserterRowNumber == rowPos)
        return false;
      int index1 = this.PositionCache(rowPos);
      if (index1 < 0)
        return false;
      IVistaDBRow vistaDbRow = this.hashTable[this[index1]];
      for (int index2 = 0; index2 < vistaDbRow.Count; ++index2)
        this.insertedRow[index2].Value = vistaDbRow[index2].Value;
      this.insertedRow.ClearModified();
      this.inserterRowNumber = rowPos;
      return true;
    }

    internal void SetDataToColumn(int keyIndex, int colIndex, object val)
    {
      if (!this.optimisticLock && !this.inserting)
        this.table.Lock(this[this.PositionCache((long) keyIndex)].RowId);
      this.insertedRow[colIndex].Value = val is DBNull ? (object) null : val;
      this.inserterRowNumber = (long) keyIndex;
    }

    internal int CheckRowCount()
    {
      if (this.tableRowCount == this.tableNewRowCount)
        return 0;
      this.tableRowCount = this.tableNewRowCount;
      return -1;
    }

    internal void CloseTable()
    {
      if (this.table == null)
        return;
      this.table.Close();
      this.Clear();
      this.minRowIndex = -1L;
      this.maxRowIndex = -1L;
      this.tableNewRowCount = 0L;
    }

    internal string GetTableActiveIndex()
    {
      if (this.table == null)
        throw new InvalidOperationException("Table isn't opened");
      return this.table.ActiveIndex;
    }

    internal void DeleteRow(long index)
    {
      this.SynchronizeTableData((int) index, TypeOfOperation.Delete);
      --this.tableRowCount;
      --this.tableNewRowCount;
    }

    internal void InsertRow()
    {
      try
      {
        int num;
        if (this.Count > 0)
        {
          IVistaDBRow vistaDbRow = this[this.PositionCache(this.tableRowCount - 1L)];
          this.table.CurrentKey = vistaDbRow;
          num = -1;
          if (vistaDbRow.CompareKey(this.table.CurrentKey) != 0)
          {
            this.ResetCache();
            this.tableRowCount = this.tableNewRowCount;
            throw new VistaDBDataTableException(2011);
          }
        }
        else
        {
          this.table.First();
          num = 0;
        }
        while (!this.table.EndOfTable)
        {
          this.table.Next();
          ++num;
        }
        this.table.Last();
        if (num != 0)
        {
          this.tableRowCount += (long) num;
          this.tableNewRowCount += (long) num;
          throw new VistaDBDataTableException(2012);
        }
        this.inserterRowNumber = this.tableRowCount;
        this.inserting = true;
        foreach (IVistaDBValue vistaDbValue in (IEnumerable) this.insertedRow)
          vistaDbValue.Value = (object) null;
        this.insertedRow.ClearModified();
        ++this.tableRowCount;
        ++this.tableNewRowCount;
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
        this.table.SetScope(lowExp, highExp);
        this.scopeHighExpression = highExp;
        this.scopeLowExpression = lowExp;
        this.tableScoped = true;
        this.ResetCache();
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
        this.table.ResetScope();
        this.tableScoped = false;
        this.scopeHighExpression = (string) null;
        this.scopeLowExpression = (string) null;
        this.ResetCache();
        this.tableRowCount = this.tableNewRowCount;
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
        this.table.SetFilter(exp, optimize);
        this.filterExpression = exp;
        this.tableFiltered = exp != null;
        this.ResetCache();
        this.tableRowCount = this.tableNewRowCount;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2015);
      }
    }

    internal bool CancelInsert()
    {
      if (this.inserting)
      {
        this.inserting = false;
        if (this.tableNewRowCount > 0L)
        {
          --this.tableRowCount;
          --this.tableNewRowCount;
        }
      }
      return true;
    }

    internal int SynchronizeTableData(int index, TypeOfOperation typeOp)
    {
      lock (this.SynchObj)
      {
        switch (typeOp)
        {
          case TypeOfOperation.Update:
            int index1 = this.PositionCache((long) index);
            IVistaDBRow vistaDbRow1 = this[index1];
            try
            {
              this.table.CurrentKey = vistaDbRow1;
              if (this.table.CurrentKey.CompareKey(vistaDbRow1) != 0)
                throw new VistaDBDataTableException(2016);
              this.table.CurrentRow = this.insertedRow;
              this.table.Post();
              return 1;
            }
            catch (VistaDBException ex)
            {
              throw new VistaDBDataTableException((Exception) ex, 2019);
            }
            finally
            {
              if (!this.optimisticLock)
                this.table.Unlock(vistaDbRow1.RowId);
              this.inserterRowNumber = -1L;
              this.ReReadCache(index1, vistaDbRow1, this.table.CurrentKey, typeOp);
            }
          case TypeOfOperation.Insert | TypeOfOperation.Update:
            bool flag = false;
            try
            {
              this.table.Insert();
              this.table.CurrentRow = this.insertedRow;
              this.table.Post();
              IVistaDBRow currentKey = this.table.CurrentKey;
              this.table.CurrentKey = this.table.CurrentKey;
              if (this.table.EndOfTable || this.table.StartOfTable || this.table.CurrentKey.CompareKey(currentKey) != 0)
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
              this.maxRowIndex = flag ? (long) (index - 1) : (long) index;
              this.table.Last();
              this.Clear();
              int num = 0;
              for (int capacity = this.Capacity; num < capacity; ++num)
              {
                if (this.table.StartOfTable)
                {
                  this.minRowIndex = 0L;
                  this.maxRowIndex = this.minRowIndex + (long) this.Count - 1L;
                  break;
                }
                this.Insert(0, this.table.CurrentKey);
                this.hashTable.Add(this[0], this.table.CurrentRow);
                this.table.Prev();
                this.minRowIndex = this.maxRowIndex - (long) this.Count + 1L;
              }
            }
          case TypeOfOperation.Delete:
            int index2 = this.PositionCache((long) index);
            IVistaDBRow vistaDbRow2 = this[index2];
            this.table.CurrentKey = vistaDbRow2;
            if (this.table.CurrentKey.CompareKey(vistaDbRow2) != 0)
              throw new VistaDBDataTableException(2016);
            try
            {
              this.table.Delete();
              return 3;
            }
            catch (VistaDBException ex)
            {
              throw new VistaDBDataTableException((Exception) ex, 2022);
            }
            finally
            {
              this.ReReadCache(index2, vistaDbRow2, this.table.CurrentKey, typeOp);
              this.inserterRowNumber = -1L;
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
        if (!this.table.Find(keyExpr, idxName, partialMaching, softPos))
          return false;
        this.Clear();
        this.inserterRowNumber = -1L;
        for (int index = 0; index < this.Capacity && !this.table.EndOfTable; ++index)
        {
          this.Add(this.table.CurrentKey);
          this.hashTable.Add(this[this.Count - 1], this.table.CurrentRow);
          this.table.Next();
        }
        this.minRowIndex = 0L;
        this.maxRowIndex = (long) (this.Count - 1);
        this.checkBOT = false;
        this.tableNewRowCount = (long) this.Count;
        return true;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2017);
      }
      catch (Exception ex)
      {
      }
      return false;
    }

    internal long ChangeActiveIndex(long rowIndex, string activeIndex)
    {
      lock (this.SynchObj)
      {
        try
        {
          this.checkBOT = false;
          this.checkEOT = false;
          int index1 = this.PositionCache(rowIndex);
          if (index1 > -1)
            this.table.CurrentKey = this[index1];
          this.table.ActiveIndex = activeIndex;
          this.Clear();
          if (index1 < 0)
            return -1;
          this.Add(this.table.CurrentKey);
          this.hashTable.Add(this[0], this.table.CurrentRow);
          int num = 0;
          for (int index2 = index1 - 1; index2 >= 0; --index2)
          {
            this.table.Prev();
            if (this.table.StartOfTable)
            {
              this.minRowIndex = 0L;
              this.maxRowIndex = (long) (this.Count - 1);
              return (long) num;
            }
            this.Insert(0, this.table.CurrentKey);
            this.hashTable.Add(this[0], this.table.CurrentRow);
            ++num;
          }
          this.table.CurrentKey = this[this.Count - 1];
          for (int index2 = index1 + 1; index2 < this.Capacity; ++index2)
          {
            this.table.Next();
            if (this.table.EndOfTable)
            {
              this.CalculateMinMaxRows();
              if (this.Count == 0)
                throw new Exception("ERROR");
              return this.minRowIndex + (long) num;
            }
            this.Add(this.table.CurrentKey);
            this.hashTable.Add(this[index2], this.table.CurrentRow);
          }
          this.CalculateMinMaxRows();
          if (this.Count == 0)
            throw new Exception("ERROR");
          return (long) num + this.minRowIndex;
        }
        catch (VistaDBException ex)
        {
          throw new VistaDBDataTableException((Exception) ex, 2018);
        }
        finally
        {
          this.inserterRowNumber = -1L;
          this.checkBOT = true;
          this.checkEOT = true;
        }
      }
    }

    internal bool TableFiltered
    {
      get
      {
        return this.tableFiltered;
      }
      set
      {
        this.tableFiltered = value;
      }
    }

    internal bool IsInserting
    {
      get
      {
        return this.inserting;
      }
      set
      {
        this.inserting = value;
      }
    }

    internal long TableRowCount
    {
      get
      {
        return this.tableRowCount;
      }
    }

    internal bool OptimisticLock
    {
      get
      {
        return this.optimisticLock;
      }
      set
      {
        this.optimisticLock = value;
      }
    }

    public new void Clear()
    {
      this.hashTable.Clear();
      base.Clear();
    }

    public new void RemoveAt(int index)
    {
      this.hashTable.Remove(this[index]);
      base.RemoveAt(index);
    }

    internal IVistaDBRow InsertedRow
    {
      get
      {
        return this.insertedRow;
      }
    }
  }
}
