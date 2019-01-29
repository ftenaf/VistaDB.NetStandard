using System;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal class TableCache<TKey> : WeakReferenceCache<TKey, object[]>, ITableCache
  {
    private uint[] m_RegisteredColumns = new uint[0];
    private const int DefaultCapacity = 100;
    private string m_TableName;
    private List<ColumnCache> m_ColumnCacheList;
    private IVistaDBDatabase m_Database;
    private IVistaDBTable m_Table;
    private List<int> m_RegisteredColumnsList;

    public string FilterFormatString { get; set; }

    public TableCache(IVistaDBDatabase database, string tableName, string filterFormatString)
      : base(100)
    {
      this.Initialize(database, tableName, filterFormatString);
    }

    public TableCache(IVistaDBDatabase database, string tableName, string filterFormatString, CultureInfo culture, bool caseInsensitive)
      : base(100, (IEqualityComparer<TKey>) StringComparer.Create(culture, caseInsensitive))
    {
      this.Initialize(database, tableName, filterFormatString);
    }

    private void Initialize(IVistaDBDatabase database, string tableName, string filterFormatString)
    {
      this.m_Database = database;
      this.m_TableName = tableName;
      this.FilterFormatString = filterFormatString;
      if (database != null)
        this.m_ColumnCacheList = new List<ColumnCache>();
      else
        this.m_ColumnCacheList = (List<ColumnCache>) null;
    }

    public ColumnCache GetColumnCache(string resultColumnName)
    {
      foreach (ColumnCache columnCache in this.m_ColumnCacheList)
      {
        if (string.Compare(resultColumnName, columnCache.ResultColumnName, StringComparison.InvariantCultureIgnoreCase) == 0)
          return columnCache;
      }
      int count = this.m_ColumnCacheList.Count;
      ColumnCache columnCache1 = new ColumnCache((ITableCache) this, resultColumnName, count);
      this.m_ColumnCacheList.Add(columnCache1);
      return columnCache1;
    }

    public void RegisterColumnSignature(int columnIndex)
    {
      if (this.m_RegisteredColumns == null)
        return;
      if (columnIndex < 0)
      {
        this.m_RegisteredColumns = (uint[]) null;
        this.m_RegisteredColumnsList = (List<int>) null;
        this.Clear();
      }
      else
      {
        int index1 = columnIndex / 32;
        int num1 = columnIndex % 32;
        if (index1 >= this.m_RegisteredColumns.Length)
        {
          uint[] numArray = new uint[index1 + 1];
          for (int index2 = 0; index2 < this.m_RegisteredColumns.Length; ++index2)
            numArray[index2] = this.m_RegisteredColumns[index2];
          this.m_RegisteredColumns = numArray;
        }
        uint num2 = (uint) (1 << num1);
        if (((int) this.m_RegisteredColumns[index1] & (int) num2) != 0)
          return;
        this.m_RegisteredColumns[index1] |= num2;
        this.m_RegisteredColumnsList = (List<int>) null;
        this.Clear();
      }
    }

    public bool IsColumnSignatureRegistered(int columnIndex)
    {
      if (this.m_RegisteredColumns == null)
        return true;
      int index = columnIndex / 32;
      int num = columnIndex % 32;
      if (index >= this.m_RegisteredColumns.Length)
        return false;
      return ((int) this.m_RegisteredColumns[index] & 1 << num) != 0;
    }

    public IEnumerable<int> GetRegisteredColumns()
    {
      if (this.m_RegisteredColumns == null)
        return (IEnumerable<int>) null;
      if (this.m_RegisteredColumnsList == null)
      {
        List<int> intList = new List<int>();
        int num1 = 0;
        for (int index = 0; index < this.m_RegisteredColumns.Length; ++index)
        {
          uint num2 = 1;
          while (num2 != 0U)
          {
            if (((int) this.m_RegisteredColumns[index] & (int) num2) != 0)
              intList.Add(num1);
            ++num1;
            num2 <<= 1;
          }
        }
        this.m_RegisteredColumnsList = intList;
      }
      return (IEnumerable<int>) this.m_RegisteredColumnsList;
    }

    public object[] GetValues(object key)
    {
      if (key == null)
        return (object[]) null;
      return this[(TKey) key];
    }

    public void SetValues(object key, object[] values)
    {
      if (key == null)
        return;
      this[(TKey) key] = values;
    }

    public override object[] FetchValue(TKey key)
    {
      if (this.m_Database == null)
        return (object[]) null;
      if (this.m_Table == null)
        this.m_Table = this.m_Database.OpenTable(this.m_TableName, false, true);
      this.m_Table.ResetFilter();
      this.m_Table.SetFilter(typeof (TKey) == typeof (string) ? string.Format(this.FilterFormatString, (object) key.ToString().Replace("'", "''")) : string.Format(this.FilterFormatString, (object) key), true);
      this.m_Table.First();
      if (this.m_Table.EndOfTable)
        return new object[0];
      object[] objArray = new object[this.m_ColumnCacheList.Count];
      for (int index = 0; index < this.m_ColumnCacheList.Count; ++index)
      {
        IVistaDBValue vistaDbValue = this.m_Table.Get(this.m_ColumnCacheList[index].ResultColumnName);
        objArray[index] = vistaDbValue == null ? (object) null : vistaDbValue.Value;
      }
      return objArray;
    }

    public void Close()
    {
      if (this.m_Table == null)
        return;
      this.m_Table.Close();
      this.m_Database = (IVistaDBDatabase) null;
      this.m_Table = (IVistaDBTable) null;
    }
  }
}
