using System;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal class TableCache<TKey> : WeakReferenceCache<TKey, object[]>, ITableCache
  {
    private uint[] m_RegisteredColumns = new uint[0];
        private string m_TableName;
    private List<ColumnCache> m_ColumnCacheList;
    private IVistaDBDatabase m_Database;
    private IVistaDBTable m_Table;
    private List<int> m_RegisteredColumnsList;

    public string FilterFormatString { get; set; }

    public TableCache(IVistaDBDatabase database, string tableName, string filterFormatString)
      : base(100)
    {
      Initialize(database, tableName, filterFormatString);
    }

    public TableCache(IVistaDBDatabase database, string tableName, string filterFormatString, CultureInfo culture, bool caseInsensitive)
      : base(100, (IEqualityComparer<TKey>) StringComparer.Create(culture, caseInsensitive))
    {
      Initialize(database, tableName, filterFormatString);
    }

    private void Initialize(IVistaDBDatabase database, string tableName, string filterFormatString)
    {
      m_Database = database;
      m_TableName = tableName;
      FilterFormatString = filterFormatString;
      if (database != null)
        m_ColumnCacheList = new List<ColumnCache>();
      else
        m_ColumnCacheList = null;
    }

    public ColumnCache GetColumnCache(string resultColumnName)
    {
      foreach (ColumnCache columnCache in m_ColumnCacheList)
      {
        if (string.Compare(resultColumnName, columnCache.ResultColumnName, StringComparison.InvariantCultureIgnoreCase) == 0)
          return columnCache;
      }
      int count = m_ColumnCacheList.Count;
      ColumnCache columnCache1 = new ColumnCache(this, resultColumnName, count);
      m_ColumnCacheList.Add(columnCache1);
      return columnCache1;
    }

    public void RegisterColumnSignature(int columnIndex)
    {
      if (m_RegisteredColumns == null)
        return;
      if (columnIndex < 0)
      {
        m_RegisteredColumns = null;
        m_RegisteredColumnsList = null;
        Clear();
      }
      else
      {
        int index1 = columnIndex / 32;
        int num1 = columnIndex % 32;
        if (index1 >= m_RegisteredColumns.Length)
        {
          uint[] numArray = new uint[index1 + 1];
          for (int index2 = 0; index2 < m_RegisteredColumns.Length; ++index2)
            numArray[index2] = m_RegisteredColumns[index2];
          m_RegisteredColumns = numArray;
        }
        uint num2 = (uint) (1 << num1);
        if (((int) m_RegisteredColumns[index1] & (int) num2) != 0)
          return;
        m_RegisteredColumns[index1] |= num2;
        m_RegisteredColumnsList = null;
        Clear();
      }
    }

    public bool IsColumnSignatureRegistered(int columnIndex)
    {
      if (m_RegisteredColumns == null)
        return true;
      int index = columnIndex / 32;
      int num = columnIndex % 32;
      if (index >= m_RegisteredColumns.Length)
        return false;
      return ((int) m_RegisteredColumns[index] & 1 << num) != 0;
    }

    public IEnumerable<int> GetRegisteredColumns()
    {
      if (m_RegisteredColumns == null)
        return null;
      if (m_RegisteredColumnsList == null)
      {
        List<int> intList = new List<int>();
        int num1 = 0;
        for (int index = 0; index < m_RegisteredColumns.Length; ++index)
        {
          uint num2 = 1;
          while (num2 != 0U)
          {
            if (((int) m_RegisteredColumns[index] & (int) num2) != 0)
              intList.Add(num1);
            ++num1;
            num2 <<= 1;
          }
        }
        m_RegisteredColumnsList = intList;
      }
      return m_RegisteredColumnsList;
    }

    public object[] GetValues(object key)
    {
      if (key == null)
        return null;
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
      if (m_Database == null)
        return null;
      if (m_Table == null)
        m_Table = m_Database.OpenTable(m_TableName, false, true);
      m_Table.ResetFilter();
      m_Table.SetFilter(typeof (TKey) == typeof (string) ? string.Format(FilterFormatString, key.ToString().Replace("'", "''")) : string.Format(FilterFormatString, key), true);
      m_Table.First();
      if (m_Table.EndOfTable)
        return new object[0];
      object[] objArray = new object[m_ColumnCacheList.Count];
      for (int index = 0; index < m_ColumnCacheList.Count; ++index)
      {
        IVistaDBValue vistaDbValue = m_Table.Get(m_ColumnCacheList[index].ResultColumnName);
        objArray[index] = vistaDbValue == null ? null : vistaDbValue.Value;
      }
      return objArray;
    }

    public void Close()
    {
      if (m_Table == null)
        return;
      m_Table.Close();
      m_Database = null;
      m_Table = null;
    }
  }
}
