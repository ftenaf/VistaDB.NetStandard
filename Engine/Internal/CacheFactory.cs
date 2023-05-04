using System;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.DDA;
using VistaDB.Engine.SQL;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.Internal
{
  internal class CacheFactory
  {
    private readonly Dictionary<string, ITableCache> m_CachedTables = new Dictionary<string, ITableCache>();
    private readonly Dictionary<string, KeyedLookupTable> m_KeyedTables = new Dictionary<string, KeyedLookupTable>();

    public static CacheFactory Instance { get; private set; }

    static CacheFactory()
    {
            Reset();
    }

    public static void Reset()
    {
      if (Instance != null)
                Instance.Close();
            Instance = new CacheFactory();
    }

    public KeyedLookupTable GetLookupTable(IVistaDBDatabase database, SourceTable table, string activeIndex, ColumnSignature keyColumn)
    {
      KeyedLookupTable keyedLookupTable;
      if (!m_KeyedTables.TryGetValue(table.Alias, out keyedLookupTable))
      {
        ITableCache tableCache = GetTableCache(database, table.TableName, activeIndex, keyColumn.DataType);
        if (tableCache == null)
          return (KeyedLookupTable) null;
        keyedLookupTable = new KeyedLookupTable(tableCache, keyColumn, table.Parent);
        m_KeyedTables[table.Alias] = keyedLookupTable;
      }
      return keyedLookupTable;
    }

    public KeyedLookupTable GetLookupTable(string tableAlias)
    {
      KeyedLookupTable keyedLookupTable;
      if (!m_KeyedTables.TryGetValue(tableAlias, out keyedLookupTable))
        return (KeyedLookupTable) null;
      return keyedLookupTable;
    }

    public ITableCache GetTableCache(IVistaDBDatabase database, string tableName, string indexColumnName, VistaDBType indexType)
    {
      ITableCache tableCache = FindTableCache(tableName, indexColumnName);
      if (tableCache == null)
      {
        string filterFormatString = GetFilterFormatString(indexColumnName, indexType);
        tableCache = CreateTableCache(database, tableName, indexColumnName, indexType, filterFormatString);
      }
      return tableCache;
    }

    public ColumnCache GetColumnCache(IVistaDBDatabase database, string tableName, string indexColumnName, VistaDBType indexType, string returnColumnName)
    {
      ITableCache tableCache = FindTableCache(tableName, indexColumnName);
      if (tableCache == null)
      {
        string filterFormatString = GetFilterFormatString(indexColumnName, indexType);
        tableCache = CreateTableCache(database, tableName, indexColumnName, indexType, filterFormatString);
      }
      return tableCache?.GetColumnCache(returnColumnName);
    }

    public void Close()
    {
      foreach (KeyValuePair<string, ITableCache> cachedTable in m_CachedTables)
        cachedTable.Value.Close();
      m_CachedTables.Clear();
    }

    private ITableCache FindTableCache(string tableName, string indexColumnName)
    {
      ITableCache tableCache;
      if (!m_CachedTables.TryGetValue(GetTableCacheKey(tableName, indexColumnName), out tableCache))
        return (ITableCache) null;
      return tableCache;
    }

    private ITableCache CreateTableCache(IVistaDBDatabase database, string tableName, string indexColumnName, VistaDBType indexType, string filterFormatString)
    {
      string tableCacheKey = GetTableCacheKey(tableName, indexColumnName);
      ITableCache tableCache;
      switch (indexType)
      {
        case VistaDBType.Char:
        case VistaDBType.NChar:
        case VistaDBType.VarChar:
        case VistaDBType.NVarChar:
          tableCache = (ITableCache) new TableCache<string>(database, tableName, filterFormatString, CultureInfo.InvariantCulture, true);
          break;
        case VistaDBType.SmallInt:
          tableCache = (ITableCache) new TableCache<short>(database, tableName, filterFormatString);
          break;
        case VistaDBType.Int:
          tableCache = (ITableCache) new TableCache<int>(database, tableName, filterFormatString);
          break;
        case VistaDBType.BigInt:
          tableCache = (ITableCache) new TableCache<long>(database, tableName, filterFormatString);
          break;
        case VistaDBType.Real:
          tableCache = (ITableCache) new TableCache<float>(database, tableName, filterFormatString);
          break;
        case VistaDBType.Float:
          tableCache = (ITableCache) new TableCache<double>(database, tableName, filterFormatString);
          break;
        case VistaDBType.UniqueIdentifier:
          tableCache = (ITableCache) new TableCache<Guid>(database, tableName, filterFormatString);
          break;
        default:
          return (ITableCache) null;
      }
      m_CachedTables.Add(tableCacheKey, tableCache);
      return tableCache;
    }

    private string GetTableCacheKey(string tableName, string indexColumnName)
    {
      return tableName.ToUpperInvariant() + "." + indexColumnName.ToUpperInvariant();
    }

    private string GetFilterFormatString(string indexColumnName, VistaDBType indexType)
    {
      string str;
      switch (indexType)
      {
        case VistaDBType.Char:
        case VistaDBType.NChar:
        case VistaDBType.VarChar:
        case VistaDBType.NVarChar:
          str = indexColumnName + " = '{0}'";
          break;
        case VistaDBType.UniqueIdentifier:
          str = indexColumnName + " = '{{{0}}}'";
          break;
        default:
          str = indexColumnName + " = {0}";
          break;
      }
      return str;
    }
  }
}
