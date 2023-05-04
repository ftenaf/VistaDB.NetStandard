using System.Collections.Generic;
using VistaDB.Engine.SQL;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.Internal
{
  internal class KeyedLookupTable
  {
    private readonly ITableCache m_LookupTable;
    private readonly ColumnSignature m_KeyColumn;
    private readonly Statement m_ParentStatement;
    private object[] m_CurrentDataValues;
    private object m_CurrentKeyValue;
    private object m_LoadedKeyValue;
    private long m_CurrentKeyTableVersion;

    internal KeyedLookupTable(ITableCache lookupTable, ColumnSignature keyColumn, Statement parent)
    {
      m_LookupTable = lookupTable;
      m_KeyColumn = keyColumn;
      m_ParentStatement = parent;
      m_CurrentKeyTableVersion = -1L;
    }

    internal QuickJoinLookupColumn GetLookupColumn(ColumnSignature originalColumn)
    {
      int dataIndex = RegisterColumn(originalColumn.ColumnName);
      return new QuickJoinLookupColumn(originalColumn, m_ParentStatement, this, dataIndex);
    }

    internal long TableVersion
    {
      get
      {
        if (!GetIsChanged())
          return m_CurrentKeyTableVersion;
        return -1;
      }
    }

    internal bool GetIsChanged()
    {
      if (m_CurrentKeyTableVersion >= 0L)
        return m_KeyColumn.TableVersion != m_CurrentKeyTableVersion;
      return true;
    }

    private object[] InternalExecute()
    {
      if (!m_KeyColumn.Table.Opened)
        return null;
      if (GetIsChanged())
      {
        m_CurrentKeyValue = m_KeyColumn.Execute().Value;
        m_CurrentKeyTableVersion = m_KeyColumn.TableVersion;
        if (m_LoadedKeyValue == null || !m_LoadedKeyValue.Equals(m_CurrentKeyValue))
        {
          m_CurrentDataValues = m_LookupTable.GetValues(m_CurrentKeyValue);
          m_LoadedKeyValue = m_CurrentKeyValue;
        }
      }
      return m_CurrentDataValues;
    }

    internal object GetValue(int dataIndex)
    {
      InternalExecute();
      if (m_CurrentDataValues == null || m_CurrentDataValues.Length <= dataIndex)
        return null;
      return m_CurrentDataValues[dataIndex];
    }

    internal object[] GetValues()
    {
      InternalExecute();
      return m_CurrentDataValues;
    }

    internal object GetKeyValue()
    {
      InternalExecute();
      return m_CurrentKeyValue;
    }

    internal void SetValues(object[] values)
    {
      InternalExecute();
      if (ReferenceEquals(values, m_CurrentDataValues))
        return;
      m_LookupTable.SetValues(m_CurrentKeyValue, values);
      m_CurrentDataValues = values;
    }

    internal int RegisterColumn(string columnName)
    {
      return m_LookupTable.GetColumnCache(columnName).ResultColumnIndex;
    }

    internal void RegisterColumnSignature(int columnIndex)
    {
      m_LookupTable.RegisterColumnSignature(columnIndex);
    }

    internal bool IsColumnSignatureRegistered(int columnIndex)
    {
      return m_LookupTable.IsColumnSignatureRegistered(columnIndex);
    }

    internal IEnumerable<int> GetRegisteredColumns()
    {
      return m_LookupTable.GetRegisteredColumns();
    }
  }
}
