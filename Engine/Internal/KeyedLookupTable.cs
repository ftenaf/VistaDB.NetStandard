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
      this.m_LookupTable = lookupTable;
      this.m_KeyColumn = keyColumn;
      this.m_ParentStatement = parent;
      this.m_CurrentKeyTableVersion = -1L;
    }

    internal QuickJoinLookupColumn GetLookupColumn(ColumnSignature originalColumn)
    {
      int dataIndex = this.RegisterColumn(originalColumn.ColumnName);
      return new QuickJoinLookupColumn(originalColumn, this.m_ParentStatement, this, dataIndex);
    }

    internal long TableVersion
    {
      get
      {
        if (!this.GetIsChanged())
          return this.m_CurrentKeyTableVersion;
        return -1;
      }
    }

    internal bool GetIsChanged()
    {
      if (this.m_CurrentKeyTableVersion >= 0L)
        return this.m_KeyColumn.TableVersion != this.m_CurrentKeyTableVersion;
      return true;
    }

    private object[] InternalExecute()
    {
      if (!this.m_KeyColumn.Table.Opened)
        return (object[]) null;
      if (this.GetIsChanged())
      {
        this.m_CurrentKeyValue = ((IValue) this.m_KeyColumn.Execute()).Value;
        this.m_CurrentKeyTableVersion = this.m_KeyColumn.TableVersion;
        if (this.m_LoadedKeyValue == null || !this.m_LoadedKeyValue.Equals(this.m_CurrentKeyValue))
        {
          this.m_CurrentDataValues = this.m_LookupTable.GetValues(this.m_CurrentKeyValue);
          this.m_LoadedKeyValue = this.m_CurrentKeyValue;
        }
      }
      return this.m_CurrentDataValues;
    }

    internal object GetValue(int dataIndex)
    {
      this.InternalExecute();
      if (this.m_CurrentDataValues == null || this.m_CurrentDataValues.Length <= dataIndex)
        return (object) null;
      return this.m_CurrentDataValues[dataIndex];
    }

    internal object[] GetValues()
    {
      this.InternalExecute();
      return this.m_CurrentDataValues;
    }

    internal object GetKeyValue()
    {
      this.InternalExecute();
      return this.m_CurrentKeyValue;
    }

    internal void SetValues(object[] values)
    {
      this.InternalExecute();
      if (object.ReferenceEquals((object) values, (object) this.m_CurrentDataValues))
        return;
      this.m_LookupTable.SetValues(this.m_CurrentKeyValue, values);
      this.m_CurrentDataValues = values;
    }

    internal int RegisterColumn(string columnName)
    {
      return this.m_LookupTable.GetColumnCache(columnName).ResultColumnIndex;
    }

    internal void RegisterColumnSignature(int columnIndex)
    {
      this.m_LookupTable.RegisterColumnSignature(columnIndex);
    }

    internal bool IsColumnSignatureRegistered(int columnIndex)
    {
      return this.m_LookupTable.IsColumnSignatureRegistered(columnIndex);
    }

    internal IEnumerable<int> GetRegisteredColumns()
    {
      return this.m_LookupTable.GetRegisteredColumns();
    }
  }
}
