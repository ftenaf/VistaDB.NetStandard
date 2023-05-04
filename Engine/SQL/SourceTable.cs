using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal abstract class SourceTable : IRowSet
  {
    private bool notEmpty = true;
    protected Statement parent;
    protected string tableName;
    protected string tableAlias;
    protected int collectionOrder;
    protected int lineNo;
    protected int symbolNo;
    protected long dataVersion;
    private IQuerySchemaInfo schema;
    protected bool stopNext;
    private bool rowUpdated;
    private bool rowAvailable;
    private bool readOnly;
    private SourceTable nextTable;
    private List<SourceTable> joinedTables;
    protected bool alwaysAllowNull;
    private ColumnSignature optimizedIndexColumn;
    private ColumnSignature optimizedKeyColumn;
    private string optimizedIndexName;
    private bool optimizedCaching;

    protected SourceTable(Statement parent, string tableName, string alias, int collectionOrder, int lineNo, int symbolNo)
    {
      this.tableName = tableName;
      tableAlias = alias == null ? "" : alias;
      rowAvailable = true;
      dataVersion = 0L;
      schema = null;
      this.parent = parent;
      this.collectionOrder = collectionOrder;
      this.lineNo = lineNo;
      this.symbolNo = symbolNo;
      stopNext = false;
      rowUpdated = true;
      readOnly = true;
      nextTable = null;
      joinedTables = null;
      alwaysAllowNull = false;
    }

    public abstract IColumn SimpleGetColumn(int colIndex);

    public abstract void Post();

    public abstract void Close();

    public abstract void FreeTable();

    public abstract IVistaDBTableSchema GetTableSchema();

    public abstract IColumn GetLastIdentity(string columnName);

    public abstract string CreateIndex(string expression, bool instantly);

    public abstract int GetColumnCount();

    protected abstract void OnOpen(bool readOnly);

    protected abstract bool OnFirst();

    protected abstract bool OnNext();

    protected abstract IQuerySchemaInfo InternalPrepare();

    protected abstract void InternalInsert();

    protected abstract void InternalPutValue(int columnIndex, IColumn columnValue);

    protected abstract void InternalDeleteRow();

    public abstract bool Eof { get; }

    public abstract bool IsNativeTable { get; }

    public abstract bool Opened { get; }

    public abstract bool IsUpdatable { get; }

    internal virtual IRow DoGetIndexStructure(string indexName)
    {
      return null;
    }

    protected virtual SourceTable CreateSourceTableByName(IVistaDBTableNameCollection tableNames, IViewList views)
    {
      return this;
    }

    internal virtual long GetOptimizedRowCount(string colName)
    {
      return -1;
    }

    internal virtual void DoOpenExternalRelationships(bool insert, bool delete)
    {
    }

    internal virtual void DoFreeExternalRelationships()
    {
    }

    internal virtual void DoFreezeSelfRelationships()
    {
    }

    internal virtual void DoDefreezeSelfRelationships()
    {
    }

    private bool SyncNextTables(ConstraintOperations constraints)
    {
      if (nextTable == null)
        return true;
      if (!nextTable.Opened)
        nextTable.Open();
      return nextTable.First(constraints);
    }

    public object GetValue(int colIndex)
    {
      if (rowAvailable && !Eof)
        return SimpleGetColumn(colIndex).Value;
      return null;
    }

    public void Unprepare()
    {
      schema = null;
    }

    public void Open()
    {
      if (Opened)
        return;
      OnOpen(readOnly);
    }

    public bool First(ConstraintOperations constraints)
    {
      rowUpdated = true;
      ++dataVersion;
      ResetOptimization();
      notEmpty = constraints == null || !constraints.ActivateOptimizedFilter(collectionOrder);
      rowAvailable = notEmpty && OnFirst();
      SyncNextTables(constraints);
      return rowAvailable;
    }

    public void Insert()
    {
      InternalInsert();
    }

    public void PutValue(int columnIndex, IColumn columnValue)
    {
      InternalPutValue(columnIndex, columnValue);
      ++dataVersion;
    }

    public void DeleteRow()
    {
      InternalDeleteRow();
      stopNext = true;
      rowUpdated = true;
      ++dataVersion;
    }

    public void AddJoinedTable(SourceTable table)
    {
      if (joinedTables == null)
        joinedTables = new List<SourceTable>();
      else if (joinedTables.IndexOf(table) >= 0)
        return;
      joinedTables.Add(table);
      table.AddJoinedTable(this);
    }

    internal void SetJoinOptimizationColumns(ColumnSignature leftColumn, ColumnSignature rightColumn, string indexName, bool useCache)
    {
      optimizedIndexColumn = null;
      optimizedKeyColumn = null;
      optimizedIndexName = null;
      if (string.IsNullOrEmpty(tableAlias))
        return;
      bool flag1 = !string.IsNullOrEmpty(leftColumn.TableAlias) && leftColumn.TableAlias == tableAlias;
      bool flag2 = !string.IsNullOrEmpty(rightColumn.TableAlias) && rightColumn.TableAlias == tableAlias;
      if (flag1)
      {
        if (!flag2)
        {
          optimizedIndexColumn = leftColumn;
          optimizedKeyColumn = rightColumn;
        }
      }
      else if (flag2)
      {
        optimizedIndexColumn = rightColumn;
        optimizedKeyColumn = leftColumn;
      }
      optimizedIndexName = indexName;
      optimizedCaching = useCache;
    }

    internal void ClearJoinOptimizationColumns()
    {
      optimizedIndexColumn = null;
      optimizedKeyColumn = null;
      optimizedIndexName = null;
      optimizedCaching = false;
    }

    internal void ClearJoinOptimizationCaching()
    {
      optimizedCaching = false;
    }

    internal virtual bool ActivateOptimizedConstraints(out bool emptyResultSet)
    {
      ClearJoinOptimizationColumns();
      emptyResultSet = false;
      return false;
    }

    internal virtual void RegisterColumnSignature(int columnIndex)
    {
    }

    public string Alias
    {
      get
      {
        return tableAlias;
      }
    }

    public string TableName
    {
      get
      {
        return tableName;
      }
    }

    public IQuerySchemaInfo Schema
    {
      get
      {
        return schema;
      }
    }

    public int CollectionOrder
    {
      get
      {
        return collectionOrder;
      }
      internal set
      {
        collectionOrder = value;
      }
    }

    public long Version
    {
      get
      {
        return dataVersion;
      }
    }

    public int LineNo
    {
      get
      {
        return lineNo;
      }
    }

    public int SymbolNo
    {
      get
      {
        return symbolNo;
      }
    }

    public Statement Parent
    {
      get
      {
        return parent;
      }
    }

    public bool ReadOnly
    {
      get
      {
        return readOnly;
      }
      set
      {
        readOnly = value;
      }
    }

    internal ColumnSignature OptimizedIndexColumn
    {
      get
      {
        return optimizedIndexColumn;
      }
    }

    internal ColumnSignature OptimizedKeyColumn
    {
      get
      {
        return optimizedKeyColumn;
      }
    }

    internal string OptimizedIndexName
    {
      get
      {
        return optimizedIndexName;
      }
    }

    internal bool OptimizedCaching
    {
      get
      {
        return optimizedCaching;
      }
    }

    public bool Next(ConstraintOperations constraints)
    {
      rowUpdated = true;
      ++dataVersion;
      if (stopNext)
      {
        stopNext = false;
        rowAvailable = notEmpty && !Eof;
      }
      else
        rowAvailable = notEmpty && OnNext();
      SyncNextTables(constraints);
      return rowAvailable;
    }

    public bool ExecuteRowset(ConstraintOperations constraints)
    {
      rowAvailable = notEmpty && !Eof;
      rowUpdated = false;
      return rowAvailable;
    }

    public void MarkRowNotAvailable()
    {
      rowAvailable = false;
      rowUpdated = true;
    }

    public bool IsEquals(IRowSet rowSet)
    {
      if (GetType() != rowSet.GetType())
        return false;
      SourceTable sourceTable = (SourceTable) rowSet;
      if (parent.Connection.CompareString(tableAlias, sourceTable.tableAlias, true) == 0)
        return parent.Connection.CompareString(tableName, sourceTable.tableName, true) == 0;
      return false;
    }

    public void Prepare()
    {
      if (schema != null)
        return;
      schema = InternalPrepare();
    }

    public bool Optimize(ConstraintOperations constrainOperations)
    {
      return true;
    }

    public void SetUpdated()
    {
      rowUpdated = true;
    }

    public void ClearUpdated()
    {
      rowUpdated = false;
    }

    public bool RowAvailable
    {
      get
      {
        return rowAvailable;
      }
    }

    public bool RowUpdated
    {
      get
      {
        return rowUpdated;
      }
    }

    public virtual bool OuterRow
    {
      get
      {
        return false;
      }
    }

    public IRowSet PrepareTables(IVistaDBTableNameCollection tableNames, IViewList views, TableCollection tableList, bool alwaysAllowNull, ref int tableIndex)
    {
      SourceTable sourceTableByName = CreateSourceTableByName(tableNames, views);
      sourceTableByName.collectionOrder = tableIndex;
      if (tableList != null)
      {
        if (tableIndex > 0)
          tableList[tableIndex - 1].nextTable = sourceTableByName;
        tableList.AddTable(sourceTableByName);
        tableIndex = tableList.Count;
      }
      this.alwaysAllowNull = alwaysAllowNull;
      return sourceTableByName;
    }

    internal virtual void PushTemporaryTableCache(TriggerAction triggerAction)
    {
    }

    internal virtual void PopTemporaryTableCache(TriggerAction triggerAction)
    {
    }

    internal virtual void PrepareTriggers(TriggerAction triggerAction)
    {
    }

    internal virtual void ExecuteTriggers(TriggerAction eventType, bool justReset)
    {
    }

    internal virtual IOptimizedFilter BuildFilterMap(string indexName, IRow lowScopeValue, IRow highScopeValue, bool excludeNulls)
    {
      return null;
    }

    internal virtual bool BeginOptimizedFiltering(IOptimizedFilter filter, string pivotIndex)
    {
      return true;
    }

    internal virtual void ResetOptimizedFiltering()
    {
    }

    internal virtual bool SetScope(IRow leftScope, IRow rightScope)
    {
      return true;
    }

    internal virtual string ActiveIndex
    {
      set
      {
      }
    }

    internal virtual void ResetOptimization()
    {
      ActiveIndex = null;
    }

    internal virtual IVistaDBIndexCollection TemporaryIndexes
    {
      get
      {
        return null;
      }
    }

    internal void SetNextTable(SourceTable table)
    {
      nextTable = table;
    }
  }
}
