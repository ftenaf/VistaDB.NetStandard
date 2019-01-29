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
      this.tableAlias = alias == null ? "" : alias;
      this.rowAvailable = true;
      this.dataVersion = 0L;
      this.schema = (IQuerySchemaInfo) null;
      this.parent = parent;
      this.collectionOrder = collectionOrder;
      this.lineNo = lineNo;
      this.symbolNo = symbolNo;
      this.stopNext = false;
      this.rowUpdated = true;
      this.readOnly = true;
      this.nextTable = (SourceTable) null;
      this.joinedTables = (List<SourceTable>) null;
      this.alwaysAllowNull = false;
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
      return (IRow) null;
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
      if (this.nextTable == null)
        return true;
      if (!this.nextTable.Opened)
        this.nextTable.Open();
      return this.nextTable.First(constraints);
    }

    public object GetValue(int colIndex)
    {
      if (this.rowAvailable && !this.Eof)
        return ((IValue) this.SimpleGetColumn(colIndex)).Value;
      return (object) null;
    }

    public void Unprepare()
    {
      this.schema = (IQuerySchemaInfo) null;
    }

    public void Open()
    {
      if (this.Opened)
        return;
      this.OnOpen(this.readOnly);
    }

    public bool First(ConstraintOperations constraints)
    {
      this.rowUpdated = true;
      ++this.dataVersion;
      this.ResetOptimization();
      this.notEmpty = constraints == null || !constraints.ActivateOptimizedFilter(this.collectionOrder);
      this.rowAvailable = this.notEmpty && this.OnFirst();
      this.SyncNextTables(constraints);
      return this.rowAvailable;
    }

    public void Insert()
    {
      this.InternalInsert();
    }

    public void PutValue(int columnIndex, IColumn columnValue)
    {
      this.InternalPutValue(columnIndex, columnValue);
      ++this.dataVersion;
    }

    public void DeleteRow()
    {
      this.InternalDeleteRow();
      this.stopNext = true;
      this.rowUpdated = true;
      ++this.dataVersion;
    }

    public void AddJoinedTable(SourceTable table)
    {
      if (this.joinedTables == null)
        this.joinedTables = new List<SourceTable>();
      else if (this.joinedTables.IndexOf(table) >= 0)
        return;
      this.joinedTables.Add(table);
      table.AddJoinedTable(this);
    }

    internal void SetJoinOptimizationColumns(ColumnSignature leftColumn, ColumnSignature rightColumn, string indexName, bool useCache)
    {
      this.optimizedIndexColumn = (ColumnSignature) null;
      this.optimizedKeyColumn = (ColumnSignature) null;
      this.optimizedIndexName = (string) null;
      if (string.IsNullOrEmpty(this.tableAlias))
        return;
      bool flag1 = !string.IsNullOrEmpty(leftColumn.TableAlias) && leftColumn.TableAlias == this.tableAlias;
      bool flag2 = !string.IsNullOrEmpty(rightColumn.TableAlias) && rightColumn.TableAlias == this.tableAlias;
      if (flag1)
      {
        if (!flag2)
        {
          this.optimizedIndexColumn = leftColumn;
          this.optimizedKeyColumn = rightColumn;
        }
      }
      else if (flag2)
      {
        this.optimizedIndexColumn = rightColumn;
        this.optimizedKeyColumn = leftColumn;
      }
      this.optimizedIndexName = indexName;
      this.optimizedCaching = useCache;
    }

    internal void ClearJoinOptimizationColumns()
    {
      this.optimizedIndexColumn = (ColumnSignature) null;
      this.optimizedKeyColumn = (ColumnSignature) null;
      this.optimizedIndexName = (string) null;
      this.optimizedCaching = false;
    }

    internal void ClearJoinOptimizationCaching()
    {
      this.optimizedCaching = false;
    }

    internal virtual bool ActivateOptimizedConstraints(out bool emptyResultSet)
    {
      this.ClearJoinOptimizationColumns();
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
        return this.tableAlias;
      }
    }

    public string TableName
    {
      get
      {
        return this.tableName;
      }
    }

    public IQuerySchemaInfo Schema
    {
      get
      {
        return this.schema;
      }
    }

    public int CollectionOrder
    {
      get
      {
        return this.collectionOrder;
      }
      internal set
      {
        this.collectionOrder = value;
      }
    }

    public long Version
    {
      get
      {
        return this.dataVersion;
      }
    }

    public int LineNo
    {
      get
      {
        return this.lineNo;
      }
    }

    public int SymbolNo
    {
      get
      {
        return this.symbolNo;
      }
    }

    public Statement Parent
    {
      get
      {
        return this.parent;
      }
    }

    public bool ReadOnly
    {
      get
      {
        return this.readOnly;
      }
      set
      {
        this.readOnly = value;
      }
    }

    internal ColumnSignature OptimizedIndexColumn
    {
      get
      {
        return this.optimizedIndexColumn;
      }
    }

    internal ColumnSignature OptimizedKeyColumn
    {
      get
      {
        return this.optimizedKeyColumn;
      }
    }

    internal string OptimizedIndexName
    {
      get
      {
        return this.optimizedIndexName;
      }
    }

    internal bool OptimizedCaching
    {
      get
      {
        return this.optimizedCaching;
      }
    }

    public bool Next(ConstraintOperations constraints)
    {
      this.rowUpdated = true;
      ++this.dataVersion;
      if (this.stopNext)
      {
        this.stopNext = false;
        this.rowAvailable = this.notEmpty && !this.Eof;
      }
      else
        this.rowAvailable = this.notEmpty && this.OnNext();
      this.SyncNextTables(constraints);
      return this.rowAvailable;
    }

    public bool ExecuteRowset(ConstraintOperations constraints)
    {
      this.rowAvailable = this.notEmpty && !this.Eof;
      this.rowUpdated = false;
      return this.rowAvailable;
    }

    public void MarkRowNotAvailable()
    {
      this.rowAvailable = false;
      this.rowUpdated = true;
    }

    public bool IsEquals(IRowSet rowSet)
    {
      if (this.GetType() != rowSet.GetType())
        return false;
      SourceTable sourceTable = (SourceTable) rowSet;
      if (this.parent.Connection.CompareString(this.tableAlias, sourceTable.tableAlias, true) == 0)
        return this.parent.Connection.CompareString(this.tableName, sourceTable.tableName, true) == 0;
      return false;
    }

    public void Prepare()
    {
      if (this.schema != null)
        return;
      this.schema = this.InternalPrepare();
    }

    public bool Optimize(ConstraintOperations constrainOperations)
    {
      return true;
    }

    public void SetUpdated()
    {
      this.rowUpdated = true;
    }

    public void ClearUpdated()
    {
      this.rowUpdated = false;
    }

    public bool RowAvailable
    {
      get
      {
        return this.rowAvailable;
      }
    }

    public bool RowUpdated
    {
      get
      {
        return this.rowUpdated;
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
      SourceTable sourceTableByName = this.CreateSourceTableByName(tableNames, views);
      sourceTableByName.collectionOrder = tableIndex;
      if (tableList != null)
      {
        if (tableIndex > 0)
          tableList[tableIndex - 1].nextTable = sourceTableByName;
        tableList.AddTable(sourceTableByName);
        tableIndex = tableList.Count;
      }
      this.alwaysAllowNull = alwaysAllowNull;
      return (IRowSet) sourceTableByName;
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
      return (IOptimizedFilter) null;
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
      this.ActiveIndex = (string) null;
    }

    internal virtual IVistaDBIndexCollection TemporaryIndexes
    {
      get
      {
        return (IVistaDBIndexCollection) null;
      }
    }

    internal void SetNextTable(SourceTable table)
    {
      this.nextTable = table;
    }
  }
}
