using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class NativeSourceTable : SourceTable, IQuerySchemaInfo
  {
    private const string StartTempIndexName = "TemporaryIndex";
    private static long IndexCounter;
    private ITable table;
    private string tempIndexExpression;
    private string tempIndexName;
    private IVistaDBTableSchema tableSchema;
    private NativeSourceTable.Relationships relationships;
    private KeyedLookupTable keyedLookupCache;
    private Dictionary<int, bool> columnsToRegister;
    private long optimizedDataVersion;
    private int optimizedDataPosition;
    private object[] optimizedDataValues;
    private Row optimizedDataRow;
    private Row optimizedLeftScope;
    private Row optimizedRightScope;

    public NativeSourceTable(Statement parent, string tableName, string alias, int index, int lineNo, int symbolNo)
      : base(parent, tableName, alias, index, lineNo, symbolNo)
    {
      this.table = (ITable) null;
      this.tempIndexExpression = (string) null;
      this.tempIndexName = (string) null;
      this.tableSchema = (IVistaDBTableSchema) null;
    }

    public NativeSourceTable(Statement parent, ITable table)
      : this(parent, (string) null, (string) null, -1, 0, 0)
    {
      this.table = table;
    }

    internal override long GetOptimizedRowCount(string columnName)
    {
      if (columnName == null || !this.tableSchema[columnName].AllowNull)
        return this.table.RowCount;
      return -1;
    }

    internal override void DoOpenExternalRelationships(bool insert, bool delete)
    {
      if (this.relationships == null)
        this.relationships = new NativeSourceTable.Relationships(this.parent.Database.GetRelationships(this.tableAlias, insert, delete));
      foreach (string name in this.relationships.Names)
        this.relationships[name] = this.parent.Connection.OpenTable(name, this.table.IsExclusive, this.table.IsReadOnly);
    }

    internal override void DoFreeExternalRelationships()
    {
      if (this.relationships == null)
        return;
      foreach (string name in this.relationships.Names)
      {
        this.parent.Connection.FreeTable(this.relationships[name]);
        this.relationships[name] = (ITable) null;
      }
    }

    internal override void DoFreezeSelfRelationships()
    {
      this.table.FreezeSelfRelationships();
    }

    internal override void DoDefreezeSelfRelationships()
    {
      this.table.DefreezeSelfRelationships();
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      if (this.OptimizedCaching && this.optimizedDataValues != null)
        return (IColumn) this.optimizedDataRow[colIndex];
      return (IColumn) this.table.Get(colIndex);
    }

    internal override IRow DoGetIndexStructure(string indexName)
    {
      return this.table.KeyStructure(indexName);
    }

    public override void Post()
    {
      if (this.parent.ConstraintOperations != null)
      {
        IRow row = this.table.CurrentKey.CopyInstance();
        this.InternalPost();
        this.table.CurrentKey = row;
        this.stopNext = this.table.CurrentKey.CompareKey((IVistaDBRow) row) > 0;
      }
      else
        this.InternalPost();
    }

    private void InternalPost()
    {
      if (this.parent.Connection.GetSynchronization())
        this.table.Post();
      else
        ((IVistaDBTable) this.table).Post();
    }

    public override void Close()
    {
      this.DoFreeExternalRelationships();
      if (this.table == null)
        return;
      this.parent.Connection.CloseTable(this.table);
      this.table = (ITable) null;
      this.tempIndexExpression = (string) null;
      if (this.relationships != null)
        this.relationships.Dispose();
      this.relationships = (NativeSourceTable.Relationships) null;
    }

    public override void FreeTable()
    {
      if (this.table == null)
        return;
      this.parent.Connection.FreeTable(this.table);
      this.table = (ITable) null;
      this.tempIndexExpression = (string) null;
    }

    public override IVistaDBTableSchema GetTableSchema()
    {
      if (this.parent.Connection.CompareString(this.tableName, Database.SystemSchema, true) == 0)
        return this.parent.Database.TableSchema((string) null);
      return this.parent.Database.TableSchema(this.tableName);
    }

    public override IColumn GetLastIdentity(string columnName)
    {
      if (this.table != null)
        return (IColumn) this.parent.Database.GetLastIdentity(this.tableName, columnName);
      return (IColumn) null;
    }

    public override string CreateIndex(string expression, bool instantly)
    {
      if (this.parent.Connection.CompareString(this.tableName, Database.SystemSchema, true) == 0)
        return (string) null;
      if (this.tempIndexName == null || !this.parent.Connection.IsIndexExisting(this.tableName, this.tempIndexName))
      {
        this.tempIndexExpression = expression;
        this.tempIndexName = "TemporaryIndex" + NativeSourceTable.IndexCounter++.ToString();
      }
      if (instantly)
        this.table.CreateTemporaryIndex(this.tempIndexName, this.tempIndexExpression, false);
      return this.tempIndexName;
    }

    public override int GetColumnCount()
    {
      return this.ColumnCount;
    }

    internal override void PushTemporaryTableCache(TriggerAction triggerAction)
    {
    }

    internal override void PopTemporaryTableCache(TriggerAction triggerAction)
    {
    }

    internal override void PrepareTriggers(TriggerAction triggerAction)
    {
      this.table.PrepareTriggers(triggerAction);
    }

    internal override void ExecuteTriggers(TriggerAction eventType, bool justReset)
    {
      this.table.ExecuteTriggers(eventType, justReset);
    }

    internal override IOptimizedFilter BuildFilterMap(string indexName, IRow lowScopeValue, IRow highScopeValue, bool excludeNulls)
    {
      return this.table.BuildFilterMap(indexName, lowScopeValue, highScopeValue, excludeNulls);
    }

    internal override bool BeginOptimizedFiltering(IOptimizedFilter filter, string pivotIndex)
    {
      this.table.BeginOptimizedFiltering(filter, pivotIndex);
      this.table.PrepareFtsOptimization();
      return false;
    }

    internal override void ResetOptimizedFiltering()
    {
      this.table.ResetOptimizedFiltering();
    }

    internal override bool SetScope(IRow leftScope, IRow rightScope)
    {
      this.table.SetScope((IVistaDBRow) leftScope, (IVistaDBRow) rightScope);
      this.table.PrepareFtsOptimization();
      return false;
    }

    internal override void ResetOptimization()
    {
      if (this.table == null)
        return;
      this.table.ResetOptimizedFiltering();
      base.ResetOptimization();
    }

    internal override IVistaDBIndexCollection TemporaryIndexes
    {
      get
      {
        if (this.table != null)
          return this.table.TemporaryIndexes;
        return (IVistaDBIndexCollection) null;
      }
    }

    internal override string ActiveIndex
    {
      set
      {
        this.table.ActiveIndex = value;
      }
    }

    internal override void RegisterColumnSignature(int columnIndex)
    {
      if (this.keyedLookupCache != null)
      {
        this.keyedLookupCache.RegisterColumnSignature(columnIndex);
      }
      else
      {
        if (this.columnsToRegister == null)
          this.columnsToRegister = new Dictionary<int, bool>();
        else if (this.columnsToRegister.ContainsKey(-1))
          return;
        if (columnIndex < 0)
        {
          this.columnsToRegister.Clear();
          this.columnsToRegister.Add(-1, true);
        }
        else
          this.columnsToRegister[columnIndex] = true;
      }
    }

    internal override bool ActivateOptimizedConstraints(out bool emptyResultSet)
    {
      emptyResultSet = false;
      if ((Signature) this.OptimizedIndexColumn == (Signature) null || (Signature) this.OptimizedKeyColumn == (Signature) null)
        return false;
      if (string.IsNullOrEmpty(this.OptimizedIndexName))
      {
        this.ClearJoinOptimizationColumns();
        return false;
      }
      this.optimizedDataPosition = int.MinValue;
      this.optimizedDataValues = (object[]) null;
      if (this.OptimizedCaching)
      {
        if (this.keyedLookupCache == null)
        {
          SelectStatement parent = this.Parent as SelectStatement;
          CacheFactory cacheFactory = parent == null ? (CacheFactory) null : parent.CacheFactory;
          if (cacheFactory != null)
          {
            KeyedLookupTable lookupTable = cacheFactory.GetLookupTable((IVistaDBDatabase) null, (SourceTable) this, this.OptimizedIndexName, this.OptimizedKeyColumn);
            if (lookupTable != null)
            {
              this.keyedLookupCache = lookupTable;
              if (this.columnsToRegister != null && this.columnsToRegister.Count > 0)
              {
                foreach (int key in this.columnsToRegister.Keys)
                  this.keyedLookupCache.RegisterColumnSignature(key);
              }
              this.columnsToRegister = (Dictionary<int, bool>) null;
            }
          }
        }
        if (this.keyedLookupCache == null)
        {
          this.ClearJoinOptimizationCaching();
        }
        else
        {
          this.optimizedDataVersion = -1L;
          this.optimizedDataValues = this.keyedLookupCache.GetValues();
          if (this.optimizedDataValues != null)
          {
            if (this.optimizedDataRow == null)
              this.optimizedDataRow = (Row) this.table.CurrentRow.CopyInstance();
            int count = this.optimizedDataRow.Count;
            if (this.optimizedDataValues.Length > 0)
            {
              this.optimizedDataPosition = 0;
              for (int index = 0; index < count; ++index)
                this.optimizedDataRow[index].Value = this.optimizedDataValues[index];
            }
            else
            {
              emptyResultSet = true;
              this.optimizedDataPosition = int.MaxValue;
              for (int index = 0; index < count; ++index)
                this.optimizedDataRow[index].Value = (object) null;
            }
            this.optimizedDataVersion = this.dataVersion;
          }
        }
      }
      this.table.ActiveIndex = this.OptimizedIndexName;
      if (this.optimizedDataValues == null)
      {
        if (this.optimizedLeftScope == null || this.optimizedRightScope == null)
        {
          this.optimizedLeftScope = (Row) this.table.CurrentKey.CopyInstance();
          this.optimizedRightScope = this.optimizedLeftScope.CopyInstance();
          this.optimizedLeftScope.InitTop();
          this.optimizedRightScope.InitBottom();
          this.optimizedLeftScope.RowId = Row.MinRowId + 1U;
          this.optimizedRightScope.RowId = Row.MaxRowId - 1U;
        }
        object obj = ((IValue) this.OptimizedKeyColumn.Execute()).Value;
        if (obj != null)
        {
          this.optimizedLeftScope[0].Value = obj;
          this.optimizedRightScope[0].Value = obj;
          this.table.SetScope((IVistaDBRow) this.optimizedLeftScope, (IVistaDBRow) this.optimizedRightScope);
          if (this.OptimizedCaching && this.keyedLookupCache != null)
          {
            this.table.First();
            emptyResultSet = this.table.EndOfTable;
            this.optimizedDataPosition = 0;
            if (emptyResultSet)
              this.keyedLookupCache.SetValues(new object[0]);
            else
              this.SaveOptimizedKeyedRow();
          }
        }
        else
          emptyResultSet = true;
      }
      return true;
    }

    private void SaveOptimizedKeyedRow()
    {
      if (!this.OptimizedCaching || (Signature) this.OptimizedKeyColumn == (Signature) null || (this.keyedLookupCache == null || this.optimizedDataValues != null) || (this.optimizedDataPosition != 0 || this.table.EndOfTable || this.optimizedDataVersion >= 0L && this.optimizedDataVersion == this.dataVersion))
        return;
      int count = this.table.CurrentRow.Count;
      object[] values = new object[count];
      IEnumerable<int> registeredColumns = this.keyedLookupCache.GetRegisteredColumns();
      if (registeredColumns == null)
      {
        for (int index = 0; index < count; ++index)
        {
          IColumn column = this.table.CurrentRow[index];
          values[index] = column.IsNull || column.ExtendedType || column.SystemType != typeof (string) ? ((IValue) column).Value : (object) column.ToString();
        }
      }
      else
      {
        foreach (int index in registeredColumns)
        {
          IColumn column = this.table.CurrentRow[index];
          values[index] = column.IsNull || column.ExtendedType || column.SystemType != typeof (string) ? ((IValue) column).Value : (object) column.ToString();
        }
      }
      this.keyedLookupCache.SetValues(values);
      this.optimizedDataVersion = this.dataVersion;
    }

    protected override void OnOpen(bool readOnly)
    {
      if (this.tempIndexExpression != null)
        readOnly = false;
      this.table = this.parent.Connection.OpenTable(this.tableName, false, readOnly);
      if (this.table == (ITable) this.parent.Database)
      {
        this.tempIndexExpression = (string) null;
      }
      else
      {
        if (this.tempIndexExpression == null)
          return;
        this.table.CreateTemporaryIndex(this.tempIndexName, this.tempIndexExpression, false);
      }
    }

    protected override bool OnFirst()
    {
      if (this.table == null)
        return false;
      this.optimizedDataPosition = 0;
      if (this.optimizedDataValues == null)
        this.table.First();
      else if (this.optimizedDataValues.Length == 0)
        this.optimizedDataPosition = int.MaxValue;
      return !this.Eof;
    }

    protected override bool OnNext()
    {
      if (this.optimizedDataPosition >= 0 && this.optimizedDataPosition != int.MaxValue)
        ++this.optimizedDataPosition;
      if (this.optimizedDataValues == null)
        this.table.Next();
      return !this.Eof;
    }

    protected override IQuerySchemaInfo InternalPrepare()
    {
      this.tableSchema = this.parent.Connection.CompareString(this.tableName, Database.SystemSchema, true) != 0 ? this.parent.Database.TableSchema(this.tableName) : this.parent.Database.TableSchema((string) null);
      return (IQuerySchemaInfo) this;
    }

    protected override void InternalInsert()
    {
      this.table.Insert();
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
      this.table.Put(columnIndex, (IVistaDBValue) columnValue);
    }

    protected override void InternalDeleteRow()
    {
      if (this.parent.Connection.GetSynchronization())
        this.table.Delete();
      else
        ((IVistaDBTable) this.table).Delete();
    }

    protected override SourceTable CreateSourceTableByName(IVistaDBTableNameCollection tableNames, IViewList views)
    {
      if (this.parent.Connection.CompareString(this.tableName, Database.SystemSchema, true) == 0)
        return (SourceTable) this;
      if (tableNames == null)
        tableNames = this.parent.Database.GetTableNames();
      if (tableNames.Contains(this.tableName))
        return (SourceTable) this;
      if (views == null)
        views = this.parent.Database.EnumViews();
      IView view = (IView) views[(object) this.tableName];
      if (view != null)
        return (SourceTable) this.CreateViewSource(view);
      throw new VistaDBSQLException(572, this.tableName, this.lineNo, this.symbolNo);
    }

    private BaseViewSourceTable CreateViewSource(IView view)
    {
      CreateViewStatement createViewStatement = (CreateViewStatement) this.parent.Connection.CreateBatchStatement(view.Expression, 0L).SubQuery(0);
      int num = (int) createViewStatement.PrepareQuery();
      SelectStatement selectStatement = createViewStatement.SelectStatement;
      if (selectStatement.IsLiveQuery())
        return (BaseViewSourceTable) new LiveViewSourceTable(this.parent, view, createViewStatement.ColumnNames, selectStatement, this.tableAlias, this.collectionOrder, this.lineNo, this.symbolNo);
      return (BaseViewSourceTable) new QueryViewSourceTable(this.parent, view, createViewStatement.ColumnNames, selectStatement, this.tableAlias, this.collectionOrder, this.lineNo, this.symbolNo);
    }

    public override bool Eof
    {
      get
      {
        if (this.optimizedDataValues == null)
          return this.table.EndOfTable;
        return this.optimizedDataPosition != 0;
      }
    }

    public override bool IsNativeTable
    {
      get
      {
        return true;
      }
    }

    public override bool Opened
    {
      get
      {
        return this.table != null;
      }
    }

    public override bool IsUpdatable
    {
      get
      {
        return true;
      }
    }

    public string GetAliasName(int ordinal)
    {
      return this.tableSchema[ordinal].Name;
    }

    public int GetColumnOrdinal(string name)
    {
      IVistaDBColumnAttributes columnAttributes = this.tableSchema[name];
      if (columnAttributes != null)
        return columnAttributes.RowIndex;
      return -1;
    }

    public int GetWidth(int ordinal)
    {
      return this.tableSchema[ordinal].MaxLength;
    }

    public bool GetIsKey(int ordinal)
    {
      IVistaDBKeyColumn[] vistaDbKeyColumnArray = (IVistaDBKeyColumn[]) null;
      foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) this.tableSchema.Indexes.Values)
      {
        if (indexInformation.Primary)
        {
          vistaDbKeyColumnArray = indexInformation.KeyStructure;
          break;
        }
      }
      if (vistaDbKeyColumnArray == null)
        return false;
      int index = 0;
      for (int length = vistaDbKeyColumnArray.Length; index < length; ++index)
      {
        if (vistaDbKeyColumnArray[index].RowIndex == ordinal)
          return true;
      }
      return false;
    }

    public string GetColumnName(int ordinal)
    {
      return this.tableSchema[ordinal].Name;
    }

    public string GetTableName(int ordinal)
    {
      return this.tableName;
    }

    public Type GetColumnType(int ordinal)
    {
      return Utils.GetSystemType(this.tableSchema[ordinal].Type);
    }

    public bool GetIsAllowNull(int ordinal)
    {
      if (!this.alwaysAllowNull)
        return this.tableSchema[ordinal].AllowNull;
      return true;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return this.tableSchema[ordinal].Type;
    }

    public bool GetIsAliased(int ordinal)
    {
      return false;
    }

    public bool GetIsExpression(int ordinal)
    {
      return false;
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      return this.tableSchema.Identities.ContainsKey(this.tableSchema[ordinal].Name);
    }

    public bool GetIsLong(int ordinal)
    {
      return false;
    }

    public bool GetIsReadOnly(int ordinal)
    {
      return this.tableSchema[ordinal].ReadOnly;
    }

    public string GetDataTypeName(int ordinal)
    {
      return this.tableSchema[ordinal].Type.ToString();
    }

    public DataTable GetSchemaTable()
    {
      return (DataTable) null;
    }

    public string GetColumnDescription(int ordinal)
    {
      return this.tableSchema[ordinal].Description;
    }

    public string GetColumnCaption(int ordinal)
    {
      return this.tableSchema[ordinal].Caption;
    }

    public bool GetIsEncrypted(int ordinal)
    {
      return this.tableSchema[ordinal].Encrypted;
    }

    public int GetCodePage(int ordinal)
    {
      return this.tableSchema[ordinal].CodePage;
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
      IVistaDBIdentityInformation identity = this.tableSchema.Identities[this.tableSchema[ordinal].Name];
      if (identity == null)
      {
        step = (string) null;
        seed = (string) null;
        return (string) null;
      }
      step = identity.StepExpression;
      seed = (string) null;
      return (string) null;
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
      IVistaDBDefaultValueInformation defaultValue = this.tableSchema.DefaultValues[this.tableSchema[ordinal].Name];
      if (defaultValue == null)
      {
        useInUpdate = false;
        return (string) null;
      }
      useInUpdate = defaultValue.UseInUpdate;
      return defaultValue.Expression;
    }

    public int ColumnCount
    {
      get
      {
        return this.tableSchema.ColumnCount;
      }
    }

    private class Relationships : IDisposable
    {
      private InsensitiveHashtable linkedTables;
      private List<string> names;

      internal Relationships(InsensitiveHashtable linkedTables)
      {
        this.linkedTables = linkedTables;
        this.names = new List<string>(linkedTables.Count);
        foreach (string key in (IEnumerable) linkedTables.Keys)
          this.names.Add(key);
      }

      internal List<string> Names
      {
        get
        {
          return this.names;
        }
      }

      internal ITable this[string name]
      {
        get
        {
          return (ITable) this.linkedTables[(object) name];
        }
        set
        {
          if (!this.linkedTables.Contains((object) name))
            return;
          this.linkedTables[(object) name] = (object) value;
        }
      }

      public void Dispose()
      {
        this.linkedTables.Clear();
        this.linkedTables = (InsensitiveHashtable) null;
        this.names.Clear();
        this.names = (List<string>) null;
      }
    }
  }
}
