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
        private static long IndexCounter;
        private ITable table;
        private string tempIndexExpression;
        private string tempIndexName;
        private IVistaDBTableSchema tableSchema;
        private Relationships relationships;
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
            table = (ITable)null;
            tempIndexExpression = (string)null;
            tempIndexName = (string)null;
            tableSchema = (IVistaDBTableSchema)null;
        }

        public NativeSourceTable(Statement parent, ITable table)
          : this(parent, (string)null, (string)null, -1, 0, 0)
        {
            this.table = table;
        }

        internal override long GetOptimizedRowCount(string columnName)
        {
            if (columnName == null || !tableSchema[columnName].AllowNull)
                return table.RowCount;
            return -1;
        }

        internal override void DoOpenExternalRelationships(bool insert, bool delete)
        {
            if (relationships == null)
                relationships = new Relationships(parent.Database.GetRelationships(tableAlias, insert, delete));
            foreach (string name in relationships.Names)
                relationships[name] = parent.Connection.OpenTable(name, table.IsExclusive, table.IsReadOnly);
        }

        internal override void DoFreeExternalRelationships()
        {
            if (relationships == null)
                return;
            foreach (string name in relationships.Names)
            {
                parent.Connection.FreeTable(relationships[name]);
                relationships[name] = (ITable)null;
            }
        }

        internal override void DoFreezeSelfRelationships()
        {
            table.FreezeSelfRelationships();
        }

        internal override void DoDefreezeSelfRelationships()
        {
            table.DefreezeSelfRelationships();
        }

        public override IColumn SimpleGetColumn(int colIndex)
        {
            if (OptimizedCaching && optimizedDataValues != null)
                return (IColumn)optimizedDataRow[colIndex];
            return (IColumn)table.Get(colIndex);
        }

        internal override IRow DoGetIndexStructure(string indexName)
        {
            return table.KeyStructure(indexName);
        }

        public override void Post()
        {
            if (parent.ConstraintOperations != null)
            {
                IRow row = table.CurrentKey.CopyInstance();
                InternalPost();
                table.CurrentKey = row;
                stopNext = table.CurrentKey.CompareKey((IVistaDBRow)row) > 0;
            }
            else
                InternalPost();
        }

        private void InternalPost()
        {
            if (parent.Connection.GetSynchronization())
                table.Post();
            else
                ((IVistaDBTable)table).Post();
        }

        public override void Close()
        {
            DoFreeExternalRelationships();
            if (table == null)
                return;
            parent.Connection.CloseTable(table);
            table = (ITable)null;
            tempIndexExpression = (string)null;
            if (relationships != null)
                relationships.Dispose();
            relationships = (Relationships)null;
        }

        public override void FreeTable()
        {
            if (table == null)
                return;
            parent.Connection.FreeTable(table);
            table = (ITable)null;
            tempIndexExpression = (string)null;
        }

        public override IVistaDBTableSchema GetTableSchema()
        {
            if (parent.Connection.CompareString(tableName, Database.SystemSchema, true) == 0)
                return parent.Database.TableSchema((string)null);
            return parent.Database.TableSchema(tableName);
        }

        public override IColumn GetLastIdentity(string columnName)
        {
            if (table != null)
                return (IColumn)parent.Database.GetLastIdentity(tableName, columnName);
            return (IColumn)null;
        }

        public override string CreateIndex(string expression, bool instantly)
        {
            if (parent.Connection.CompareString(tableName, Database.SystemSchema, true) == 0)
                return (string)null;
            if (tempIndexName == null || !parent.Connection.IsIndexExisting(tableName, tempIndexName))
            {
                tempIndexExpression = expression;
                tempIndexName = "TemporaryIndex" + IndexCounter++.ToString();
            }
            if (instantly)
                table.CreateTemporaryIndex(tempIndexName, tempIndexExpression, false);
            return tempIndexName;
        }

        public override int GetColumnCount()
        {
            return ColumnCount;
        }

        internal override void PushTemporaryTableCache(TriggerAction triggerAction)
        {
        }

        internal override void PopTemporaryTableCache(TriggerAction triggerAction)
        {
        }

        internal override void PrepareTriggers(TriggerAction triggerAction)
        {
            table.PrepareTriggers(triggerAction);
        }

        internal override void ExecuteTriggers(TriggerAction eventType, bool justReset)
        {
            table.ExecuteTriggers(eventType, justReset);
        }

        internal override IOptimizedFilter BuildFilterMap(string indexName, IRow lowScopeValue, IRow highScopeValue, bool excludeNulls)
        {
            return table.BuildFilterMap(indexName, lowScopeValue, highScopeValue, excludeNulls);
        }

        internal override bool BeginOptimizedFiltering(IOptimizedFilter filter, string pivotIndex)
        {
            table.BeginOptimizedFiltering(filter, pivotIndex);
            table.PrepareFtsOptimization();
            return false;
        }

        internal override void ResetOptimizedFiltering()
        {
            table.ResetOptimizedFiltering();
        }

        internal override bool SetScope(IRow leftScope, IRow rightScope)
        {
            table.SetScope((IVistaDBRow)leftScope, (IVistaDBRow)rightScope);
            table.PrepareFtsOptimization();
            return false;
        }

        internal override void ResetOptimization()
        {
            if (table == null)
                return;
            table.ResetOptimizedFiltering();
            base.ResetOptimization();
        }

        internal override IVistaDBIndexCollection TemporaryIndexes
        {
            get
            {
                if (table != null)
                    return table.TemporaryIndexes;
                return (IVistaDBIndexCollection)null;
            }
        }

        internal override string ActiveIndex
        {
            set
            {
                table.ActiveIndex = value;
            }
        }

        internal override void RegisterColumnSignature(int columnIndex)
        {
            if (keyedLookupCache != null)
            {
                keyedLookupCache.RegisterColumnSignature(columnIndex);
            }
            else
            {
                if (columnsToRegister == null)
                    columnsToRegister = new Dictionary<int, bool>();
                else if (columnsToRegister.ContainsKey(-1))
                    return;
                if (columnIndex < 0)
                {
                    columnsToRegister.Clear();
                    columnsToRegister.Add(-1, true);
                }
                else
                    columnsToRegister[columnIndex] = true;
            }
        }

        internal override bool ActivateOptimizedConstraints(out bool emptyResultSet)
        {
            emptyResultSet = false;
            if ((Signature)OptimizedIndexColumn == (Signature)null || (Signature)OptimizedKeyColumn == (Signature)null)
                return false;
            if (string.IsNullOrEmpty(OptimizedIndexName))
            {
                ClearJoinOptimizationColumns();
                return false;
            }
            optimizedDataPosition = int.MinValue;
            optimizedDataValues = (object[])null;
            if (OptimizedCaching)
            {
                if (keyedLookupCache == null)
                {
                    SelectStatement parent = Parent as SelectStatement;
                    CacheFactory cacheFactory = parent == null ? (CacheFactory)null : parent.CacheFactory;
                    if (cacheFactory != null)
                    {
                        KeyedLookupTable lookupTable = cacheFactory.GetLookupTable((IVistaDBDatabase)null, (SourceTable)this, OptimizedIndexName, OptimizedKeyColumn);
                        if (lookupTable != null)
                        {
                            keyedLookupCache = lookupTable;
                            if (columnsToRegister != null && columnsToRegister.Count > 0)
                            {
                                foreach (int key in columnsToRegister.Keys)
                                    keyedLookupCache.RegisterColumnSignature(key);
                            }
                            columnsToRegister = (Dictionary<int, bool>)null;
                        }
                    }
                }
                if (keyedLookupCache == null)
                {
                    ClearJoinOptimizationCaching();
                }
                else
                {
                    optimizedDataVersion = -1L;
                    optimizedDataValues = keyedLookupCache.GetValues();
                    if (optimizedDataValues != null)
                    {
                        if (optimizedDataRow == null)
                            optimizedDataRow = (Row)table.CurrentRow.CopyInstance();
                        int count = optimizedDataRow.Count;
                        if (optimizedDataValues.Length > 0)
                        {
                            optimizedDataPosition = 0;
                            for (int index = 0; index < count; ++index)
                                optimizedDataRow[index].Value = optimizedDataValues[index];
                        }
                        else
                        {
                            emptyResultSet = true;
                            optimizedDataPosition = int.MaxValue;
                            for (int index = 0; index < count; ++index)
                                optimizedDataRow[index].Value = (object)null;
                        }
                        optimizedDataVersion = dataVersion;
                    }
                }
            }
            table.ActiveIndex = OptimizedIndexName;
            if (optimizedDataValues == null)
            {
                if (optimizedLeftScope == null || optimizedRightScope == null)
                {
                    optimizedLeftScope = (Row)table.CurrentKey.CopyInstance();
                    optimizedRightScope = optimizedLeftScope.CopyInstance();
                    optimizedLeftScope.InitTop();
                    optimizedRightScope.InitBottom();
                    optimizedLeftScope.RowId = Row.MinRowId + 1U;
                    optimizedRightScope.RowId = Row.MaxRowId - 1U;
                }
                object obj = ((IValue)OptimizedKeyColumn.Execute()).Value;
                if (obj != null)
                {
                    optimizedLeftScope[0].Value = obj;
                    optimizedRightScope[0].Value = obj;
                    table.SetScope((IVistaDBRow)optimizedLeftScope, (IVistaDBRow)optimizedRightScope);
                    if (OptimizedCaching && keyedLookupCache != null)
                    {
                        table.First();
                        emptyResultSet = table.EndOfTable;
                        optimizedDataPosition = 0;
                        if (emptyResultSet)
                            keyedLookupCache.SetValues(new object[0]);
                        else
                            SaveOptimizedKeyedRow();
                    }
                }
                else
                    emptyResultSet = true;
            }
            return true;
        }

        private void SaveOptimizedKeyedRow()
        {
            if (!OptimizedCaching || (Signature)OptimizedKeyColumn == (Signature)null || (keyedLookupCache == null || optimizedDataValues != null) || (optimizedDataPosition != 0 || table.EndOfTable || optimizedDataVersion >= 0L && optimizedDataVersion == dataVersion))
                return;
            int count = table.CurrentRow.Count;
            object[] values = new object[count];
            IEnumerable<int> registeredColumns = keyedLookupCache.GetRegisteredColumns();
            if (registeredColumns == null)
            {
                for (int index = 0; index < count; ++index)
                {
                    IColumn column = table.CurrentRow[index];
                    values[index] = column.IsNull || column.ExtendedType || column.SystemType != typeof(string) ? ((IValue)column).Value : (object)column.ToString();
                }
            }
            else
            {
                foreach (int index in registeredColumns)
                {
                    IColumn column = table.CurrentRow[index];
                    values[index] = column.IsNull || column.ExtendedType || column.SystemType != typeof(string) ? ((IValue)column).Value : (object)column.ToString();
                }
            }
            keyedLookupCache.SetValues(values);
            optimizedDataVersion = dataVersion;
        }

        protected override void OnOpen(bool readOnly)
        {
            if (tempIndexExpression != null)
                readOnly = false;
            table = parent.Connection.OpenTable(tableName, false, readOnly);
            if (table == (ITable)parent.Database)
            {
                tempIndexExpression = (string)null;
            }
            else
            {
                if (tempIndexExpression == null)
                    return;
                table.CreateTemporaryIndex(tempIndexName, tempIndexExpression, false);
            }
        }

        protected override bool OnFirst()
        {
            if (table == null)
                return false;
            optimizedDataPosition = 0;
            if (optimizedDataValues == null)
                table.First();
            else if (optimizedDataValues.Length == 0)
                optimizedDataPosition = int.MaxValue;
            return !Eof;
        }

        protected override bool OnNext()
        {
            if (optimizedDataPosition >= 0 && optimizedDataPosition != int.MaxValue)
                ++optimizedDataPosition;
            if (optimizedDataValues == null)
                table.Next();
            return !Eof;
        }

        protected override IQuerySchemaInfo InternalPrepare()
        {
            tableSchema = parent.Connection.CompareString(tableName, Database.SystemSchema, true) != 0 ? parent.Database.TableSchema(tableName) : parent.Database.TableSchema((string)null);
            return (IQuerySchemaInfo)this;
        }

        protected override void InternalInsert()
        {
            table.Insert();
        }

        protected override void InternalPutValue(int columnIndex, IColumn columnValue)
        {
            table.Put(columnIndex, (IVistaDBValue)columnValue);
        }

        protected override void InternalDeleteRow()
        {
            if (parent.Connection.GetSynchronization())
                table.Delete();
            else
                ((IVistaDBTable)table).Delete();
        }

        protected override SourceTable CreateSourceTableByName(IVistaDBTableNameCollection tableNames, IViewList views)
        {
            if (parent.Connection.CompareString(tableName, Database.SystemSchema, true) == 0)
                return (SourceTable)this;
            if (tableNames == null)
                tableNames = parent.Database.GetTableNames();
            if (tableNames.Contains(tableName))
                return (SourceTable)this;
            if (views == null)
                views = parent.Database.EnumViews();
            IView view = (IView)views[(object)tableName];
            if (view != null)
                return (SourceTable)CreateViewSource(view);
            throw new VistaDBSQLException(572, tableName, lineNo, symbolNo);
        }

        private BaseViewSourceTable CreateViewSource(IView view)
        {
            CreateViewStatement createViewStatement = (CreateViewStatement)parent.Connection.CreateBatchStatement(view.Expression, 0L).SubQuery(0);
            int num = (int)createViewStatement.PrepareQuery();
            SelectStatement selectStatement = createViewStatement.SelectStatement;
            if (selectStatement.IsLiveQuery())
                return (BaseViewSourceTable)new LiveViewSourceTable(parent, view, createViewStatement.ColumnNames, selectStatement, tableAlias, collectionOrder, lineNo, symbolNo);
            return (BaseViewSourceTable)new QueryViewSourceTable(parent, view, createViewStatement.ColumnNames, selectStatement, tableAlias, collectionOrder, lineNo, symbolNo);
        }

        public override bool Eof
        {
            get
            {
                if (optimizedDataValues == null)
                    return table.EndOfTable;
                return optimizedDataPosition != 0;
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
                return table != null;
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
            return tableSchema[ordinal].Name;
        }

        public int GetColumnOrdinal(string name)
        {
            IVistaDBColumnAttributes columnAttributes = tableSchema[name];
            if (columnAttributes != null)
                return columnAttributes.RowIndex;
            return -1;
        }

        public int GetWidth(int ordinal)
        {
            return tableSchema[ordinal].MaxLength;
        }

        public bool GetIsKey(int ordinal)
        {
            IVistaDBKeyColumn[] vistaDbKeyColumnArray = (IVistaDBKeyColumn[])null;
            foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)tableSchema.Indexes.Values)
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
            return tableSchema[ordinal].Name;
        }

        public string GetTableName(int ordinal)
        {
            return tableName;
        }

        public Type GetColumnType(int ordinal)
        {
            return Utils.GetSystemType(tableSchema[ordinal].Type);
        }

        public bool GetIsAllowNull(int ordinal)
        {
            if (!alwaysAllowNull)
                return tableSchema[ordinal].AllowNull;
            return true;
        }

        public VistaDBType GetColumnVistaDBType(int ordinal)
        {
            return tableSchema[ordinal].Type;
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
            return tableSchema.Identities.ContainsKey(tableSchema[ordinal].Name);
        }

        public bool GetIsLong(int ordinal)
        {
            return false;
        }

        public bool GetIsReadOnly(int ordinal)
        {
            return tableSchema[ordinal].ReadOnly;
        }

        public string GetDataTypeName(int ordinal)
        {
            return tableSchema[ordinal].Type.ToString();
        }

        public DataTable GetSchemaTable()
        {
            return (DataTable)null;
        }

        public string GetColumnDescription(int ordinal)
        {
            return tableSchema[ordinal].Description;
        }

        public string GetColumnCaption(int ordinal)
        {
            return tableSchema[ordinal].Description;
        }

        public bool GetIsEncrypted(int ordinal)
        {
            return tableSchema[ordinal].Encrypted;
        }

        public int GetCodePage(int ordinal)
        {
            return tableSchema[ordinal].CodePage;
        }

        public string GetIdentity(int ordinal, out string step, out string seed)
        {
            IVistaDBIdentityInformation identity = tableSchema.Identities[tableSchema[ordinal].Name];
            if (identity == null)
            {
                step = (string)null;
                seed = (string)null;
                return (string)null;
            }
            step = identity.StepExpression;
            seed = (string)null;
            return (string)null;
        }

        public string GetDefaultValue(int ordinal, out bool useInUpdate)
        {
            IVistaDBDefaultValueInformation defaultValue = tableSchema.DefaultValues[tableSchema[ordinal].Name];
            if (defaultValue == null)
            {
                useInUpdate = false;
                return (string)null;
            }
            useInUpdate = defaultValue.UseInUpdate;
            return defaultValue.Expression;
        }

        public int ColumnCount
        {
            get
            {
                return tableSchema.ColumnCount;
            }
        }

        private class Relationships : IDisposable
        {
            private InsensitiveHashtable linkedTables;
            private List<string> names;

            internal Relationships(InsensitiveHashtable linkedTables)
            {
                this.linkedTables = linkedTables;
                names = new List<string>(linkedTables.Count);
                foreach (string key in (IEnumerable)linkedTables.Keys)
                    names.Add(key);
            }

            internal List<string> Names
            {
                get
                {
                    return names;
                }
            }

            internal ITable this[string name]
            {
                get
                {
                    return (ITable)linkedTables[(object)name];
                }
                set
                {
                    if (!linkedTables.Contains((object)name))
                        return;
                    linkedTables[(object)name] = (object)value;
                }
            }

            public void Dispose()
            {
                linkedTables.Clear();
                linkedTables = (InsensitiveHashtable)null;
                names.Clear();
                names = (List<string>)null;
            }
        }
    }
}
