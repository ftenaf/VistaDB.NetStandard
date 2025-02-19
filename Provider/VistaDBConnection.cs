﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Provider
{
    public sealed class VistaDBConnection : DbConnection, ICloneable
    {
        internal static readonly CrossConversion Conversion = new CrossConversion(CultureInfo.CurrentCulture);
        private static readonly ConnectionPoolCollection PoolsCollection = new ConnectionPoolCollection();
        public static readonly string SystemSchema = Engine.Core.Database.SystemSchema;
        public static readonly string SystemTableType = "SYSTEM_TABLE";
        public static readonly string UserTableType = "BASE TABLE";
        private static readonly object InfoMessageEvent = new object();
        private bool canPack;
        private VistaDBConnectionStringBuilder connectionString;
        private ILocalSQLConnection localSqlConnection;

        public static void PackDatabase(string fileName, string encryptionKeyString, bool backup, OperationCallbackDelegate operationCallbackDelegate)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.PackDatabase(fileName, encryptionKeyString, backup, operationCallbackDelegate);
        }

        public static void PackDatabase(string fileName, string encryptionKeyString, string newencryptionKeyString, int newPageSize, int newLCID, bool newCaseSensitive, bool backup, OperationCallbackDelegate operationCallbackDelegate)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.PackDatabase(fileName, encryptionKeyString, newencryptionKeyString, newPageSize, newLCID, newCaseSensitive, backup, operationCallbackDelegate);
        }

        public static void PackDatabase(string fileName, OperationCallbackDelegate operationCallback)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.PackDatabase(fileName, operationCallback);
        }

        public static void PackDatabase(string fileName)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.PackDatabase(fileName);
        }

        public static void RepairDatabase(string fileName, string encryptionKeyString, OperationCallbackDelegate operationCallbackDelegate)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.RepairDatabase(fileName, encryptionKeyString, operationCallbackDelegate);
        }

        public static void RepairDatabase(string fileName, OperationCallbackDelegate operationCallback)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.RepairDatabase(fileName, operationCallback);
        }

        public static void RepairDatabase(string fileName)
        {
            using (IVistaDBDDA vistaDbdda = VistaDBEngine.Connections.OpenDDA())
                vistaDbdda.RepairDatabase(fileName);
        }

        public static void ClearAllPools()
        {
            PoolsCollection.Clear();
        }

        public static void ClearPool(VistaDBConnection connection)
        {
            PoolsCollection.ClearPool(connection);
        }

        public VistaDBConnection()
        {
            connectionString = new VistaDBConnectionStringBuilder();
            canPack = true;
        }

        public VistaDBConnection(string connectionString)
          : this()
        {
            ConnectionString = connectionString;
        }

        public VistaDBConnection(IVistaDBDatabase database)
          : this()
        {
            canPack = false;
            InstantiateLocalSqlConnection((IDatabase)database);
        }

        public override string ConnectionString
        {
            get
            {
                return connectionString.ConnectionString;
            }
            set
            {
                connectionString.ConnectionString = value;
            }
        }

        public bool ContextConnection
        {
            get
            {
                return connectionString.ContextConnection;
            }
        }

        public override string Database
        {
            get
            {
                return connectionString.Database;
            }
        }

        public override string DataSource
        {
            get
            {
                return connectionString.DataSource;
            }
        }

        public bool IsolatedStorage
        {
            get
            {
                return connectionString.IsolatedStorage;
            }
        }

        public VistaDBTransaction.TransactionMode TransactionMode
        {
            get
            {
                MustBeOpened();
                return connectionString.TransactionMode;
            }
        }

        public VistaDBDatabaseOpenMode OpenMode
        {
            get
            {
                return connectionString.OpenMode;
            }
        }

        public string Password
        {
            get
            {
                return connectionString.Password;
            }
        }

        public override string ServerVersion
        {
            get
            {
                return "VistaDB 4.1";
            }
        }

        public int LockTimeout
        {
            get
            {
                if (localSqlConnection != null)
                    return localSqlConnection.LockTimeout;
                return -1;
            }
            set
            {
                if (localSqlConnection == null)
                    return;
                localSqlConnection.LockTimeout = value;
            }
        }

        public bool PersistentLockFiles
        {
            get
            {
                if (localSqlConnection != null)
                    return localSqlConnection.PersistentLockFiles;
                return false;
            }
            set
            {
                if (localSqlConnection == null)
                    return;
                localSqlConnection.PersistentLockFiles = value;
            }
        }

        public override ConnectionState State
        {
            get
            {
                lock (SyncRoot)
                    return localSqlConnection == null || !localSqlConnection.DatabaseOpened ? ConnectionState.Closed : ConnectionState.Open;
            }
        }

        public new VistaDBTransaction BeginTransaction()
        {
            MustBeOpened();
            return new VistaDBTransaction(this);
        }

        public new VistaDBTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (isolationLevel != IsolationLevel.ReadCommitted && isolationLevel != IsolationLevel.Unspecified)
                throw new VistaDBSQLException(1013, "Use IsolationLevel.ReadCommitted instead of " + isolationLevel.ToString(), 0, 0);
            return BeginTransaction();
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException("The method is not supported.");
        }

        public override void Close()
        {
            ConnectionState state;
            lock (SyncRoot)
            {
                state = State;
                if (state == ConnectionState.Closed)
                    return;
                if (ContextConnection)
                {
                    localSqlConnection = null;
                    return;
                }
                if (!localSqlConnection.IsDatabaseOwner)
                {
                    localSqlConnection.CloseAllPooledTables();
                    return;
                }
                bool close = true;
                if (connectionString.Pooling)
                    close = PoolsCollection.PutConnectionOnHold(localSqlConnection, GetPoolConnectionString(), connectionString.MinPoolSize, connectionString.MaxPoolSize);
                ClearLocalSqlConnection(close);
            }
            if (state != ConnectionState.Open)
                return;
            OnStateChange(new StateChangeEventArgs(state, ConnectionState.Closed));
        }

        new public VistaDBCommand CreateCommand()
        {
            return new VistaDBCommand(string.Empty, this);
        }

        public IVistaDBTableSchema GetTableSchema(string tableName)
        {
            if (State != ConnectionState.Open)
                throw new InvalidOperationException();
            return localSqlConnection.TableSchema(tableName);
        }

        public override DataTable GetSchema()
        {
            return GetSchema("METADATACOLLECTIONS", null);
        }

        public override DataTable GetSchema(string collectionName)
        {
            return GetSchema(collectionName, null);
        }

        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            if (string.IsNullOrEmpty(collectionName))
                collectionName = "METADATACOLLECTIONS";
            lock (SyncRoot)
            {
                if (State != ConnectionState.Open)
                    throw new InvalidOperationException();
                string[] strArray = new string[5];
                restrictionValues?.CopyTo(strArray, 0);
                switch (UpperString(collectionName))
                {
                    case "METADATACOLLECTIONS":
                        return GetSchemaMetaDataCollections();
                    case "DATASOURCEINFORMATION":
                        return GetSchemaDataSourceInformation();
                    case "DATATYPES":
                        return GetSchemaDataTypes();
                    case "TABLES":
                        return GetSchemaTables(strArray[2], strArray[3]);
                    case "COLUMNS":
                        return GetSchemaColumns(strArray[2], strArray[3]);
                    case "INDEXES":
                        return GetSchemaIndexes(strArray[2], strArray[3]);
                    case "INDEXCOLUMNS":
                        return GetSchemaIndexColumns(strArray[2], strArray[3], strArray[4]);
                    case "FOREIGNKEYS":
                        return GetSchemaForeignKeys(strArray[2], strArray[3]);
                    case "FOREIGNKEYCOLUMNS":
                        return GetSchemaForeignKeyColumns(strArray[2], strArray[3], strArray[4]);
                    case "RESERVEDWORDS":
                        return GetSchemaReservedWords();
                    case "VIEWS":
                        return GetSchemaViews(strArray[2]);
                    case "VIEWCOLUMNS":
                        return GetSchemaViewColumns(strArray[2], strArray[3]);
                    case "PROCEDURES":
                        return GetSchemaStoredProcedures(strArray[3], strArray[2]);
                    case "PROCEDUREPARAMETERS":
                        return GetSchemaStoredProcedureParameters(strArray[2]);
                    case "RESTRICTIONS":
                        return GetSchemaRestrictions();
                }
            }
            throw new NotSupportedException();
        }

        public bool IsSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
        {
            lock (SyncRoot)
            {
                InstantiateLocalSqlConnection(null);
                return localSqlConnection.IsSyntaxCorrect(text, out lineNo, out symbolNo, out errorMessage);
            }
        }

        public bool IsViewSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
        {
            lock (SyncRoot)
            {
                InstantiateLocalSqlConnection(null);
                return localSqlConnection.IsViewSyntaxCorrect(text, out lineNo, out symbolNo, out errorMessage);
            }
        }

        public bool IsConstraintSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
        {
            lock (SyncRoot)
            {
                InstantiateLocalSqlConnection(null);
                return localSqlConnection.IsConstraintSyntaxCorrect(text, out lineNo, out symbolNo, out errorMessage);
            }
        }

        public override void Open()
        {
            lock (SyncRoot)
            {
                ConnectionState state = State;
                if (state == ConnectionState.Open)
                    return;
                if (ContextConnection)
                {
                    localSqlConnection = VistaDBContext.SQLChannel.CurrentConnection;
                    if (localSqlConnection == null)
                        throw new VistaDBSQLException(1009, string.Empty, 0, 0);
                }
                else
                {
                    if (localSqlConnection != null)
                    {
                        localSqlConnection.Dispose();
                        localSqlConnection = null;
                    }
                    if (connectionString.Pooling)
                        localSqlConnection = PoolsCollection.GetConnection(GetPoolConnectionString());
                    if (localSqlConnection != null)
                    {
                        if (!(localSqlConnection is IPooledSQLConnection))
                            return;
                        ((IPooledSQLConnection)localSqlConnection).InitializeConnectionFromPool(this);
                    }
                    else
                    {
                        InstantiateLocalSqlConnection(null);
                        string cryptoKeyString = Password;
                        if (cryptoKeyString == string.Empty)
                            cryptoKeyString = null;
                        localSqlConnection.OpenDatabase(DataSource, OpenMode, cryptoKeyString, IsolatedStorage);
                        if (state != ConnectionState.Closed)
                            return;
                        OnStateChange(new StateChangeEventArgs(state, ConnectionState.Open));
                    }
                }
            }
        }

        private void PackOperationCallback(IVistaDBOperationCallbackStatus status)
        {
            if (InfoMessageHandler == null)
                return;
            InfoMessageHandler(this, new VistaDBInfoMessageEventArgs(status.Message, status.ObjectName));
        }

        public void PackDatabase()
        {
            PackDatabase(false);
        }

        public void PackDatabase(bool backup)
        {
            if (!canPack)
                throw new VistaDBException(347);
            bool flag = State == ConnectionState.Open;
            Close();
            string encryptionKeyString = Password;
            if (string.IsNullOrEmpty(encryptionKeyString))
                encryptionKeyString = null;
            PackDatabase(DataSource, encryptionKeyString, backup, new OperationCallbackDelegate(PackOperationCallback));
            if (!flag)
                return;
            Open();
        }

        private void RepairOperationCallback(IVistaDBOperationCallbackStatus status)
        {
            if (InfoMessageHandler == null)
                return;
            InfoMessageHandler(this, new VistaDBInfoMessageEventArgs(status.Message, status.ObjectName));
        }

        public void RepairDatabase()
        {
            if (!canPack)
                throw new VistaDBException(347);
            bool flag = State == ConnectionState.Open;
            Close();
            string encryptionKeyString = Password;
            if (string.IsNullOrEmpty(encryptionKeyString))
                encryptionKeyString = null;
            RepairDatabase(DataSource, encryptionKeyString, new OperationCallbackDelegate(RepairOperationCallback));
            if (!flag)
                return;
            Open();
        }

        internal void OnPrintMessage(string message)
        {
            VistaDBInfoMessageEventHandler infoMessageHandler = InfoMessageHandler;
            if (infoMessageHandler == null)
                return;
            infoMessageHandler(this, new VistaDBInfoMessageEventArgs(message, string.Empty));
        }

        public event VistaDBInfoMessageEventHandler InfoMessage
        {
            add
            {
                Events.AddHandler(InfoMessageEvent, value);
            }
            remove
            {
                Events.RemoveHandler(InfoMessageEvent, value);
            }
        }

        private VistaDBInfoMessageEventHandler InfoMessageHandler
        {
            get
            {
                return (VistaDBInfoMessageEventHandler)Events[InfoMessageEvent];
            }
        }

        protected override DbProviderFactory DbProviderFactory
        {
            get
            {
                return VistaDBProviderFactory.Instance;
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return BeginTransaction(isolationLevel);
        }

        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Close();
            base.Dispose(disposing);
        }

        private void InstantiateLocalSqlConnection(IDatabase database)
        {
            if (localSqlConnection != null)
                return;
            localSqlConnection = VistaDBEngine.Connections.OpenSQLConnection(this, database);
        }

        private void ClearLocalSqlConnection(bool close)
        {
            if (localSqlConnection == null)
                return;
            localSqlConnection.CloseAllPooledTables();
            if (close)
                localSqlConnection.Dispose();
            localSqlConnection = null;
        }

        private string GetPoolConnectionString()
        {
            if (DataSource == null || DataSource.Length == 0)
                return null;
            return DataSource + ";" + OpenMode.ToString() + ";" + Password;
        }

        private void MustBeOpened()
        {
            if (State != ConnectionState.Open)
                throw new VistaDBSQLException(1012, string.Empty, 0, 0);
        }

        private int CompareString(string s1, string s2, bool ignoreCase)
        {
            return string.Compare(s1, s2, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
        }

        private string UpperString(string s)
        {
            return s.ToUpperInvariant();
        }

        private DataTable GetSchemaMetaDataCollections()
        {
            DataTable dataTable = new DataTable("MetaDataCollections");
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("CollectionName", typeof(string));
            dataTable.Columns.Add("NumberOfRestrictions", typeof(int));
            dataTable.Columns.Add("NumberOfIdentifierParts", typeof(int));
            dataTable.BeginLoadData();
            using (StringReader stringReader = new StringReader(SQLResource.MetaDataCollections))
            {
                int num = (int)dataTable.ReadXml(stringReader);
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaRestrictions()
        {
            DataTable dataTable = new DataTable("Restrictions");
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("CollectionName", typeof(string));
            dataTable.Columns.Add("RestrictionName", typeof(string));
            dataTable.Columns.Add("RestrictionDefault", typeof(string));
            dataTable.Columns.Add("RestrictionNumber", typeof(int));
            dataTable.BeginLoadData();
            using (StringReader stringReader = new StringReader(SQLResource.Restrictions))
            {
                int num = (int)dataTable.ReadXml(stringReader);
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaDataSourceInformation()
        {
            DataTable dataTable = new DataTable("DataSourceInformation");
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add(DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.DataSourceProductName, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.DataSourceProductVersion, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.DataSourceProductVersionNormalized, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.GroupByBehavior, typeof(GroupByBehavior));
            dataTable.Columns.Add(DbMetaDataColumnNames.IdentifierPattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.IdentifierCase, typeof(IdentifierCase));
            dataTable.Columns.Add(DbMetaDataColumnNames.OrderByColumnsInSelect, typeof(bool));
            dataTable.Columns.Add(DbMetaDataColumnNames.ParameterMarkerFormat, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.ParameterMarkerPattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.ParameterNameMaxLength, typeof(int));
            dataTable.Columns.Add(DbMetaDataColumnNames.ParameterNamePattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.QuotedIdentifierPattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.QuotedIdentifierCase, typeof(IdentifierCase));
            dataTable.Columns.Add(DbMetaDataColumnNames.StatementSeparatorPattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.StringLiteralPattern, typeof(string));
            dataTable.Columns.Add(DbMetaDataColumnNames.SupportedJoinOperators, typeof(SupportedJoinOperators));
            dataTable.BeginLoadData();
            DataRow row = dataTable.NewRow();
            row[DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern] = "\\.";
            row[DbMetaDataColumnNames.DataSourceProductName] = "VistaDB";
            row[DbMetaDataColumnNames.DataSourceProductVersion] = "4.3.3.34";
            row[DbMetaDataColumnNames.DataSourceProductVersionNormalized] = "4.1";
            row[DbMetaDataColumnNames.IdentifierPattern] = "(^\\[\\p{Lo}\\p{Lu}\\p{Ll}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Nd}@$#_]*$)|(^\\[[^\\]\\0]|\\]\\]+\\]$)|(^\\\"[^\\\"\\0]|\\\"\\\"+\\\"$)";
            row[DbMetaDataColumnNames.OrderByColumnsInSelect] = false;
            row[DbMetaDataColumnNames.ParameterMarkerFormat] = "{0}";
            row[DbMetaDataColumnNames.ParameterMarkerPattern] = "(@[A-Za-z0-9_]+)";
            row[DbMetaDataColumnNames.ParameterNameMaxLength] = 128;
            row[DbMetaDataColumnNames.ParameterNamePattern] = "^[\\w_@#][\\w_@#]*(?=\\s+|$)";
            row[DbMetaDataColumnNames.QuotedIdentifierPattern] = "(([^\\[]|\\]\\])*)";
            row[DbMetaDataColumnNames.StatementSeparatorPattern] = ";";
            row[DbMetaDataColumnNames.StringLiteralPattern] = "('([^']|'')*')";
            row[DbMetaDataColumnNames.GroupByBehavior] = GroupByBehavior.Unrelated;
            row[DbMetaDataColumnNames.IdentifierCase] = IdentifierCase.Insensitive;
            row[DbMetaDataColumnNames.QuotedIdentifierCase] = IdentifierCase.Insensitive;
            row[DbMetaDataColumnNames.SupportedJoinOperators] = SupportedJoinOperators.Inner | SupportedJoinOperators.LeftOuter | SupportedJoinOperators.RightOuter;
            dataTable.Rows.Add(row);
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaDataTypes()
        {
            DataTable dataTable = new DataTable("DataTypes");
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("TypeName", typeof(string));
            dataTable.Columns.Add("ProviderDbType", typeof(int));
            dataTable.Columns.Add("ColumnSize", typeof(long));
            dataTable.Columns.Add("CreateFormat", typeof(string));
            dataTable.Columns.Add("CreateParameters", typeof(string));
            dataTable.Columns.Add("DataType", typeof(string));
            dataTable.Columns.Add("IsAutoIncrementable", typeof(bool));
            dataTable.Columns.Add("IsBestMatch", typeof(bool));
            dataTable.Columns.Add("IsCaseSensitive", typeof(bool));
            dataTable.Columns.Add("IsFixedLength", typeof(bool));
            dataTable.Columns.Add("IsFixedPrecisionScale", typeof(bool));
            dataTable.Columns.Add("IsLong", typeof(bool));
            dataTable.Columns.Add("IsNullable", typeof(bool));
            dataTable.Columns.Add("IsSearchable", typeof(bool));
            dataTable.Columns.Add("IsSearchableWithLike", typeof(bool));
            dataTable.Columns.Add("IsUnsigned", typeof(bool));
            dataTable.Columns.Add("MaximumScale", typeof(short));
            dataTable.Columns.Add("MinimumScale", typeof(short));
            dataTable.Columns.Add("IsConcurrencyType", typeof(bool));
            dataTable.Columns.Add("IsLiteralsSupported", typeof(bool));
            dataTable.Columns.Add("LiteralPrefix", typeof(string));
            dataTable.Columns.Add("LiteralSuffix", typeof(string));
            dataTable.BeginLoadData();
            using (StringReader stringReader = new StringReader(SQLResource.DataTypes))
            {
                int num = (int)dataTable.ReadXml(stringReader);
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private Database.TableIdMap GetTables(string tableName, string tableType)
        {
            Database.TableIdMap tableIdMap;
            if (tableName == null)
            {
                tableIdMap = tableType == null || CompareString(tableType, UserTableType, true) == 0 ? (Database.TableIdMap)localSqlConnection.GetTables() : null;
                if (tableType == null || CompareString(tableType, SystemTableType, true) == 0)
                {
                    if (tableIdMap == null)
                        tableIdMap = new Database.TableIdMap();
                    tableIdMap.AddTable(null);
                }
            }
            else
            {
                tableIdMap = new Database.TableIdMap();
                if (CompareString(tableName, SystemSchema, true) == 0)
                {
                    if (tableType == null || CompareString(tableType, UserTableType, true) == 0)
                        tableIdMap.AddTable(null);
                }
                else if (tableType == null || CompareString(tableType, SystemTableType, true) == 0)
                {
                    foreach (KeyValuePair<ulong, string> table in (Database.TableIdMap)localSqlConnection.GetTables())
                    {
                        if (CompareString(table.Value, tableName, true) == 0)
                        {
                            tableIdMap.Add(table.Value, table.Key);
                            break;
                        }
                    }
                }
            }
            return tableIdMap;
        }

        private DataTable GetSchemaTables(string tableName, string tableType)
        {
            DataTable dataTable = new DataTable("Tables");
            tableName = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableName);
            tableType = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableType);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("TABLE_TYPE", typeof(string));
            dataTable.Columns.Add("TABLE_DESCRIPTION", typeof(string));
            dataTable.DefaultView.Sort = "TABLE_NAME";
            dataTable.BeginLoadData();
            Database.TableIdMap tables = GetTables(tableName, tableType);
            if (tables != null)
            {
                foreach (string key in (IEnumerable<string>)tables.Keys)
                {
                    IVistaDBTableSchema vistaDbTableSchema = localSqlConnection.TableSchema(key);
                    DataRow row = dataTable.NewRow();
                    row["TABLE_NAME"] = key == null ? SystemSchema : (object)vistaDbTableSchema.Name;
                    row["TABLE_TYPE"] = key == null ? SystemTableType : (object)UserTableType;
                    row["TABLE_DESCRIPTION"] = vistaDbTableSchema.Description;
                    dataTable.Rows.Add(row);
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaColumns(string tableName, string columnName)
        {
            DataTable dataTable = new DataTable("Columns");
            tableName = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableName);
            columnName = VistaDBCommandBuilder.InternalUnquoteIdentifier(columnName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("COLUMN_NAME", typeof(string));
            dataTable.Columns.Add("ORDINAL_POSITION", typeof(int));
            dataTable.Columns.Add("COLUMN_DEFAULT", typeof(string));
            dataTable.Columns.Add("IS_NULLABLE", typeof(bool));
            dataTable.Columns.Add("DATA_TYPE", typeof(string));
            dataTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
            dataTable.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(int));
            dataTable.Columns.Add("NUMERIC_PRECISION", typeof(int));
            dataTable.Columns.Add("NUMERIC_PRECISION_RADIX", typeof(short));
            dataTable.Columns.Add("NUMERIC_SCALE", typeof(int));
            dataTable.Columns.Add("DATETIME_PRECISION", typeof(long));
            dataTable.Columns.Add("CHARACTER_SET_CATALOG", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_SCHEMA", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_NAME", typeof(string));
            dataTable.Columns.Add("COLLATION_CATALOG", typeof(string));
            dataTable.Columns.Add("COLLATION_SCHEMA", typeof(string));
            dataTable.Columns.Add("COLLATION_NAME", typeof(string));
            dataTable.Columns.Add("DOMAIN_CATALOG", typeof(string));
            dataTable.Columns.Add("DOMAIN_NAME", typeof(string));
            dataTable.Columns.Add("DESCRIPTION", typeof(string));
            dataTable.Columns.Add("PRIMARY_KEY", typeof(bool));
            dataTable.Columns.Add("COLUMN_CAPTION", typeof(string));
            dataTable.Columns.Add("COLUMN_ENCRYPTED", typeof(bool));
            dataTable.Columns.Add("COLUMN_PACKED", typeof(bool));
            dataTable.Columns.Add("TYPE_GUID", typeof(Guid));
            dataTable.Columns.Add("COLUMN_HASDEFAULT", typeof(bool));
            dataTable.Columns.Add("COLUMN_GUID", typeof(Guid));
            dataTable.Columns.Add("COLUMN_PROPID", typeof(long));
            dataTable.DefaultView.Sort = "TABLE_NAME, ORDINAL_POSITION";
            dataTable.BeginLoadData();
            foreach (string key in (IEnumerable<string>)GetTables(tableName, null).Keys)
            {
                IVistaDBTableSchema vistaDbTableSchema = localSqlConnection.TableSchema(key);
                int index1 = 0;
                for (int columnCount = vistaDbTableSchema.ColumnCount; index1 < columnCount; ++index1)
                {
                    IVistaDBColumnAttributes columnAttributes = vistaDbTableSchema[index1];
                    if (columnName == null || CompareString(columnName, columnAttributes.Name, true) == 0)
                    {
                        IVistaDBDefaultValueCollection defaultValues = vistaDbTableSchema.DefaultValues;
                        DataRow row = dataTable.NewRow();
                        bool flag1 = defaultValues.ContainsKey(columnAttributes.Name);
                        row["TABLE_NAME"] = key == null ? SystemSchema : (object)vistaDbTableSchema.Name;
                        row["COLUMN_NAME"] = columnAttributes.Name;
                        row["ORDINAL_POSITION"] = index1;
                        row["COLUMN_HASDEFAULT"] = flag1;
                        row["COLUMN_DEFAULT"] = flag1 ? defaultValues[columnAttributes.Name].Expression : (object)DBNull.Value;
                        row["IS_NULLABLE"] = columnAttributes.AllowNull;
                        row["DATA_TYPE"] = columnAttributes.Type.ToString();
                        row["CHARACTER_MAXIMUM_LENGTH"] = columnAttributes.MaxLength;
                        row["DESCRIPTION"] = columnAttributes.Description;
                        row["COLUMN_CAPTION"] = columnAttributes.Caption;
                        row["COLUMN_ENCRYPTED"] = columnAttributes.Encrypted;
                        row["COLUMN_PACKED"] = columnAttributes.Packed;
                        row["CHARACTER_SET_NAME"] = columnAttributes.CodePage == 0 ? null : (object)columnAttributes.CodePage.ToString();
                        bool flag2 = false;
                        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)vistaDbTableSchema.Indexes.Values)
                        {
                            if (indexInformation.Primary)
                            {
                                IVistaDBKeyColumn[] keyStructure = indexInformation.KeyStructure;
                                int index2 = 0;
                                for (int length = indexInformation.KeyStructure.Length; index2 < length; ++index2)
                                {
                                    if (keyStructure[index2].RowIndex == index1)
                                    {
                                        flag2 = true;
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                        row["PRIMARY_KEY"] = flag2;
                        dataTable.Rows.Add(row);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaIndexes(string tableName, string indexName)
        {
            DataTable dataTable = new DataTable("Indexes");
            tableName = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableName);
            indexName = VistaDBCommandBuilder.InternalUnquoteIdentifier(indexName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_NAME", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("TYPE_DESC", typeof(string));
            dataTable.Columns.Add("INDEX_NAME", typeof(string));
            dataTable.Columns.Add("PRIMARY_KEY", typeof(bool));
            dataTable.Columns.Add("UNIQUE", typeof(bool));
            dataTable.Columns.Add("FOREIGN_KEY_INDEX", typeof(bool));
            dataTable.Columns.Add("EXPRESSION", typeof(string));
            dataTable.Columns.Add("FULLTEXTSEARCH", typeof(bool));
            dataTable.DefaultView.Sort = "TABLE_NAME, INDEX_NAME";
            dataTable.BeginLoadData();
            foreach (string key in (IEnumerable<string>)GetTables(tableName, null).Keys)
            {
                IVistaDBTableSchema vistaDbTableSchema = localSqlConnection.TableSchema(key);
                foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)vistaDbTableSchema.Indexes.Values)
                {
                    if (indexName == null || CompareString(indexName, indexInformation.Name, true) == 0)
                    {
                        DataRow row = dataTable.NewRow();
                        row["TABLE_NAME"] = key == null ? SystemSchema : (object)vistaDbTableSchema.Name;
                        row["INDEX_NAME"] = indexInformation.Name;
                        row["PRIMARY_KEY"] = indexInformation.Primary;
                        row["UNIQUE"] = indexInformation.Unique;
                        row["FOREIGN_KEY_INDEX"] = indexInformation.FKConstraint;
                        row["EXPRESSION"] = indexInformation.KeyExpression;
                        row["FullTextSearch"] = indexInformation.FullTextSearch;
                        dataTable.Rows.Add(row);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaIndexColumns(string tableName, string indexName, string columnName)
        {
            DataTable dataTable = new DataTable("IndexColumns");
            tableName = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableName);
            indexName = VistaDBCommandBuilder.InternalUnquoteIdentifier(indexName);
            columnName = VistaDBCommandBuilder.InternalUnquoteIdentifier(columnName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_NAME", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("COLUMN_NAME", typeof(string));
            dataTable.Columns.Add("ORDINAL_POSITION", typeof(int));
            dataTable.Columns.Add("KEYTYPE", typeof(ushort));
            dataTable.Columns.Add("INDEX_NAME", typeof(string));
            dataTable.DefaultView.Sort = "TABLE_NAME, INDEX_NAME, ORDINAL_POSITION";
            dataTable.BeginLoadData();
            foreach (string key in (IEnumerable<string>)GetTables(tableName, null).Keys)
            {
                IVistaDBTableSchema vistaDbTableSchema = localSqlConnection.TableSchema(key);
                foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)vistaDbTableSchema.Indexes.Values)
                {
                    if (indexName == null || CompareString(indexName, indexInformation.Name, true) == 0)
                    {
                        IVistaDBKeyColumn[] keyStructure = indexInformation.KeyStructure;
                        int index = 0;
                        for (int length = keyStructure.Length; index < length; ++index)
                        {
                            IVistaDBColumnAttributes columnAttributes = vistaDbTableSchema[keyStructure[index].RowIndex];
                            if (columnName == null || CompareString(columnName, columnAttributes.Name, true) == 0)
                            {
                                DataRow row = dataTable.NewRow();
                                row["CONSTRAINT_NAME"] = indexInformation.Name;
                                row["TABLE_NAME"] = key == null ? SystemSchema : (object)vistaDbTableSchema.Name;
                                row["COLUMN_NAME"] = columnAttributes.Name;
                                row["ORDINAL_POSITION"] = index;
                                row["INDEX_NAME"] = indexInformation.Name;
                                dataTable.Rows.Add(row);
                            }
                        }
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaForeignKeys(string tableName, string keyName)
        {
            DataTable dataTable = new DataTable("ForeignKeys");
            tableName = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableName);
            keyName = VistaDBCommandBuilder.InternalUnquoteIdentifier(keyName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_NAME", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_TYPE", typeof(string));
            dataTable.Columns.Add("IS_DEFERRABLE", typeof(string));
            dataTable.Columns.Add("INITIALLY_DEFERRED", typeof(string));
            dataTable.Columns.Add("FKEY_TO_TABLE", typeof(string));
            dataTable.Columns.Add("FKEY_TO_CATALOG", typeof(string));
            dataTable.Columns.Add("FKEY_TO_SCHEMA", typeof(string));
            dataTable.DefaultView.Sort = "TABLE_NAME, CONSTRAINT_NAME";
            dataTable.BeginLoadData();
            List<string> stringList;
            if (tableName == null)
            {
                stringList = new List<string>(localSqlConnection.GetTables());
            }
            else
            {
                stringList = new List<string>();
                stringList.Add(tableName);
            }
            foreach (string tableName1 in stringList)
            {
                IVistaDBTableSchema vistaDbTableSchema = localSqlConnection.TableSchema(tableName1);
                if (keyName == null)
                {
                    foreach (IVistaDBRelationshipInformation foreignKey in (IEnumerable<IVistaDBRelationshipInformation>)vistaDbTableSchema.ForeignKeys)
                    {
                        DataRow row = dataTable.NewRow();
                        row["CONSTRAINT_NAME"] = foreignKey.Name;
                        row["TABLE_NAME"] = tableName1 == null ? SystemSchema : (object)vistaDbTableSchema.Name;
                        row["CONSTRAINT_TYPE"] = "FOREIGN KEY";
                        row["IS_DEFERRABLE"] = "NO";
                        row["INITIALLY_DEFERRED"] = "NO";
                        row["FKEY_TO_TABLE"] = foreignKey.PrimaryTable == null ? SystemSchema : (object)foreignKey.PrimaryTable;
                        dataTable.Rows.Add(row);
                    }
                }
                else
                {
                    IVistaDBRelationshipInformation relationshipInformation;
                    if (vistaDbTableSchema.ForeignKeys.TryGetValue(keyName, out relationshipInformation))
                    {
                        DataRow row = dataTable.NewRow();
                        row["CONSTRAINT_NAME"] = relationshipInformation.Name;
                        row["TABLE_NAME"] = tableName1 == null ? SystemSchema : (object)vistaDbTableSchema.Name;
                        row["CONSTRAINT_TYPE"] = "FOREIGN KEY";
                        row["IS_DEFERRABLE"] = "NO";
                        row["INITIALLY_DEFERRED"] = "NO";
                        row["FKEY_TO_TABLE"] = relationshipInformation.PrimaryTable == null ? SystemSchema : (object)relationshipInformation.PrimaryTable;
                        dataTable.Rows.Add(row);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaForeignKeyColumns(string tableName, string keyName, string columnName)
        {
            DataTable dataTable = new DataTable("ForeignKeyColumns");
            tableName = VistaDBCommandBuilder.InternalUnquoteIdentifier(tableName);
            keyName = VistaDBCommandBuilder.InternalUnquoteIdentifier(keyName);
            columnName = VistaDBCommandBuilder.InternalUnquoteIdentifier(columnName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_NAME", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("CONSTRAINT_TYPE", typeof(string));
            dataTable.Columns.Add("IS_DEFERRABLE", typeof(bool));
            dataTable.Columns.Add("INITIALLY_DEFERRED", typeof(bool));
            dataTable.Columns.Add("FKEY_FROM_COLUMN", typeof(string));
            dataTable.Columns.Add("FKEY_FROM_ORDINAL_POSITION", typeof(int));
            dataTable.Columns.Add("FKEY_TO_CATALOG", typeof(string));
            dataTable.Columns.Add("FKEY_TO_SCHEMA", typeof(string));
            dataTable.Columns.Add("FKEY_TO_TABLE", typeof(string));
            dataTable.Columns.Add("FKEY_TO_COLUMN", typeof(string));
            dataTable.DefaultView.Sort = "TABLE_NAME, CONSTRAINT_NAME, FKEY_FROM_ORDINAL_POSITION";
            dataTable.BeginLoadData();
            foreach (string key in (IEnumerable<string>)GetTables(tableName, null).Keys)
            {
                IVistaDBTableSchema vistaDbTableSchema1 = localSqlConnection.TableSchema(key);
                foreach (IVistaDBRelationshipInformation relationshipInformation in (IEnumerable<IVistaDBRelationshipInformation>)vistaDbTableSchema1.ForeignKeys.Values)
                {
                    if (keyName == null || CompareString(keyName, relationshipInformation.Name, false) == 0)
                    {
                        IVistaDBKeyColumn[] keyStructure = vistaDbTableSchema1.Indexes[relationshipInformation.Name].KeyStructure;
                        IVistaDBKeyColumn[] vistaDbKeyColumnArray = null;
                        IVistaDBTableSchema vistaDbTableSchema2 = localSqlConnection.TableSchema(relationshipInformation.PrimaryTable);
                        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>)vistaDbTableSchema2.Indexes.Values)
                        {
                            if (indexInformation.Primary)
                            {
                                vistaDbKeyColumnArray = indexInformation.KeyStructure;
                                break;
                            }
                        }
                        int index = 0;
                        for (int length = keyStructure.Length; index < length; ++index)
                        {
                            string name = vistaDbTableSchema1[keyStructure[index].RowIndex].Name;
                            if (columnName == null || CompareString(columnName, name, true) == 0)
                            {
                                DataRow row = dataTable.NewRow();
                                row["CONSTRAINT_NAME"] = relationshipInformation.Name;
                                row["TABLE_NAME"] = key == null ? SystemSchema : (object)vistaDbTableSchema1.Name;
                                row["CONSTRAINT_TYPE"] = "FOREIGN KEY";
                                row["IS_DEFERRABLE"] = false;
                                row["INITIALLY_DEFERRED"] = false;
                                row["FKEY_FROM_COLUMN"] = name;
                                row["FKEY_FROM_ORDINAL_POSITION"] = index;
                                row["FKEY_TO_TABLE"] = relationshipInformation.PrimaryTable == null ? SystemSchema : (object)relationshipInformation.PrimaryTable;
                                row["FKEY_TO_COLUMN"] = vistaDbTableSchema2[vistaDbKeyColumnArray[index].RowIndex].Name;
                                dataTable.Rows.Add(row);
                            }
                        }
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaReservedWords()
        {
            DataTable dataTable = new DataTable("ReservedWords");
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("ReservedWord", typeof(string));
            dataTable.BeginLoadData();
            using (StringReader stringReader = new StringReader(SQLResource.ReservedWords_VDB4))
            {
                for (string str = stringReader.ReadLine(); str != null; str = stringReader.ReadLine())
                {
                    DataRow row = dataTable.NewRow();
                    row[0] = str;
                    dataTable.Rows.Add(row);
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaStoredProcedureParameters(string storedProcedure)
        {
            string commandText = "SELECT PARAM_NAME,PARAM_TYPE,PARAM_ORDER,IS_PARAM_OUT,PROC_NAME FROM sp_stored_procedures()  WHERE PARAM_ORDER > -1 UNION ALL SELECT PARAM_NAME,PARAM_TYPE,PARAM_ORDER,IS_PARAM_OUT,PROC_NAME FROM sp_udf()  WHERE PARAM_ORDER > -1";
            DataTable dataTable = new DataTable("SpParameters");
            storedProcedure = VistaDBCommandBuilder.InternalUnquoteIdentifier(storedProcedure);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("SPECIFIC_CATALOG", typeof(string));
            dataTable.Columns.Add("SPECIFIC_SCHEMA", typeof(string));
            dataTable.Columns.Add("SPECIFIC_NAME", typeof(string));
            dataTable.Columns.Add("ORDINAL_POSITION", typeof(string));
            dataTable.Columns.Add("PARAMETER_MODE", typeof(string));
            dataTable.Columns.Add("IS_RESULT", typeof(string));
            dataTable.Columns.Add("AS_LOCATOR", typeof(string));
            dataTable.Columns.Add("PARAMETER_NAME", typeof(string));
            dataTable.Columns.Add("DATA_TYPE", typeof(string));
            dataTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
            dataTable.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(int));
            dataTable.Columns.Add("COLLATION_CATALOG", typeof(string));
            dataTable.Columns.Add("COLLATION_SCHEMA", typeof(string));
            dataTable.Columns.Add("COLLATION_NAME", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_CATALOG", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_SCHEMA", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_NAME", typeof(string));
            dataTable.Columns.Add("NUMERIC_PRECISION", typeof(byte));
            dataTable.Columns.Add("NUMERIC_PRECISION_RADIX", typeof(short));
            dataTable.Columns.Add("NUMERIC_SCALE", typeof(int));
            dataTable.Columns.Add("DATETIME_PRECISION", typeof(short));
            dataTable.Columns.Add("INTERVAL_TYPE", typeof(string));
            dataTable.Columns.Add("INTERVAL_PRECISION", typeof(short));
            dataTable.Columns.Add("PROCEDURE_NAME", typeof(string));
            dataTable.Columns.Add("PARAMETER_DATA_TYPE", typeof(string));
            dataTable.Columns.Add("PARAMETER_SIZE", typeof(int));
            dataTable.Columns.Add("PARAMETER_DIRECTION", typeof(int));
            dataTable.Columns.Add("IS_NULLABLE", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.DefaultView.Sort = "PARAMETER_NAME";
            dataTable.BeginLoadData();
            if (!string.IsNullOrEmpty(storedProcedure))
                commandText = "SELECT * FROM (" + commandText + ") WHERE UPPER(PROC_NAME) = '" + storedProcedure.ToUpperInvariant() + "'";
            IVistaDBTableSchema tableSchema = GetTableSchema(null);
            using (VistaDBCommand vistaDbCommand = new VistaDBCommand(commandText, this))
            {
                using (VistaDBDataReader vistaDbDataReader = vistaDbCommand.ExecuteReader())
                {
                    while (vistaDbDataReader.Read())
                    {
                        string name = vistaDbDataReader["PARAM_NAME"] as string;
                        VistaDBType type = (VistaDBType)vistaDbDataReader["PARAM_TYPE"];
                        IVistaDBColumnAttributes columnAttributes = tableSchema.AddColumn(name, type);
                        DataRow row = dataTable.NewRow();
                        row["PARAMETER_NAME"] = name;
                        row["DATA_TYPE"] = type;
                        row["ORDINAL_POSITION"] = vistaDbDataReader["PARAM_ORDER"];
                        row["PARAMETER_DIRECTION"] = vistaDbDataReader["IS_PARAM_OUT"];
                        row["PROCEDURE_NAME"] = vistaDbDataReader["PROC_NAME"];
                        row["SPECIFIC_NAME"] = vistaDbDataReader["PROC_NAME"];
                        row["PARAMETER_DATA_TYPE"] = columnAttributes.Type.ToString();
                        row["IS_NULLABLE"] = "YES";
                        row["PARAMETER_SIZE"] = 0;
                        row["NUMERIC_PRECISION"] = 0;
                        row["NUMERIC_SCALE"] = 0;
                        dataTable.Rows.Add(row);
                        tableSchema.DropColumn(name);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaStoredProcedures(string storedProcedure, string objectsFilter)
        {
            DataTable dataTable = new DataTable("StoredProcedures");
            storedProcedure = VistaDBCommandBuilder.InternalUnquoteIdentifier(storedProcedure);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("SPECIFIC_CATALOG", typeof(string));
            dataTable.Columns.Add("SPECIFIC_SCHEMA", typeof(string));
            dataTable.Columns.Add("SPECIFIC_NAME", typeof(string));
            dataTable.Columns.Add("ROUTINE_CATALOG", typeof(string));
            dataTable.Columns.Add("ROUTINE_SCHEMA", typeof(string));
            dataTable.Columns.Add("ROUTINE_NAME", typeof(string));
            dataTable.Columns.Add("ROUTINE_TYPE", typeof(string));
            dataTable.Columns.Add("CREATED", typeof(DateTime));
            dataTable.Columns.Add("LAST_ALTERED", typeof(DateTime));
            dataTable.Columns.Add("ROUTINE_DESCRIPTION", typeof(string));
            dataTable.Columns.Add("ROUTINE_DEFINITION", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.DefaultView.Sort = "ROUTINE_NAME";
            dataTable.BeginLoadData();
            string commandText = "SELECT DISTINCT PROC_NAME as ROUTINE_NAME, Proc_description as ROUTINE_DESC, PROC_BODY as 'ROUTINE_DEFINITION', 'PROCEDURE' as ROUTINE_TYPE FROM sp_stored_procedures() UNION SELECT DISTINCT PROC_NAME as ROUTINE_NAME,Proc_description as ROUTINE_DESC, PROC_BODY as 'ROUTINE_DEFINITION', 'FUNCTION' as ROUTINE_TYPE FROM sp_udf()";
            bool flag = false;
            if (!string.IsNullOrEmpty(storedProcedure))
            {
                flag = true;
                commandText = "SELECT * FROM ( " + commandText + ") WHERE UPPER(ROUTINE_NAME) = '" + storedProcedure.ToUpperInvariant() + "'";
            }
            if (!string.IsNullOrEmpty(objectsFilter))
            {
                string str;
                if (!flag)
                    str = "SELECT * FROM (" + commandText + ") WHERE UPPER(ROUTINE_TYPE)= '" + objectsFilter.ToUpperInvariant() + "'";
                else
                    str = commandText + " AND UPPER(ROUTINE_TYPE)= '" + objectsFilter.ToUpperInvariant() + "'";
                commandText = str;
            }
            using (VistaDBCommand vistaDbCommand = new VistaDBCommand(commandText, this))
            {
                using (VistaDBDataReader vistaDbDataReader = vistaDbCommand.ExecuteReader())
                {
                    while (vistaDbDataReader.Read())
                    {
                        DataRow row = dataTable.NewRow();
                        row["SPECIFIC_NAME"] = vistaDbDataReader["ROUTINE_NAME"];
                        row["ROUTINE_NAME"] = vistaDbDataReader["ROUTINE_NAME"];
                        row["ROUTINE_TYPE"] = vistaDbDataReader["ROUTINE_TYPE"];
                        row["ROUTINE_DESCRIPTION"] = vistaDbDataReader["ROUTINE_DESC"];
                        row["ROUTINE_DEFINITION"] = vistaDbDataReader["ROUTINE_DEFINITION"];
                        dataTable.Rows.Add(row);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaViews(string viewName)
        {
            DataTable dataTable = new DataTable("Views");
            viewName = VistaDBCommandBuilder.InternalUnquoteIdentifier(viewName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("CHECK_OPTION", typeof(string));
            dataTable.Columns.Add("IS_UPDATABLE", typeof(bool));
            dataTable.Columns.Add("TABLE_DESCRIPTION", typeof(string));
            dataTable.DefaultView.Sort = "TABLE_NAME";
            dataTable.BeginLoadData();
            string commandText = "SELECT * FROM GetViews()";
            if (viewName != null)
                commandText = commandText + " WHERE VIEW_NAME = '" + viewName + "'";
            using (VistaDBCommand vistaDbCommand = new VistaDBCommand(commandText, this))
            {
                using (VistaDBDataReader vistaDbDataReader = vistaDbCommand.ExecuteReader())
                {
                    while (vistaDbDataReader.Read())
                    {
                        DataRow row = dataTable.NewRow();
                        row["TABLE_NAME"] = vistaDbDataReader["VIEW_NAME"];
                        row["TABLE_DESCRIPTION"] = vistaDbDataReader["DESCRIPTION"];
                        row["IS_UPDATABLE"] = vistaDbDataReader["IS_UPDATABLE"];
                        dataTable.Rows.Add(row);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private DataTable GetSchemaViewColumns(string viewName, string columnName)
        {
            DataTable dataTable = new DataTable("Columns");
            viewName = VistaDBCommandBuilder.InternalUnquoteIdentifier(viewName);
            columnName = VistaDBCommandBuilder.InternalUnquoteIdentifier(columnName);
            dataTable.Locale = CultureInfo.InvariantCulture;
            dataTable.Columns.Add("VIEW_CATALOG", typeof(string));
            dataTable.Columns.Add("VIEW_SCHEMA", typeof(string));
            dataTable.Columns.Add("VIEW_NAME", typeof(string));
            dataTable.Columns.Add("TABLE_CATALOG", typeof(string));
            dataTable.Columns.Add("TABLE_SCHEMA", typeof(string));
            dataTable.Columns.Add("TABLE_NAME", typeof(string));
            dataTable.Columns.Add("COLUMN_NAME", typeof(string));
            dataTable.Columns.Add("COLUMN_GUID", typeof(Guid));
            dataTable.Columns.Add("COLUMN_PROPID", typeof(long));
            dataTable.Columns.Add("ORDINAL_POSITION", typeof(int));
            dataTable.Columns.Add("COLUMN_HASDEFAULT", typeof(bool));
            dataTable.Columns.Add("COLUMN_DEFAULT", typeof(string));
            dataTable.Columns.Add("IS_NULLABLE", typeof(bool));
            dataTable.Columns.Add("DATA_TYPE", typeof(string));
            dataTable.Columns.Add("TYPE_GUID", typeof(Guid));
            dataTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
            dataTable.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(int));
            dataTable.Columns.Add("NUMERIC_PRECISION", typeof(int));
            dataTable.Columns.Add("NUMERIC_SCALE", typeof(int));
            dataTable.Columns.Add("DATETIME_PRECISION", typeof(long));
            dataTable.Columns.Add("CHARACTER_SET_CATALOG", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_SCHEMA", typeof(string));
            dataTable.Columns.Add("CHARACTER_SET_NAME", typeof(string));
            dataTable.Columns.Add("COLLATION_CATALOG", typeof(string));
            dataTable.Columns.Add("COLLATION_SCHEMA", typeof(string));
            dataTable.Columns.Add("COLLATION_NAME", typeof(string));
            dataTable.Columns.Add("DOMAIN_CATALOG", typeof(string));
            dataTable.Columns.Add("DOMAIN_NAME", typeof(string));
            dataTable.Columns.Add("DESCRIPTION", typeof(string));
            dataTable.Columns.Add("PRIMARY_KEY", typeof(bool));
            dataTable.Columns.Add("COLUMN_CAPTION", typeof(string));
            dataTable.Columns.Add("COLUMN_ENCRYPTED", typeof(bool));
            dataTable.Columns.Add("COLUMN_PACKED", typeof(bool));
            dataTable.DefaultView.Sort = "TABLE_NAME";
            dataTable.BeginLoadData();
            string commandText = viewName == null ? "SELECT * FROM GetViewColumns()" : "SELECT * FROM GetViewColumns('" + viewName + "')";
            if (columnName != null)
                commandText = commandText + " WHERE COLUMN_NAME = '" + columnName + "'";
            using (VistaDBCommand vistaDbCommand = new VistaDBCommand(commandText, this))
            {
                using (VistaDBDataReader vistaDbDataReader = vistaDbCommand.ExecuteReader())
                {
                    while (vistaDbDataReader.Read())
                    {
                        DataRow row = dataTable.NewRow();
                        row["VIEW_NAME"] = vistaDbDataReader["VIEW_NAME"];
                        row["COLUMN_NAME"] = vistaDbDataReader["COLUMN_NAME"];
                        row["ORDINAL_POSITION"] = vistaDbDataReader["COLUMN_ORDINAL"];
                        row["COLUMN_HASDEFAULT"] = vistaDbDataReader["DEFAULT_VALUE"] != DBNull.Value;
                        row["COLUMN_DEFAULT"] = vistaDbDataReader["DEFAULT_VALUE"];
                        row["IS_NULLABLE"] = vistaDbDataReader["ALLOW_NULL"];
                        row["DATA_TYPE"] = vistaDbDataReader["DATA_TYPE_NAME"];
                        row["CHARACTER_MAXIMUM_LENGTH"] = vistaDbDataReader["COLUMN_SIZE"];
                        row["CHARACTER_SET_NAME"] = (int)vistaDbDataReader["CODE_PAGE"] == 0 ? DBNull.Value : (object)vistaDbDataReader["CODE_PAGE"].ToString();
                        row["DESCRIPTION"] = vistaDbDataReader["COLUMN_DESCRIPTION"];
                        row["PRIMARY_KEY"] = vistaDbDataReader["IS_KEY"];
                        row["COLUMN_CAPTION"] = vistaDbDataReader["COLUMN_CAPTION"];
                        dataTable.Rows.Add(row);
                    }
                }
            }
            dataTable.AcceptChanges();
            dataTable.EndLoadData();
            return dataTable;
        }

        private object SyncRoot
        {
            get
            {
                return this;
            }
        }

        internal IQueryStatement CreateQuery(string commandText)
        {
            InstantiateLocalSqlConnection(null);
            return localSqlConnection.CreateQuery(commandText);
        }

        internal void FreeQuery(IQueryStatement query, bool cleanup)
        {
            if (localSqlConnection == null)
                return;
            localSqlConnection.FreeQuery(query);
            if (!cleanup || localSqlConnection.CurrentTransaction != null || (OpenMode == VistaDBDatabaseOpenMode.ExclusiveReadWrite || OpenMode == VistaDBDatabaseOpenMode.ExclusiveReadOnly))
                return;
            localSqlConnection.CloseAllPooledTables();
        }

        internal void RetrieveConnectionInfo()
        {
            if (CompareString(DataSource, localSqlConnection.FileName, true) == 0)
                return;
            connectionString.DataSource = localSqlConnection.FileName;
            connectionString.OpenMode = localSqlConnection.OpenMode;
            connectionString.Password = localSqlConnection.Password;
        }

        internal void InternalBeginTransaction(VistaDBTransaction parentTransaction)
        {
            if (TransactionMode == VistaDBTransaction.TransactionMode.Off)
                throw new VistaDBException(460);
            lock (SyncRoot)
            {
                MustBeOpened();
                if (TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
                    return;
                localSqlConnection.BeginTransaction(parentTransaction);
            }
        }

        internal void InternalCommitTransaction()
        {
            if (TransactionMode == VistaDBTransaction.TransactionMode.Off)
                throw new VistaDBException(460);
            if (TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
                return;
            lock (SyncRoot)
            {
                MustBeOpened();
                localSqlConnection.CommitTransaction();
            }
        }

        internal void InternalRollbackTransaction()
        {
            if (TransactionMode == VistaDBTransaction.TransactionMode.Off)
                throw new VistaDBException(460);
            if (TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
                return;
            lock (SyncRoot)
            {
                MustBeOpened();
                localSqlConnection.RollbackTransaction();
            }
        }

        internal VistaDBTransaction Transaction
        {
            get
            {
                lock (SyncRoot)
                    return localSqlConnection == null ? null : localSqlConnection.CurrentTransaction;
            }
        }

        object ICloneable.Clone()
        {
            VistaDBConnection vistaDbConnection = new VistaDBConnection(ConnectionString);
            lock (SyncRoot)
            {
                if (State == ConnectionState.Open)
                    vistaDbConnection.Open();
            }
            return vistaDbConnection;
        }

        public static class SchemaConstants
        {
            public const string SCHEMA_METADATACOLLECTIONS = "METADATACOLLECTIONS";
            public const string SCHEMA_DATASOURCEINFORMATION = "DATASOURCEINFORMATION";
            public const string SCHEMA_DATATYPES = "DATATYPES";
            public const string SCHEMA_COLUMNS = "COLUMNS";
            public const string SCHEMA_INDEXES = "INDEXES";
            public const string SCHEMA_INDEXCOLUMNS = "INDEXCOLUMNS";
            public const string SCHEMA_TABLES = "TABLES";
            public const string SCHEMA_FOREIGNKEYS = "FOREIGNKEYS";
            public const string SCHEMA_FOREIGNKEYCOLUMNS = "FOREIGNKEYCOLUMNS";
            public const string SCHEMA_RESERVEDWORDS = "RESERVEDWORDS";
            public const string SCHEMA_VIEWS = "VIEWS";
            public const string SCHEMA_VIEWCOLUMNS = "VIEWCOLUMNS";
            public const string SCHEMA_STOREDPROCEDURES = "PROCEDURES";
            public const string SCHEMA_SPPARAMETERS = "PROCEDUREPARAMETERS";
            public const string SCHEMA_RESTRICTIONS = "RESTRICTIONS";
        }

        private class ConnectionPool
        {
            private int minPoolSize = 1;
            private int maxPoolSize = 100;
            private int count;
            private ILocalSQLConnection[] connections;

            internal ConnectionPool(int minPoolSize, int maxPoolSize, ILocalSQLConnection conn)
            {
                this.minPoolSize = minPoolSize;
                this.maxPoolSize = maxPoolSize;
                count = 0;
                connections = new ILocalSQLConnection[maxPoolSize];
                Push(conn);
            }

            internal ILocalSQLConnection Pop()
            {
                lock (connections)
                {
                    if (count > 0)
                    {
                        ILocalSQLConnection connection = connections[count - 1];
                        connections[count--] = count <= minPoolSize ? null : (ILocalSQLConnection)null;
                        return connection;
                    }
                }
                return null;
            }

            internal void Push(ILocalSQLConnection connection)
            {
                if (connection == null)
                    return;
                lock (connections)
                {
                    if (count >= maxPoolSize)
                        connection.Dispose();
                    else
                        connections[count++] = connection;
                }
            }

            public void Clear()
            {
                lock (connections)
                {
                    int index = 0;
                    for (int count = this.count; index < count; ++index)
                    {
                        connections[index].Dispose();
                        connections[index] = null;
                    }
                }
            }
        }

        private class ConnectionPoolCollection
        {
            private Dictionary<string, ConnectionPool> pools;

            public ConnectionPoolCollection()
            {
                pools = new Dictionary<string, ConnectionPool>(StringComparer.OrdinalIgnoreCase);
            }

            public void Clear()
            {
                lock (pools)
                {
                    foreach (ConnectionPool connectionPool in pools.Values)
                        connectionPool.Clear();
                    pools.Clear();
                }
            }

            internal void ClearPool(VistaDBConnection connection)
            {
                lock (pools)
                {
                    string connectionString = connection.GetPoolConnectionString();
                    ConnectionPool connectionPool;
                    if (!pools.TryGetValue(connectionString, out connectionPool))
                        return;
                    connectionPool.Clear();
                    pools.Remove(connectionString);
                }
            }

            internal bool PutConnectionOnHold(ILocalSQLConnection connection, string connectionString, int minPoolSize, int maxPoolSize)
            {
                lock (pools)
                {
                    if (connection is IPooledSQLConnection)
                        ((IPooledSQLConnection)connection).PrepareConnectionForPool();
                    connection.CloseAllPooledTables();
                    ConnectionPool connectionPool;
                    if (pools.TryGetValue(connectionString, out connectionPool))
                    {
                        connectionPool.Push(connection);
                        return false;
                    }
                    if (minPoolSize <= 0 || connectionString == null)
                        return connection != null;
                    pools.Add(connectionString, new ConnectionPool(minPoolSize, maxPoolSize, connection));
                }
                return false;
            }

            internal ILocalSQLConnection GetConnection(string connectionString)
            {
                if (connectionString == null)
                    return null;
                if (pools.Count == 0)
                    return null;
                ConnectionPool connectionPool;
                if (pools.TryGetValue(connectionString, out connectionPool))
                    return connectionPool.Pop();
                return null;
            }
        }
    }
}
