using System;
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
    private static readonly VistaDBConnection.ConnectionPoolCollection PoolsCollection = new VistaDBConnection.ConnectionPoolCollection();
    public static readonly string SystemSchema = VistaDB.Engine.Core.Database.SystemSchema;
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
      VistaDBConnection.PoolsCollection.Clear();
    }

    public static void ClearPool(VistaDBConnection connection)
    {
      VistaDBConnection.PoolsCollection.ClearPool(connection);
    }

    public VistaDBConnection()
    {
      this.connectionString = new VistaDBConnectionStringBuilder();
      this.canPack = true;
    }

    public VistaDBConnection(string connectionString)
      : this()
    {
      this.ConnectionString = connectionString;
    }

    public VistaDBConnection(IVistaDBDatabase database)
      : this()
    {
      this.canPack = false;
      this.InstantiateLocalSqlConnection((IDatabase) database);
    }

    public override string ConnectionString
    {
      get
      {
        return this.connectionString.ConnectionString;
      }
      set
      {
        this.connectionString.ConnectionString = value;
      }
    }

    public bool ContextConnection
    {
      get
      {
        return this.connectionString.ContextConnection;
      }
    }

    public override string Database
    {
      get
      {
        return this.connectionString.Database;
      }
    }

    public override string DataSource
    {
      get
      {
        return this.connectionString.DataSource;
      }
    }

    public bool IsolatedStorage
    {
      get
      {
        return this.connectionString.IsolatedStorage;
      }
    }

    public VistaDBTransaction.TransactionMode TransactionMode
    {
      get
      {
        this.MustBeOpened();
        return this.connectionString.TransactionMode;
      }
    }

    public VistaDBDatabaseOpenMode OpenMode
    {
      get
      {
        return this.connectionString.OpenMode;
      }
    }

    public string Password
    {
      get
      {
        return this.connectionString.Password;
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
        if (this.localSqlConnection != null)
          return this.localSqlConnection.LockTimeout;
        return -1;
      }
      set
      {
        if (this.localSqlConnection == null)
          return;
        this.localSqlConnection.LockTimeout = value;
      }
    }

    public bool PersistentLockFiles
    {
      get
      {
        if (this.localSqlConnection != null)
          return this.localSqlConnection.PersistentLockFiles;
        return false;
      }
      set
      {
        if (this.localSqlConnection == null)
          return;
        this.localSqlConnection.PersistentLockFiles = value;
      }
    }

    public override ConnectionState State
    {
      get
      {
        lock (this.SyncRoot)
          return this.localSqlConnection == null || !this.localSqlConnection.DatabaseOpened ? ConnectionState.Closed : ConnectionState.Open;
      }
    }

    public VistaDBTransaction BeginTransaction()
    {
      this.MustBeOpened();
      return new VistaDBTransaction(this);
    }

    public VistaDBTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
      if (isolationLevel != IsolationLevel.ReadCommitted && isolationLevel != IsolationLevel.Unspecified)
        throw new VistaDBSQLException(1013, "Use IsolationLevel.ReadCommitted instead of " + isolationLevel.ToString(), 0, 0);
      return this.BeginTransaction();
    }

    public override void ChangeDatabase(string databaseName)
    {
      throw new NotImplementedException("The method is not supported.");
    }

    public override void Close()
    {
      ConnectionState state;
      lock (this.SyncRoot)
      {
        state = this.State;
        if (state == ConnectionState.Closed)
          return;
        if (this.ContextConnection)
        {
          this.localSqlConnection = (ILocalSQLConnection) null;
          return;
        }
        if (!this.localSqlConnection.IsDatabaseOwner)
        {
          this.localSqlConnection.CloseAllPooledTables();
          return;
        }
        bool close = true;
        if (this.connectionString.Pooling)
          close = VistaDBConnection.PoolsCollection.PutConnectionOnHold(this.localSqlConnection, this.GetPoolConnectionString(), this.connectionString.MinPoolSize, this.connectionString.MaxPoolSize);
        this.ClearLocalSqlConnection(close);
      }
      if (state != ConnectionState.Open)
        return;
      this.OnStateChange(new StateChangeEventArgs(state, ConnectionState.Closed));
    }

    public VistaDBCommand CreateCommand()
    {
      return new VistaDBCommand(string.Empty, this);
    }

    public IVistaDBTableSchema GetTableSchema(string tableName)
    {
      if (this.State != ConnectionState.Open)
        throw new InvalidOperationException();
      return this.localSqlConnection.TableSchema(tableName);
    }

    public override DataTable GetSchema()
    {
      return this.GetSchema("METADATACOLLECTIONS", (string[]) null);
    }

    public override DataTable GetSchema(string collectionName)
    {
      return this.GetSchema(collectionName, (string[]) null);
    }

    public override DataTable GetSchema(string collectionName, string[] restrictionValues)
    {
      if (string.IsNullOrEmpty(collectionName))
        collectionName = "METADATACOLLECTIONS";
      lock (this.SyncRoot)
      {
        if (this.State != ConnectionState.Open)
          throw new InvalidOperationException();
        string[] strArray = new string[5];
        restrictionValues?.CopyTo((Array) strArray, 0);
        switch (this.UpperString(collectionName))
        {
          case "METADATACOLLECTIONS":
            return this.GetSchemaMetaDataCollections();
          case "DATASOURCEINFORMATION":
            return this.GetSchemaDataSourceInformation();
          case "DATATYPES":
            return this.GetSchemaDataTypes();
          case "TABLES":
            return this.GetSchemaTables(strArray[2], strArray[3]);
          case "COLUMNS":
            return this.GetSchemaColumns(strArray[2], strArray[3]);
          case "INDEXES":
            return this.GetSchemaIndexes(strArray[2], strArray[3]);
          case "INDEXCOLUMNS":
            return this.GetSchemaIndexColumns(strArray[2], strArray[3], strArray[4]);
          case "FOREIGNKEYS":
            return this.GetSchemaForeignKeys(strArray[2], strArray[3]);
          case "FOREIGNKEYCOLUMNS":
            return this.GetSchemaForeignKeyColumns(strArray[2], strArray[3], strArray[4]);
          case "RESERVEDWORDS":
            return this.GetSchemaReservedWords();
          case "VIEWS":
            return this.GetSchemaViews(strArray[2]);
          case "VIEWCOLUMNS":
            return this.GetSchemaViewColumns(strArray[2], strArray[3]);
          case "PROCEDURES":
            return this.GetSchemaStoredProcedures(strArray[3], strArray[2]);
          case "PROCEDUREPARAMETERS":
            return this.GetSchemaStoredProcedureParameters(strArray[2]);
          case "RESTRICTIONS":
            return this.GetSchemaRestrictions();
        }
      }
      throw new NotSupportedException();
    }

    public bool IsSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
    {
      lock (this.SyncRoot)
      {
        this.InstantiateLocalSqlConnection((IDatabase) null);
        return this.localSqlConnection.IsSyntaxCorrect(text, out lineNo, out symbolNo, out errorMessage);
      }
    }

    public bool IsViewSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
    {
      lock (this.SyncRoot)
      {
        this.InstantiateLocalSqlConnection((IDatabase) null);
        return this.localSqlConnection.IsViewSyntaxCorrect(text, out lineNo, out symbolNo, out errorMessage);
      }
    }

    public bool IsConstraintSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
    {
      lock (this.SyncRoot)
      {
        this.InstantiateLocalSqlConnection((IDatabase) null);
        return this.localSqlConnection.IsConstraintSyntaxCorrect(text, out lineNo, out symbolNo, out errorMessage);
      }
    }

    public override void Open()
    {
      lock (this.SyncRoot)
      {
        ConnectionState state = this.State;
        if (state == ConnectionState.Open)
          return;
        if (this.ContextConnection)
        {
          this.localSqlConnection = VistaDBContext.SQLChannel.CurrentConnection;
          if (this.localSqlConnection == null)
            throw new VistaDBSQLException(1009, string.Empty, 0, 0);
        }
        else
        {
          if (this.localSqlConnection != null)
          {
            this.localSqlConnection.Dispose();
            this.localSqlConnection = (ILocalSQLConnection) null;
          }
          if (this.connectionString.Pooling)
            this.localSqlConnection = VistaDBConnection.PoolsCollection.GetConnection(this.GetPoolConnectionString());
          if (this.localSqlConnection != null)
          {
            if (!(this.localSqlConnection is IPooledSQLConnection))
              return;
            ((IPooledSQLConnection) this.localSqlConnection).InitializeConnectionFromPool((DbConnection) this);
          }
          else
          {
            this.InstantiateLocalSqlConnection((IDatabase) null);
            string cryptoKeyString = this.Password;
            if (cryptoKeyString == string.Empty)
              cryptoKeyString = (string) null;
            this.localSqlConnection.OpenDatabase(this.DataSource, this.OpenMode, cryptoKeyString, this.IsolatedStorage);
            if (state != ConnectionState.Closed)
              return;
            this.OnStateChange(new StateChangeEventArgs(state, ConnectionState.Open));
          }
        }
      }
    }

    private void PackOperationCallback(IVistaDBOperationCallbackStatus status)
    {
      if (this.InfoMessageHandler == null)
        return;
      this.InfoMessageHandler((object) this, new VistaDBInfoMessageEventArgs(status.Message, status.ObjectName));
    }

    public void PackDatabase()
    {
      this.PackDatabase(false);
    }

    public void PackDatabase(bool backup)
    {
      if (!this.canPack)
        throw new VistaDBException(347);
      bool flag = this.State == ConnectionState.Open;
      this.Close();
      string encryptionKeyString = this.Password;
      if (string.IsNullOrEmpty(encryptionKeyString))
        encryptionKeyString = (string) null;
      VistaDBConnection.PackDatabase(this.DataSource, encryptionKeyString, backup, new OperationCallbackDelegate(this.PackOperationCallback));
      if (!flag)
        return;
      this.Open();
    }

    private void RepairOperationCallback(IVistaDBOperationCallbackStatus status)
    {
      if (this.InfoMessageHandler == null)
        return;
      this.InfoMessageHandler((object) this, new VistaDBInfoMessageEventArgs(status.Message, status.ObjectName));
    }

    public void RepairDatabase()
    {
      if (!this.canPack)
        throw new VistaDBException(347);
      bool flag = this.State == ConnectionState.Open;
      this.Close();
      string encryptionKeyString = this.Password;
      if (string.IsNullOrEmpty(encryptionKeyString))
        encryptionKeyString = (string) null;
      VistaDBConnection.RepairDatabase(this.DataSource, encryptionKeyString, new OperationCallbackDelegate(this.RepairOperationCallback));
      if (!flag)
        return;
      this.Open();
    }

    internal void OnPrintMessage(string message)
    {
      VistaDBInfoMessageEventHandler infoMessageHandler = this.InfoMessageHandler;
      if (infoMessageHandler == null)
        return;
      infoMessageHandler((object) this, new VistaDBInfoMessageEventArgs(message, string.Empty));
    }

    public event VistaDBInfoMessageEventHandler InfoMessage
    {
      add
      {
        this.Events.AddHandler(VistaDBConnection.InfoMessageEvent, (Delegate) value);
      }
      remove
      {
        this.Events.RemoveHandler(VistaDBConnection.InfoMessageEvent, (Delegate) value);
      }
    }

    private VistaDBInfoMessageEventHandler InfoMessageHandler
    {
      get
      {
        return (VistaDBInfoMessageEventHandler) this.Events[VistaDBConnection.InfoMessageEvent];
      }
    }

    protected override DbProviderFactory DbProviderFactory
    {
      get
      {
        return (DbProviderFactory) VistaDBProviderFactory.Instance;
      }
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
      return (DbTransaction) this.BeginTransaction(isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
      return (DbCommand) this.CreateCommand();
    }

    protected override void Dispose(bool disposing)
    {
      if (disposing)
        this.Close();
      base.Dispose(disposing);
    }

    private void InstantiateLocalSqlConnection(IDatabase database)
    {
      if (this.localSqlConnection != null)
        return;
      this.localSqlConnection = VistaDBEngine.Connections.OpenSQLConnection(this, database);
    }

    private void ClearLocalSqlConnection(bool close)
    {
      if (this.localSqlConnection == null)
        return;
      this.localSqlConnection.CloseAllPooledTables();
      if (close)
        this.localSqlConnection.Dispose();
      this.localSqlConnection = (ILocalSQLConnection) null;
    }

    private string GetPoolConnectionString()
    {
      if (this.DataSource == null || this.DataSource.Length == 0)
        return (string) null;
      return this.DataSource + ";" + this.OpenMode.ToString() + ";" + this.Password;
    }

    private void CheckState()
    {
      if (this.State == ConnectionState.Open)
        throw new VistaDBSQLException(1005, string.Empty, 0, 0);
    }

    private void MustBeOpened()
    {
      if (this.State != ConnectionState.Open)
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
      dataTable.Columns.Add("CollectionName", typeof (string));
      dataTable.Columns.Add("NumberOfRestrictions", typeof (int));
      dataTable.Columns.Add("NumberOfIdentifierParts", typeof (int));
      dataTable.BeginLoadData();
      using (StringReader stringReader = new StringReader(SQLResource.MetaDataCollections))
      {
        int num = (int) dataTable.ReadXml((TextReader) stringReader);
      }
      dataTable.AcceptChanges();
      dataTable.EndLoadData();
      return dataTable;
    }

    private DataTable GetSchemaRestrictions()
    {
      DataTable dataTable = new DataTable("Restrictions");
      dataTable.Locale = CultureInfo.InvariantCulture;
      dataTable.Columns.Add("CollectionName", typeof (string));
      dataTable.Columns.Add("RestrictionName", typeof (string));
      dataTable.Columns.Add("RestrictionDefault", typeof (string));
      dataTable.Columns.Add("RestrictionNumber", typeof (int));
      dataTable.BeginLoadData();
      using (StringReader stringReader = new StringReader(SQLResource.Restrictions))
      {
        int num = (int) dataTable.ReadXml((TextReader) stringReader);
      }
      dataTable.AcceptChanges();
      dataTable.EndLoadData();
      return dataTable;
    }

    private DataTable GetSchemaDataSourceInformation()
    {
      DataTable dataTable = new DataTable("DataSourceInformation");
      dataTable.Locale = CultureInfo.InvariantCulture;
      dataTable.Columns.Add(DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.DataSourceProductName, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.DataSourceProductVersion, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.DataSourceProductVersionNormalized, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.GroupByBehavior, typeof (GroupByBehavior));
      dataTable.Columns.Add(DbMetaDataColumnNames.IdentifierPattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.IdentifierCase, typeof (IdentifierCase));
      dataTable.Columns.Add(DbMetaDataColumnNames.OrderByColumnsInSelect, typeof (bool));
      dataTable.Columns.Add(DbMetaDataColumnNames.ParameterMarkerFormat, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.ParameterMarkerPattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.ParameterNameMaxLength, typeof (int));
      dataTable.Columns.Add(DbMetaDataColumnNames.ParameterNamePattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.QuotedIdentifierPattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.QuotedIdentifierCase, typeof (IdentifierCase));
      dataTable.Columns.Add(DbMetaDataColumnNames.StatementSeparatorPattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.StringLiteralPattern, typeof (string));
      dataTable.Columns.Add(DbMetaDataColumnNames.SupportedJoinOperators, typeof (SupportedJoinOperators));
      dataTable.BeginLoadData();
      DataRow row = dataTable.NewRow();
      row[DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern] = (object) "\\.";
      row[DbMetaDataColumnNames.DataSourceProductName] = (object) "VistaDB";
      row[DbMetaDataColumnNames.DataSourceProductVersion] = (object) "4.3.3.34";
      row[DbMetaDataColumnNames.DataSourceProductVersionNormalized] = (object) "4.1";
      row[DbMetaDataColumnNames.IdentifierPattern] = (object) "(^\\[\\p{Lo}\\p{Lu}\\p{Ll}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Nd}@$#_]*$)|(^\\[[^\\]\\0]|\\]\\]+\\]$)|(^\\\"[^\\\"\\0]|\\\"\\\"+\\\"$)";
      row[DbMetaDataColumnNames.OrderByColumnsInSelect] = (object) false;
      row[DbMetaDataColumnNames.ParameterMarkerFormat] = (object) "{0}";
      row[DbMetaDataColumnNames.ParameterMarkerPattern] = (object) "(@[A-Za-z0-9_]+)";
      row[DbMetaDataColumnNames.ParameterNameMaxLength] = (object) 128;
      row[DbMetaDataColumnNames.ParameterNamePattern] = (object) "^[\\w_@#][\\w_@#]*(?=\\s+|$)";
      row[DbMetaDataColumnNames.QuotedIdentifierPattern] = (object) "(([^\\[]|\\]\\])*)";
      row[DbMetaDataColumnNames.StatementSeparatorPattern] = (object) ";";
      row[DbMetaDataColumnNames.StringLiteralPattern] = (object) "('([^']|'')*')";
      row[DbMetaDataColumnNames.GroupByBehavior] = (object) GroupByBehavior.Unrelated;
      row[DbMetaDataColumnNames.IdentifierCase] = (object) IdentifierCase.Insensitive;
      row[DbMetaDataColumnNames.QuotedIdentifierCase] = (object) IdentifierCase.Insensitive;
      row[DbMetaDataColumnNames.SupportedJoinOperators] = (object) (SupportedJoinOperators.Inner | SupportedJoinOperators.LeftOuter | SupportedJoinOperators.RightOuter);
      dataTable.Rows.Add(row);
      dataTable.AcceptChanges();
      dataTable.EndLoadData();
      return dataTable;
    }

    private DataTable GetSchemaDataTypes()
    {
      DataTable dataTable = new DataTable("DataTypes");
      dataTable.Locale = CultureInfo.InvariantCulture;
      dataTable.Columns.Add("TypeName", typeof (string));
      dataTable.Columns.Add("ProviderDbType", typeof (int));
      dataTable.Columns.Add("ColumnSize", typeof (long));
      dataTable.Columns.Add("CreateFormat", typeof (string));
      dataTable.Columns.Add("CreateParameters", typeof (string));
      dataTable.Columns.Add("DataType", typeof (string));
      dataTable.Columns.Add("IsAutoIncrementable", typeof (bool));
      dataTable.Columns.Add("IsBestMatch", typeof (bool));
      dataTable.Columns.Add("IsCaseSensitive", typeof (bool));
      dataTable.Columns.Add("IsFixedLength", typeof (bool));
      dataTable.Columns.Add("IsFixedPrecisionScale", typeof (bool));
      dataTable.Columns.Add("IsLong", typeof (bool));
      dataTable.Columns.Add("IsNullable", typeof (bool));
      dataTable.Columns.Add("IsSearchable", typeof (bool));
      dataTable.Columns.Add("IsSearchableWithLike", typeof (bool));
      dataTable.Columns.Add("IsUnsigned", typeof (bool));
      dataTable.Columns.Add("MaximumScale", typeof (short));
      dataTable.Columns.Add("MinimumScale", typeof (short));
      dataTable.Columns.Add("IsConcurrencyType", typeof (bool));
      dataTable.Columns.Add("IsLiteralsSupported", typeof (bool));
      dataTable.Columns.Add("LiteralPrefix", typeof (string));
      dataTable.Columns.Add("LiteralSuffix", typeof (string));
      dataTable.BeginLoadData();
      using (StringReader stringReader = new StringReader(SQLResource.DataTypes))
      {
        int num = (int) dataTable.ReadXml((TextReader) stringReader);
      }
      dataTable.AcceptChanges();
      dataTable.EndLoadData();
      return dataTable;
    }

    private VistaDB.Engine.Core.Database.TableIdMap GetTables(string tableName, string tableType)
    {
      VistaDB.Engine.Core.Database.TableIdMap tableIdMap;
      if (tableName == null)
      {
        tableIdMap = tableType == null || this.CompareString(tableType, VistaDBConnection.UserTableType, true) == 0 ? (VistaDB.Engine.Core.Database.TableIdMap) this.localSqlConnection.GetTables() : (VistaDB.Engine.Core.Database.TableIdMap) null;
        if (tableType == null || this.CompareString(tableType, VistaDBConnection.SystemTableType, true) == 0)
        {
          if (tableIdMap == null)
            tableIdMap = new VistaDB.Engine.Core.Database.TableIdMap();
          tableIdMap.AddTable((string) null);
        }
      }
      else
      {
        tableIdMap = new VistaDB.Engine.Core.Database.TableIdMap();
        if (this.CompareString(tableName, VistaDBConnection.SystemSchema, true) == 0)
        {
          if (tableType == null || this.CompareString(tableType, VistaDBConnection.UserTableType, true) == 0)
            tableIdMap.AddTable((string) null);
        }
        else if (tableType == null || this.CompareString(tableType, VistaDBConnection.SystemTableType, true) == 0)
        {
          foreach (KeyValuePair<ulong, string> table in (VistaDB.Engine.Core.Database.TableIdMap) this.localSqlConnection.GetTables())
          {
            if (this.CompareString(table.Value, tableName, true) == 0)
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
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("TABLE_TYPE", typeof (string));
      dataTable.Columns.Add("TABLE_DESCRIPTION", typeof (string));
      dataTable.DefaultView.Sort = "TABLE_NAME";
      dataTable.BeginLoadData();
      VistaDB.Engine.Core.Database.TableIdMap tables = this.GetTables(tableName, tableType);
      if (tables != null)
      {
        foreach (string key in (IEnumerable<string>) tables.Keys)
        {
          IVistaDBTableSchema vistaDbTableSchema = this.localSqlConnection.TableSchema(key);
          DataRow row = dataTable.NewRow();
          row["TABLE_NAME"] = key == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema.Name;
          row["TABLE_TYPE"] = key == null ? (object) VistaDBConnection.SystemTableType : (object) VistaDBConnection.UserTableType;
          row["TABLE_DESCRIPTION"] = (object) vistaDbTableSchema.Description;
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
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("COLUMN_NAME", typeof (string));
      dataTable.Columns.Add("ORDINAL_POSITION", typeof (int));
      dataTable.Columns.Add("COLUMN_DEFAULT", typeof (string));
      dataTable.Columns.Add("IS_NULLABLE", typeof (bool));
      dataTable.Columns.Add("DATA_TYPE", typeof (string));
      dataTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof (int));
      dataTable.Columns.Add("CHARACTER_OCTET_LENGTH", typeof (int));
      dataTable.Columns.Add("NUMERIC_PRECISION", typeof (int));
      dataTable.Columns.Add("NUMERIC_PRECISION_RADIX", typeof (short));
      dataTable.Columns.Add("NUMERIC_SCALE", typeof (int));
      dataTable.Columns.Add("DATETIME_PRECISION", typeof (long));
      dataTable.Columns.Add("CHARACTER_SET_CATALOG", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_SCHEMA", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_NAME", typeof (string));
      dataTable.Columns.Add("COLLATION_CATALOG", typeof (string));
      dataTable.Columns.Add("COLLATION_SCHEMA", typeof (string));
      dataTable.Columns.Add("COLLATION_NAME", typeof (string));
      dataTable.Columns.Add("DOMAIN_CATALOG", typeof (string));
      dataTable.Columns.Add("DOMAIN_NAME", typeof (string));
      dataTable.Columns.Add("DESCRIPTION", typeof (string));
      dataTable.Columns.Add("PRIMARY_KEY", typeof (bool));
      dataTable.Columns.Add("COLUMN_CAPTION", typeof (string));
      dataTable.Columns.Add("COLUMN_ENCRYPTED", typeof (bool));
      dataTable.Columns.Add("COLUMN_PACKED", typeof (bool));
      dataTable.Columns.Add("TYPE_GUID", typeof (Guid));
      dataTable.Columns.Add("COLUMN_HASDEFAULT", typeof (bool));
      dataTable.Columns.Add("COLUMN_GUID", typeof (Guid));
      dataTable.Columns.Add("COLUMN_PROPID", typeof (long));
      dataTable.DefaultView.Sort = "TABLE_NAME, ORDINAL_POSITION";
      dataTable.BeginLoadData();
      foreach (string key in (IEnumerable<string>) this.GetTables(tableName, (string) null).Keys)
      {
        IVistaDBTableSchema vistaDbTableSchema = this.localSqlConnection.TableSchema(key);
        int index1 = 0;
        for (int columnCount = vistaDbTableSchema.ColumnCount; index1 < columnCount; ++index1)
        {
          IVistaDBColumnAttributes columnAttributes = vistaDbTableSchema[index1];
          if (columnName == null || this.CompareString(columnName, columnAttributes.Name, true) == 0)
          {
            IVistaDBDefaultValueCollection defaultValues = vistaDbTableSchema.DefaultValues;
            DataRow row = dataTable.NewRow();
            bool flag1 = defaultValues.ContainsKey(columnAttributes.Name);
            row["TABLE_NAME"] = key == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema.Name;
            row["COLUMN_NAME"] = (object) columnAttributes.Name;
            row["ORDINAL_POSITION"] = (object) index1;
            row["COLUMN_HASDEFAULT"] = (object) flag1;
            row["COLUMN_DEFAULT"] = flag1 ? (object) defaultValues[columnAttributes.Name].Expression : (object) DBNull.Value;
            row["IS_NULLABLE"] = (object) columnAttributes.AllowNull;
            row["DATA_TYPE"] = (object) columnAttributes.Type.ToString();
            row["CHARACTER_MAXIMUM_LENGTH"] = (object) columnAttributes.MaxLength;
            row["DESCRIPTION"] = (object) columnAttributes.Description;
            row["COLUMN_CAPTION"] = (object) columnAttributes.Caption;
            row["COLUMN_ENCRYPTED"] = (object) columnAttributes.Encrypted;
            row["COLUMN_PACKED"] = (object) columnAttributes.Packed;
            row["CHARACTER_SET_NAME"] = columnAttributes.CodePage == 0 ? (object) (string) null : (object) columnAttributes.CodePage.ToString();
            bool flag2 = false;
            foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) vistaDbTableSchema.Indexes.Values)
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
            row["PRIMARY_KEY"] = (object) flag2;
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
      dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_NAME", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("TYPE_DESC", typeof (string));
      dataTable.Columns.Add("INDEX_NAME", typeof (string));
      dataTable.Columns.Add("PRIMARY_KEY", typeof (bool));
      dataTable.Columns.Add("UNIQUE", typeof (bool));
      dataTable.Columns.Add("FOREIGN_KEY_INDEX", typeof (bool));
      dataTable.Columns.Add("EXPRESSION", typeof (string));
      dataTable.Columns.Add("FULLTEXTSEARCH", typeof (bool));
      dataTable.DefaultView.Sort = "TABLE_NAME, INDEX_NAME";
      dataTable.BeginLoadData();
      foreach (string key in (IEnumerable<string>) this.GetTables(tableName, (string) null).Keys)
      {
        IVistaDBTableSchema vistaDbTableSchema = this.localSqlConnection.TableSchema(key);
        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) vistaDbTableSchema.Indexes.Values)
        {
          if (indexName == null || this.CompareString(indexName, indexInformation.Name, true) == 0)
          {
            DataRow row = dataTable.NewRow();
            row["TABLE_NAME"] = key == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema.Name;
            row["INDEX_NAME"] = (object) indexInformation.Name;
            row["PRIMARY_KEY"] = (object) indexInformation.Primary;
            row["UNIQUE"] = (object) indexInformation.Unique;
            row["FOREIGN_KEY_INDEX"] = (object) indexInformation.FKConstraint;
            row["EXPRESSION"] = (object) indexInformation.KeyExpression;
            row["FullTextSearch"] = (object) indexInformation.FullTextSearch;
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
      dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_NAME", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("COLUMN_NAME", typeof (string));
      dataTable.Columns.Add("ORDINAL_POSITION", typeof (int));
      dataTable.Columns.Add("KEYTYPE", typeof (ushort));
      dataTable.Columns.Add("INDEX_NAME", typeof (string));
      dataTable.DefaultView.Sort = "TABLE_NAME, INDEX_NAME, ORDINAL_POSITION";
      dataTable.BeginLoadData();
      foreach (string key in (IEnumerable<string>) this.GetTables(tableName, (string) null).Keys)
      {
        IVistaDBTableSchema vistaDbTableSchema = this.localSqlConnection.TableSchema(key);
        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) vistaDbTableSchema.Indexes.Values)
        {
          if (indexName == null || this.CompareString(indexName, indexInformation.Name, true) == 0)
          {
            IVistaDBKeyColumn[] keyStructure = indexInformation.KeyStructure;
            int index = 0;
            for (int length = keyStructure.Length; index < length; ++index)
            {
              IVistaDBColumnAttributes columnAttributes = vistaDbTableSchema[keyStructure[index].RowIndex];
              if (columnName == null || this.CompareString(columnName, columnAttributes.Name, true) == 0)
              {
                DataRow row = dataTable.NewRow();
                row["CONSTRAINT_NAME"] = (object) indexInformation.Name;
                row["TABLE_NAME"] = key == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema.Name;
                row["COLUMN_NAME"] = (object) columnAttributes.Name;
                row["ORDINAL_POSITION"] = (object) index;
                row["INDEX_NAME"] = (object) indexInformation.Name;
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
      dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_NAME", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_TYPE", typeof (string));
      dataTable.Columns.Add("IS_DEFERRABLE", typeof (string));
      dataTable.Columns.Add("INITIALLY_DEFERRED", typeof (string));
      dataTable.Columns.Add("FKEY_TO_TABLE", typeof (string));
      dataTable.Columns.Add("FKEY_TO_CATALOG", typeof (string));
      dataTable.Columns.Add("FKEY_TO_SCHEMA", typeof (string));
      dataTable.DefaultView.Sort = "TABLE_NAME, CONSTRAINT_NAME";
      dataTable.BeginLoadData();
      List<string> stringList;
      if (tableName == null)
      {
        stringList = new List<string>((IEnumerable<string>) this.localSqlConnection.GetTables());
      }
      else
      {
        stringList = new List<string>();
        stringList.Add(tableName);
      }
      foreach (string tableName1 in stringList)
      {
        IVistaDBTableSchema vistaDbTableSchema = this.localSqlConnection.TableSchema(tableName1);
        if (keyName == null)
        {
          foreach (IVistaDBRelationshipInformation foreignKey in (IEnumerable<IVistaDBRelationshipInformation>) vistaDbTableSchema.ForeignKeys)
          {
            DataRow row = dataTable.NewRow();
            row["CONSTRAINT_NAME"] = (object) foreignKey.Name;
            row["TABLE_NAME"] = tableName1 == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema.Name;
            row["CONSTRAINT_TYPE"] = (object) "FOREIGN KEY";
            row["IS_DEFERRABLE"] = (object) "NO";
            row["INITIALLY_DEFERRED"] = (object) "NO";
            row["FKEY_TO_TABLE"] = foreignKey.PrimaryTable == null ? (object) VistaDBConnection.SystemSchema : (object) foreignKey.PrimaryTable;
            dataTable.Rows.Add(row);
          }
        }
        else
        {
          IVistaDBRelationshipInformation relationshipInformation;
          if (vistaDbTableSchema.ForeignKeys.TryGetValue(keyName, out relationshipInformation))
          {
            DataRow row = dataTable.NewRow();
            row["CONSTRAINT_NAME"] = (object) relationshipInformation.Name;
            row["TABLE_NAME"] = tableName1 == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema.Name;
            row["CONSTRAINT_TYPE"] = (object) "FOREIGN KEY";
            row["IS_DEFERRABLE"] = (object) "NO";
            row["INITIALLY_DEFERRED"] = (object) "NO";
            row["FKEY_TO_TABLE"] = relationshipInformation.PrimaryTable == null ? (object) VistaDBConnection.SystemSchema : (object) relationshipInformation.PrimaryTable;
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
      dataTable.Columns.Add("CONSTRAINT_CATALOG", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_SCHEMA", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_NAME", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("CONSTRAINT_TYPE", typeof (string));
      dataTable.Columns.Add("IS_DEFERRABLE", typeof (bool));
      dataTable.Columns.Add("INITIALLY_DEFERRED", typeof (bool));
      dataTable.Columns.Add("FKEY_FROM_COLUMN", typeof (string));
      dataTable.Columns.Add("FKEY_FROM_ORDINAL_POSITION", typeof (int));
      dataTable.Columns.Add("FKEY_TO_CATALOG", typeof (string));
      dataTable.Columns.Add("FKEY_TO_SCHEMA", typeof (string));
      dataTable.Columns.Add("FKEY_TO_TABLE", typeof (string));
      dataTable.Columns.Add("FKEY_TO_COLUMN", typeof (string));
      dataTable.DefaultView.Sort = "TABLE_NAME, CONSTRAINT_NAME, FKEY_FROM_ORDINAL_POSITION";
      dataTable.BeginLoadData();
      foreach (string key in (IEnumerable<string>) this.GetTables(tableName, (string) null).Keys)
      {
        IVistaDBTableSchema vistaDbTableSchema1 = this.localSqlConnection.TableSchema(key);
        foreach (IVistaDBRelationshipInformation relationshipInformation in (IEnumerable<IVistaDBRelationshipInformation>) vistaDbTableSchema1.ForeignKeys.Values)
        {
          if (keyName == null || this.CompareString(keyName, relationshipInformation.Name, false) == 0)
          {
            IVistaDBKeyColumn[] keyStructure = vistaDbTableSchema1.Indexes[relationshipInformation.Name].KeyStructure;
            IVistaDBKeyColumn[] vistaDbKeyColumnArray = (IVistaDBKeyColumn[]) null;
            IVistaDBTableSchema vistaDbTableSchema2 = this.localSqlConnection.TableSchema(relationshipInformation.PrimaryTable);
            foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) vistaDbTableSchema2.Indexes.Values)
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
              if (columnName == null || this.CompareString(columnName, name, true) == 0)
              {
                DataRow row = dataTable.NewRow();
                row["CONSTRAINT_NAME"] = (object) relationshipInformation.Name;
                row["TABLE_NAME"] = key == null ? (object) VistaDBConnection.SystemSchema : (object) vistaDbTableSchema1.Name;
                row["CONSTRAINT_TYPE"] = (object) "FOREIGN KEY";
                row["IS_DEFERRABLE"] = (object) false;
                row["INITIALLY_DEFERRED"] = (object) false;
                row["FKEY_FROM_COLUMN"] = (object) name;
                row["FKEY_FROM_ORDINAL_POSITION"] = (object) index;
                row["FKEY_TO_TABLE"] = relationshipInformation.PrimaryTable == null ? (object) VistaDBConnection.SystemSchema : (object) relationshipInformation.PrimaryTable;
                row["FKEY_TO_COLUMN"] = (object) vistaDbTableSchema2[vistaDbKeyColumnArray[index].RowIndex].Name;
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
      dataTable.Columns.Add("ReservedWord", typeof (string));
      dataTable.BeginLoadData();
      using (StringReader stringReader = new StringReader(SQLResource.ReservedWords_VDB4))
      {
        for (string str = stringReader.ReadLine(); str != null; str = stringReader.ReadLine())
        {
          DataRow row = dataTable.NewRow();
          row[0] = (object) str;
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
      dataTable.Columns.Add("SPECIFIC_CATALOG", typeof (string));
      dataTable.Columns.Add("SPECIFIC_SCHEMA", typeof (string));
      dataTable.Columns.Add("SPECIFIC_NAME", typeof (string));
      dataTable.Columns.Add("ORDINAL_POSITION", typeof (string));
      dataTable.Columns.Add("PARAMETER_MODE", typeof (string));
      dataTable.Columns.Add("IS_RESULT", typeof (string));
      dataTable.Columns.Add("AS_LOCATOR", typeof (string));
      dataTable.Columns.Add("PARAMETER_NAME", typeof (string));
      dataTable.Columns.Add("DATA_TYPE", typeof (string));
      dataTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof (int));
      dataTable.Columns.Add("CHARACTER_OCTET_LENGTH", typeof (int));
      dataTable.Columns.Add("COLLATION_CATALOG", typeof (string));
      dataTable.Columns.Add("COLLATION_SCHEMA", typeof (string));
      dataTable.Columns.Add("COLLATION_NAME", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_CATALOG", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_SCHEMA", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_NAME", typeof (string));
      dataTable.Columns.Add("NUMERIC_PRECISION", typeof (byte));
      dataTable.Columns.Add("NUMERIC_PRECISION_RADIX", typeof (short));
      dataTable.Columns.Add("NUMERIC_SCALE", typeof (int));
      dataTable.Columns.Add("DATETIME_PRECISION", typeof (short));
      dataTable.Columns.Add("INTERVAL_TYPE", typeof (string));
      dataTable.Columns.Add("INTERVAL_PRECISION", typeof (short));
      dataTable.Columns.Add("PROCEDURE_NAME", typeof (string));
      dataTable.Columns.Add("PARAMETER_DATA_TYPE", typeof (string));
      dataTable.Columns.Add("PARAMETER_SIZE", typeof (int));
      dataTable.Columns.Add("PARAMETER_DIRECTION", typeof (int));
      dataTable.Columns.Add("IS_NULLABLE", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.DefaultView.Sort = "PARAMETER_NAME";
      dataTable.BeginLoadData();
      if (!string.IsNullOrEmpty(storedProcedure))
        commandText = "SELECT * FROM (" + commandText + ") WHERE UPPER(PROC_NAME) = '" + storedProcedure.ToUpperInvariant() + "'";
      IVistaDBTableSchema tableSchema = this.GetTableSchema((string) null);
      using (VistaDBCommand vistaDbCommand = new VistaDBCommand(commandText, this))
      {
        using (VistaDBDataReader vistaDbDataReader = vistaDbCommand.ExecuteReader())
        {
          while (vistaDbDataReader.Read())
          {
            string name = vistaDbDataReader["PARAM_NAME"] as string;
            VistaDBType type = (VistaDBType) vistaDbDataReader["PARAM_TYPE"];
            IVistaDBColumnAttributes columnAttributes = tableSchema.AddColumn(name, type);
            DataRow row = dataTable.NewRow();
            row["PARAMETER_NAME"] = (object) name;
            row["DATA_TYPE"] = (object) type;
            row["ORDINAL_POSITION"] = vistaDbDataReader["PARAM_ORDER"];
            row["PARAMETER_DIRECTION"] = vistaDbDataReader["IS_PARAM_OUT"];
            row["PROCEDURE_NAME"] = vistaDbDataReader["PROC_NAME"];
            row["SPECIFIC_NAME"] = vistaDbDataReader["PROC_NAME"];
            row["PARAMETER_DATA_TYPE"] = (object) columnAttributes.Type.ToString();
            row["IS_NULLABLE"] = (object) "YES";
            row["PARAMETER_SIZE"] = (object) 0;
            row["NUMERIC_PRECISION"] = (object) 0;
            row["NUMERIC_SCALE"] = (object) 0;
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
      dataTable.Columns.Add("SPECIFIC_CATALOG", typeof (string));
      dataTable.Columns.Add("SPECIFIC_SCHEMA", typeof (string));
      dataTable.Columns.Add("SPECIFIC_NAME", typeof (string));
      dataTable.Columns.Add("ROUTINE_CATALOG", typeof (string));
      dataTable.Columns.Add("ROUTINE_SCHEMA", typeof (string));
      dataTable.Columns.Add("ROUTINE_NAME", typeof (string));
      dataTable.Columns.Add("ROUTINE_TYPE", typeof (string));
      dataTable.Columns.Add("CREATED", typeof (DateTime));
      dataTable.Columns.Add("LAST_ALTERED", typeof (DateTime));
      dataTable.Columns.Add("ROUTINE_DESCRIPTION", typeof (string));
      dataTable.Columns.Add("ROUTINE_DEFINITION", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
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
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("CHECK_OPTION", typeof (string));
      dataTable.Columns.Add("IS_UPDATABLE", typeof (bool));
      dataTable.Columns.Add("TABLE_DESCRIPTION", typeof (string));
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
      dataTable.Columns.Add("VIEW_CATALOG", typeof (string));
      dataTable.Columns.Add("VIEW_SCHEMA", typeof (string));
      dataTable.Columns.Add("VIEW_NAME", typeof (string));
      dataTable.Columns.Add("TABLE_CATALOG", typeof (string));
      dataTable.Columns.Add("TABLE_SCHEMA", typeof (string));
      dataTable.Columns.Add("TABLE_NAME", typeof (string));
      dataTable.Columns.Add("COLUMN_NAME", typeof (string));
      dataTable.Columns.Add("COLUMN_GUID", typeof (Guid));
      dataTable.Columns.Add("COLUMN_PROPID", typeof (long));
      dataTable.Columns.Add("ORDINAL_POSITION", typeof (int));
      dataTable.Columns.Add("COLUMN_HASDEFAULT", typeof (bool));
      dataTable.Columns.Add("COLUMN_DEFAULT", typeof (string));
      dataTable.Columns.Add("IS_NULLABLE", typeof (bool));
      dataTable.Columns.Add("DATA_TYPE", typeof (string));
      dataTable.Columns.Add("TYPE_GUID", typeof (Guid));
      dataTable.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof (int));
      dataTable.Columns.Add("CHARACTER_OCTET_LENGTH", typeof (int));
      dataTable.Columns.Add("NUMERIC_PRECISION", typeof (int));
      dataTable.Columns.Add("NUMERIC_SCALE", typeof (int));
      dataTable.Columns.Add("DATETIME_PRECISION", typeof (long));
      dataTable.Columns.Add("CHARACTER_SET_CATALOG", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_SCHEMA", typeof (string));
      dataTable.Columns.Add("CHARACTER_SET_NAME", typeof (string));
      dataTable.Columns.Add("COLLATION_CATALOG", typeof (string));
      dataTable.Columns.Add("COLLATION_SCHEMA", typeof (string));
      dataTable.Columns.Add("COLLATION_NAME", typeof (string));
      dataTable.Columns.Add("DOMAIN_CATALOG", typeof (string));
      dataTable.Columns.Add("DOMAIN_NAME", typeof (string));
      dataTable.Columns.Add("DESCRIPTION", typeof (string));
      dataTable.Columns.Add("PRIMARY_KEY", typeof (bool));
      dataTable.Columns.Add("COLUMN_CAPTION", typeof (string));
      dataTable.Columns.Add("COLUMN_ENCRYPTED", typeof (bool));
      dataTable.Columns.Add("COLUMN_PACKED", typeof (bool));
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
            row["COLUMN_HASDEFAULT"] = (object) (vistaDbDataReader["DEFAULT_VALUE"] != DBNull.Value);
            row["COLUMN_DEFAULT"] = vistaDbDataReader["DEFAULT_VALUE"];
            row["IS_NULLABLE"] = vistaDbDataReader["ALLOW_NULL"];
            row["DATA_TYPE"] = vistaDbDataReader["DATA_TYPE_NAME"];
            row["CHARACTER_MAXIMUM_LENGTH"] = vistaDbDataReader["COLUMN_SIZE"];
            row["CHARACTER_SET_NAME"] = (int) vistaDbDataReader["CODE_PAGE"] == 0 ? (object) DBNull.Value : (object) vistaDbDataReader["CODE_PAGE"].ToString();
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
        return (object) this;
      }
    }

    internal IQueryStatement CreateQuery(string commandText)
    {
      this.InstantiateLocalSqlConnection((IDatabase) null);
      return this.localSqlConnection.CreateQuery(commandText);
    }

    internal void FreeQuery(IQueryStatement query, bool cleanup)
    {
      if (this.localSqlConnection == null)
        return;
      this.localSqlConnection.FreeQuery(query);
      if (!cleanup || this.localSqlConnection.CurrentTransaction != null || (this.OpenMode == VistaDBDatabaseOpenMode.ExclusiveReadWrite || this.OpenMode == VistaDBDatabaseOpenMode.ExclusiveReadOnly))
        return;
      this.localSqlConnection.CloseAllPooledTables();
    }

    internal void RetrieveConnectionInfo()
    {
      if (this.CompareString(this.DataSource, this.localSqlConnection.FileName, true) == 0)
        return;
      this.connectionString.DataSource = this.localSqlConnection.FileName;
      this.connectionString.OpenMode = this.localSqlConnection.OpenMode;
      this.connectionString.Password = this.localSqlConnection.Password;
    }

    internal void InternalBeginTransaction(VistaDBTransaction parentTransaction)
    {
      if (this.TransactionMode == VistaDBTransaction.TransactionMode.Off)
        throw new VistaDBException(460);
      lock (this.SyncRoot)
      {
        this.MustBeOpened();
        if (this.TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
          return;
        this.localSqlConnection.BeginTransaction(parentTransaction);
      }
    }

    internal void InternalCommitTransaction()
    {
      if (this.TransactionMode == VistaDBTransaction.TransactionMode.Off)
        throw new VistaDBException(460);
      if (this.TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
        return;
      lock (this.SyncRoot)
      {
        this.MustBeOpened();
        this.localSqlConnection.CommitTransaction();
      }
    }

    internal void InternalRollbackTransaction()
    {
      if (this.TransactionMode == VistaDBTransaction.TransactionMode.Off)
        throw new VistaDBException(460);
      if (this.TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
        return;
      lock (this.SyncRoot)
      {
        this.MustBeOpened();
        this.localSqlConnection.RollbackTransaction();
      }
    }

    internal VistaDBTransaction Transaction
    {
      get
      {
        lock (this.SyncRoot)
          return this.localSqlConnection == null ? (VistaDBTransaction) null : this.localSqlConnection.CurrentTransaction;
      }
    }

    object ICloneable.Clone()
    {
      VistaDBConnection vistaDbConnection = new VistaDBConnection(this.ConnectionString);
      lock (this.SyncRoot)
      {
        if (this.State == ConnectionState.Open)
          vistaDbConnection.Open();
      }
      return (object) vistaDbConnection;
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
        this.count = 0;
        this.connections = new ILocalSQLConnection[maxPoolSize];
        this.Push(conn);
      }

      internal ILocalSQLConnection Pop()
      {
        lock (this.connections)
        {
          if (this.count > 0)
          {
            ILocalSQLConnection connection = this.connections[this.count - 1];
            this.connections[this.count--] = this.count <= this.minPoolSize ? (ILocalSQLConnection) null : (ILocalSQLConnection) null;
            return connection;
          }
        }
        return (ILocalSQLConnection) null;
      }

      internal void Push(ILocalSQLConnection connection)
      {
        if (connection == null)
          return;
        lock (this.connections)
        {
          if (this.count >= this.maxPoolSize)
            connection.Dispose();
          else
            this.connections[this.count++] = connection;
        }
      }

      public void Clear()
      {
        lock (this.connections)
        {
          int index = 0;
          for (int count = this.count; index < count; ++index)
          {
            this.connections[index].Dispose();
            this.connections[index] = (ILocalSQLConnection) null;
          }
        }
      }
    }

    private class ConnectionPoolCollection
    {
      private Dictionary<string, VistaDBConnection.ConnectionPool> pools;

      public ConnectionPoolCollection()
      {
        this.pools = new Dictionary<string, VistaDBConnection.ConnectionPool>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
      }

      public void Clear()
      {
        lock (this.pools)
        {
          foreach (VistaDBConnection.ConnectionPool connectionPool in this.pools.Values)
            connectionPool.Clear();
          this.pools.Clear();
        }
      }

      internal void ClearPool(VistaDBConnection connection)
      {
        lock (this.pools)
        {
          string connectionString = connection.GetPoolConnectionString();
          VistaDBConnection.ConnectionPool connectionPool;
          if (!this.pools.TryGetValue(connectionString, out connectionPool))
            return;
          connectionPool.Clear();
          this.pools.Remove(connectionString);
        }
      }

      internal bool PutConnectionOnHold(ILocalSQLConnection connection, string connectionString, int minPoolSize, int maxPoolSize)
      {
        lock (this.pools)
        {
          if (connection is IPooledSQLConnection)
            ((IPooledSQLConnection) connection).PrepareConnectionForPool();
          connection.CloseAllPooledTables();
          VistaDBConnection.ConnectionPool connectionPool;
          if (this.pools.TryGetValue(connectionString, out connectionPool))
          {
            connectionPool.Push(connection);
            return false;
          }
          if (minPoolSize <= 0 || connectionString == null)
            return connection != null;
          this.pools.Add(connectionString, new VistaDBConnection.ConnectionPool(minPoolSize, maxPoolSize, connection));
        }
        return false;
      }

      internal ILocalSQLConnection GetConnection(string connectionString)
      {
        if (connectionString == null)
          return (ILocalSQLConnection) null;
        if (this.pools.Count == 0)
          return (ILocalSQLConnection) null;
        VistaDBConnection.ConnectionPool connectionPool;
        if (this.pools.TryGetValue(connectionString, out connectionPool))
          return connectionPool.Pop();
        return (ILocalSQLConnection) null;
      }
    }
  }
}
