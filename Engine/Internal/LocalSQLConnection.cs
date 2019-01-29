using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.SQL;
using VistaDB.Engine.SQL.Signatures;
using VistaDB.Provider;

namespace VistaDB.Engine.Internal
{
	internal class LocalSQLConnection : Connection, ILocalSQLConnection, IPooledSQLConnection, IVistaDBConnection, IDisposable
	{
		private readonly ActiveTriggers activeTriggers = new ActiveTriggers();
		private List<long> disposedQueries = new List<long>();
		private Dictionary<long, IQueryStatement> queries = new Dictionary<long, IQueryStatement>();
		private static Dictionary<string, IStatementDescr> Statements = new Dictionary<string, IStatementDescr>();
		private IVistaDBDDA direct;
		private long counter;
		private bool caseSensitive;
		private CultureInfo cultureInfo;
		private SQLParser parser;
		private bool optimization;
		private bool groupOptimization;
		private bool groupSynchronization;
		private bool checkView;
		private TablePool poolOfTables;
		private VistaDBTransaction parentTransaction;
		private VistaDBConnection parentConnection;

		static LocalSQLConnection()
		{
			Statements.Add("SELECT", new SelectStatementDescr());
			Statements.Add("INSERT", new InsertStatementDescr());
			Statements.Add("UPDATE", new UpdateStatementDescr());
			Statements.Add("DELETE", new DeleteStatementDescr());
			Statements.Add("CREATE", new BaseCreateStatementDescr());
			Statements.Add("DROP", new BaseDropStatementDescr());
			Statements.Add("TRUNCATE", new TruncateDescr());
			Statements.Add("ALTER", new BaseAlterStatementDescr());
			Statements.Add("SET", new BaseSetStatementDescr());
			IStatementDescr statementDescr = new ExecStatementDescr();
			Statements.Add("EXEC", statementDescr);
			Statements.Add("EXECUTE", statementDescr);
			Statements.Add("DECLARE", new DeclareStatementDescr());
			Statements.Add("IF", new IFStatementDescr());
			Statements.Add("BEGIN", new BaseBeginStatementDescr());
			Statements.Add("COMMIT", new CommitTransactionStatementDescr());
			Statements.Add("ROLLBACK", new RollbackTransactionStatementDescr());
			Statements.Add("RETURN", new ReturnStatementDescr());
			Statements.Add("WHILE", new WhileStatementDescr());
			Statements.Add("CONTINUE", new ContinueStatementDescr());
			Statements.Add("BREAK", new BreakStatementDescr());
			Statements.Add("RAISERROR", new RaiseErrorStatmentDescr());
			Statements.Add("PRINT", new PrintStatmentDesc());
		}

		internal static LocalSQLConnection CreateInstance(VistaDBEngine engine, long id, VistaDBConnection parentConnection, IDatabase database)
		{
			return new LocalSQLConnection(engine, id, parentConnection, database);
		}

		private LocalSQLConnection(VistaDBEngine engine, long id, VistaDBConnection parentConnection, IDatabase database)
		  : base(engine, id)
		{
			this.parentConnection = parentConnection;
			direct = null;
			counter = 0L;
			Database = database;
			FileName = null;
			OpenMode = VistaDBDatabaseOpenMode.ExclusiveReadOnly;
			Password = null;
			parser = null;
			optimization = true;
			groupOptimization = true;
			checkView = true;
			poolOfTables = new TablePool();
			CachedAffectedRows = 1L;
			if (Database == null)
			{
				cultureInfo = CultureInfo.InvariantCulture;
				caseSensitive = false;
			}
			else
			{
				cultureInfo = database.Culture;
				caseSensitive = database.CaseSensitive;
				OpenMode = database.Mode;
			}
		}

		public IQueryStatement this[long queryId]
		{
			get
			{
				if (!queries.TryGetValue(queryId, out IQueryStatement queryStatement))
					return null;
				return queryStatement;
			}
		}

		VistaDBTransaction ILocalSQLConnection.CurrentTransaction
		{
			get
			{
				return parentTransaction;
			}
		}

		VistaDBConnection ILocalSQLConnection.ParentConnection
		{
			get
			{
				return parentConnection;
			}
		}

		IQueryStatement ILocalSQLConnection.CreateMessageQuery(string message)
		{
			SelectMessageStringStatement messageStringStatement = new SelectMessageStringStatement(this, parser, GenerateUniqueId(), message);
			if (messageStringStatement == null)
				return null;
			queries.Add(messageStringStatement.Id, messageStringStatement);
			return messageStringStatement;
		}

		IQueryStatement ILocalSQLConnection.CreateResultQuery(TemporaryResultSet result)
		{
			SelectTableStatement selectTableStatement = new SelectTableStatement(this, parser, GenerateUniqueId(), result, result);
			if (selectTableStatement == null)
				return null;
			queries.Add(selectTableStatement.Id, selectTableStatement);
			return selectTableStatement;
		}

		IQueryStatement ILocalSQLConnection.CreateQuery(string commandText)
		{
			BatchStatement batchStatement = CreateBatchStatement(commandText, GenerateUniqueId());
			if (batchStatement == null)
				return null;
			queries.Add(batchStatement.Id, batchStatement);
			return batchStatement;
		}

		void ILocalSQLConnection.FreeQuery(IQueryStatement query)
		{
			IQueryStatement queryStatement;
			if (query == null || !queries.TryGetValue(query.Id, out queryStatement) || queryStatement != query)
				return;
			if (query.LockedDisposing)
			{
				disposedQueries.Add(query.Id);
			}
			else
			{
				queries.Remove(query.Id);
				query.Dispose();
			}
		}

		public void OpenDatabase(string fileName, VistaDBDatabaseOpenMode mode, string cryptoKeyString, bool fromIsolatedStorage)
		{
			if (Database != null)
				return;
			if (direct == null)
				direct = ParentEngine.OpenDDA();
			Database = !fromIsolatedStorage ? (IDatabase)direct.OpenDatabase(fileName, mode, cryptoKeyString) : (IDatabase)direct.OpenIsolatedDatabase(fileName, mode, cryptoKeyString);
			FileName = fileName;
			OpenMode = mode;
			Password = cryptoKeyString ?? string.Empty;
			cultureInfo = Database.Culture;
			caseSensitive = Database.CaseSensitive;
		}

		internal void OpenInMemoryDatabase(string cryptoKeyString, int lcid, bool caseSensitive)
		{
			if (Database != null)
				return;
			direct = ParentEngine.OpenDDA();
			Database = (IDatabase)direct.CreateInMemoryDatabase(cryptoKeyString, lcid, caseSensitive);
			FileName = null;
			OpenMode = VistaDBDatabaseOpenMode.ExclusiveReadWrite;
			Password = cryptoKeyString ?? string.Empty;
			cultureInfo = Database.Culture;
			this.caseSensitive = caseSensitive;
		}

		internal void CloseExternalDatabase()
		{
			Database.Close();
			Database = null;
			cultureInfo = CultureInfo.InvariantCulture;
			caseSensitive = false;
		}

		public void CloseDatabase()
		{
			if (Database == null)
				return;
			try
			{
				foreach (IDisposable disposable in queries.Values)
					disposable.Dispose();
				if (!IsDatabaseOwner)
					return;
				Database.Close();
				direct.Dispose();
			}
			finally
			{
				direct = null;
				Database = null;
				cultureInfo = CultureInfo.InvariantCulture;
				caseSensitive = false;
				queries.Clear();
			}
		}

		internal void BeginTransaction()
		{
			switch (parentConnection.TransactionMode)
			{
				case VistaDBTransaction.TransactionMode.Off:
					throw new VistaDBException(460);
				case VistaDBTransaction.TransactionMode.Ignore:
					break;
				default:
					parentTransaction = parentConnection.BeginTransaction();
					break;
			}
		}

		public void BeginTransaction(VistaDBTransaction parentTransaction)
		{
			switch (parentConnection.TransactionMode)
			{
				case VistaDBTransaction.TransactionMode.Off:
					throw new VistaDBException(460);
				case VistaDBTransaction.TransactionMode.Ignore:
					break;
				default:
					Database.BeginTransaction();
					this.parentTransaction = parentTransaction;
					break;
			}
		}

		public void CommitTransaction()
		{
			if (parentTransaction == null)
				return;
			switch (parentConnection.TransactionMode)
			{
				case VistaDBTransaction.TransactionMode.Off:
					throw new VistaDBException(460);
				case VistaDBTransaction.TransactionMode.Ignore:
					break;
				default:
					Database.CommitTransaction();
					parentTransaction = null;
					break;
			}
		}

		public void RollbackTransaction()
		{
			if (parentTransaction == null)
				return;
			switch (parentConnection.TransactionMode)
			{
				case VistaDBTransaction.TransactionMode.Off:
					throw new VistaDBException(460);
				case VistaDBTransaction.TransactionMode.Ignore:
					break;
				default:
					Database.RollbackTransaction();
					parentTransaction = null;
					break;
			}
		}

		public bool IsSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
		{
			CreateParser();
			parser.SetText(text);
			try
			{
				ParseStatementBatch(0L);
			}
			catch (VistaDBSQLException ex)
			{
				lineNo = ex.LineNo;
				symbolNo = ex.ColumnNo;
				errorMessage = ex.Message;
				return false;
			}
			lineNo = 0;
			symbolNo = 0;
			errorMessage = null;
			return true;
		}

		public bool IsViewSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
		{
			CreateParser();
			parser.SetText(text);
			try
			{
				if (!(ParseStatementBatch(0L).SubQuery(0) is CreateViewStatement))
				{
					lineNo = 1;
					symbolNo = 1;
					errorMessage = "Expected CREATE VIEW statement.";
					return false;
				}
			}
			catch (VistaDBSQLException ex)
			{
				lineNo = ex.LineNo;
				symbolNo = ex.ColumnNo;
				errorMessage = ex.Message;
				return false;
			}
			lineNo = 0;
			symbolNo = 0;
			errorMessage = null;
			return true;
		}

		public bool IsConstraintSyntaxCorrect(string text, out int lineNo, out int symbolNo, out string errorMessage)
		{
			CreateParser();
			parser.SetText(text);
			BatchStatement batchStatement = new BatchStatement(this, (Statement)null, parser, 0L);
			try
			{
				parser.NextSignature(true, true, 6);
				if (!parser.EndOfText)
				{
					lineNo = parser.TokenValue.RowNo;
					symbolNo = parser.TokenValue.ColNo;
					errorMessage = "Expected end of constraint.";
					return false;
				}
			}
			catch (VistaDBSQLException ex)
			{
				lineNo = ex.LineNo;
				symbolNo = ex.ColumnNo;
				errorMessage = ex.Message;
				return false;
			}
			finally
			{
				batchStatement.Dispose();
			}
			lineNo = 0;
			symbolNo = 0;
			errorMessage = null;
			return true;
		}

		public bool TryToCorrect(string oldText, out string newText, out int lineNo, out int symbolNo, out string errorMessage)
		{
			CreateParser();
			newText = null;
			lineNo = 0;
			symbolNo = 0;
			errorMessage = null;
			return false;
		}

		public bool DatabaseOpened
		{
			get
			{
				return Database != null;
			}
		}

		public string FileName { get; private set; }

		public VistaDBDatabaseOpenMode OpenMode { get; private set; }

		internal bool RemoveQuery(long queryId)
		{
			return queries.Remove(queryId);
		}

		internal bool QueryIsDisposed(long queryId)
		{
			if (!disposedQueries.Contains(queryId))
				return false;
			disposedQueries.Remove(queryId);
			return true;
		}

		public VistaDBException LastException { get; set; }

		public string Password { get; private set; }

		public IVistaDBTableSchema TableSchema(string tableName)
		{
			if (Database != null)
				return Database.TableSchema(tableName);
			return null;
		}

		public IVistaDBTableNameCollection GetTables()
		{
			return Database?.GetTableNames();
		}

		public IVistaDBRelationshipCollection Relationships
		{
			get
			{
				return Database?.Relationships;
			}
		}

		internal long CachedAffectedRows { get; set; }

		public bool IsDatabaseOwner
		{
			get
			{
				return direct != null;
			}
		}

		public bool IsolatedStorage
		{
			get
			{
				if (Database != null)
					return Database.IsolatedStorage;
				return false;
			}
		}

		public void RegisterTrigger(string tableName, TriggerAction type)
		{
			activeTriggers.Register(tableName, type);
		}

		public void UnregisterTrigger(string tableName, TriggerAction type)
		{
			activeTriggers.Unregister(tableName, type);
		}

		public bool IsTriggerActing(string tableName, TriggerAction type)
		{
			return activeTriggers.IsRegistered(tableName, type);
		}

		private long GenerateUniqueId()
		{
			return counter++;
		}

		private void CreateParser()
		{
			if (parser == null)
				parser = SQLParser.CreateInstance(string.Empty, cultureInfo);
			else
				parser.Culture = cultureInfo;
		}

		private void CreateParser(CurrentTokenContext context)
		{
			CreateParser();
			parser.PushContext(context);
		}

		public IDatabase Database { get; private set; }

		public int CompareString(string s1, string s2, bool ignoreCase)
		{
			return string.Compare(s1, s2, ignoreCase, cultureInfo);
		}

		public int CompareChar(char c1, char c2)
		{
			return string.Compare(c1.ToString(), c2.ToString(), !caseSensitive, cultureInfo);
		}

		public int CharIndexOf(string s, char c)
		{
			string strB = c.ToString();
			int index = 0;
			for (int length = s.Length; index < length; ++index)
			{
				if (string.Compare(s[index].ToString(), strB, !caseSensitive, cultureInfo) == 0)
					return index;
			}
			return -1;
		}

		public static int CharIndexOf(string s, char c, bool CaseSensitive, CultureInfo targetCulture)
		{
			string strB = c.ToString();
			int index = 0;
			for (int length = s.Length; index < length; ++index)
			{
				if (string.Compare(s[index].ToString(), strB, !CaseSensitive, targetCulture) == 0)
					return index;
			}
			return -1;
		}

		public string StringUpper(string s)
		{
			return s.ToUpper(cultureInfo);
		}

		public string StringLower(string s)
		{
			return s.ToLower(cultureInfo);
		}

		public void SetOptimization(bool val)
		{
			optimization = val;
		}

		public void SetGroupOptimization(bool val)
		{
			groupOptimization = val;
		}

		public void SetGroupSynchronization(bool val)
		{
			groupSynchronization = val;
		}

		public bool GetSynchronization()
		{
			return groupSynchronization;
		}

		public bool GetOptimization()
		{
			return optimization;
		}

		public bool GetGroupOptimization()
		{
			return groupOptimization;
		}

		public void SetCheckView(bool val)
		{
			checkView = val;
		}

		public bool GetCheckView()
		{
			return checkView;
		}

		internal void PrepareCLRContext(VistaDBPipe pipe)
		{
			VistaDBContext.SQLChannel.ActivateContext((ILocalSQLConnection)this, pipe);
			VistaDBContext.DDAChannel.ActivateContext((IVistaDBDatabase)this.Database, (IVistaDBPipe)null);
		}

		internal void UnprepareCLRContext()
		{
			VistaDBContext.SQLChannel.DeactivateContext();
			VistaDBContext.DDAChannel.DeactivateContext();
		}

		public ITable OpenTable(string name, bool exclusive, bool readOnly)
		{
			if (CompareString(name, VistaDB.Engine.Core.Database.SystemSchema, true) == 0)
				return (ITable)Database;
			return poolOfTables.OpenOrReuseTable(Database, name, exclusive, readOnly);
		}

		public void FreeTable(ITable table)
		{
			if (Database == table || table == null)
				return;
			poolOfTables.AddToPool(table);
		}

		public void RemoveFromPool(string tableName)
		{
			poolOfTables.RemoveFromPool(tableName);
		}

		public void CloseTable(ITable table)
		{
			if (Database == table)
				return;
			poolOfTables.CloseTable(table);
		}

		public void CloseAllPooledTables()
		{
			this.poolOfTables.CloseAllPooled();
		}

		public bool IsIndexExisting(string tableName, string indexName)
		{
			return poolOfTables.IsIndexExisting(tableName, indexName);
		}

		public void OnPrintMessage(string message)
		{
			parentConnection.OnPrintMessage(message);
		}

		internal BatchStatement CreateStoredProcedureStatement(Statement parent, string text, out List<SQLParser.VariableDeclaration> variables)
		{
			try
			{
				CreateParser(new CurrentTokenContext(CurrentTokenContext.TokenContext.StoredProcedure, string.Empty));
				parser.SetText(text);
				parser.Parent = parent;
				if (!parser.SkipToken(false))
				{
					variables = null;
					return null;
				}
				variables = parser.ParseVariables();
				parser.SkipToken(false);
				Statement statement = ParseStatement(parent, Id);
				if (statement is BatchStatement)
					return (BatchStatement)statement;
				throw new VistaDBException(632, text);
			}
			finally
			{
				parser.PopContext();
			}
		}

		internal StoredFunctionBody CreateStoredFunctionStatement(Statement parent, string text, out VistaDBType resultType, out List<SQLParser.VariableDeclaration> variables, out CreateTableStatement resultTableStatement)
		{
			CreateParser(new CurrentTokenContext(CurrentTokenContext.TokenContext.StoredFunction, string.Empty));
			try
			{
				resultTableStatement = null;
				string str = null;
				parser.SetText(text);
				parser.Parent = parent;
				parser.SkipToken(true);
				parser.SkipToken(true);
				variables = parser.ParseVariables();
				parser.ExpectedExpression(")");
				parser.SkipToken(true);
				parser.ExpectedExpression("RETURNS");
				parser.SkipToken(true);
				if (ParameterSignature.IsParameter(parser.TokenValue.Token))
				{
					str = parser.TokenValue.Token;
					parser.SkipToken(true);
					parser.ExpectedExpression("TABLE");
					parser.SkipToken(true);
					resultTableStatement = new CreateTableStatement(this, parent, parser, -1L);
					resultType = VistaDBType.Unknown;
				}
				else
				{
					resultType = parser.ReadDataType(out int len);
				}
				parser.ExpectedExpression("AS");
				parser.SkipToken(false);
				IParameter returnParameter = parent.DoGetReturnParameter();
				try
				{
					parent.DoSetReturnParameter(new BatchStatement.ParamInfo(str, VistaDBType.Unknown, ParameterDirection.ReturnValue));
					parent.DoGetReturnParameter().Value = resultTableStatement;
					StoredFunctionBody statement = ParseStatement(parent, Id) as StoredFunctionBody;
					if (resultTableStatement != null && resultTableStatement.TableName == null)
						resultTableStatement.CreateUniqueName("___$$$___returnTable");
					return statement;
				}
				finally
				{
					parent.DoSetReturnParameter(returnParameter);
				}
			}
			finally
			{
				parser.PopContext();
			}
		}

		internal CheckStatement CreateCheckConstraint(string text, string tableName, Row row)
		{
			CreateParser();
			parser.SetText(text);
			return new CheckStatement(this, null, parser, 0L, tableName, row);
		}

		internal BatchStatement CreateBatchStatement(string text, long id)
		{
			CreateParser();
			parser.SetText(text);
			return ParseStatementBatch(id);
		}

		private BatchStatement ParseStatementBatch(long id)
		{
			if (!parser.SkipToken(false))
				return null;			

			parser.PushContext(new CurrentTokenContext(CurrentTokenContext.TokenContext.UsualText, string.Empty));
			try
			{
				BatchStatement batchStatement = new BatchStatement(this, null, parser, id);
				for (bool flag = parser.EndOfText; !flag; flag = parser.SkipSemicolons())
					batchStatement.Add(ParseStatement(batchStatement, id));
				return batchStatement;
			}
			finally
			{
				parser.PopContext();
				parser.Parent = null;
			}
		}

		internal Statement ParseStatement(Statement parent, long id)
		{
			string upper = parser.TokenValue.Token.ToUpper(CultureInfo.InvariantCulture);
			int symbolNo = parser.TokenValue.SymbolNo;
			IStatementDescr statementDescr = null;
			if (Statements.ContainsKey(upper))
				statementDescr = Statements[upper];
			if (statementDescr == null)
			{
				if (parent.SubQueryCount != 0)
					throw new VistaDBSQLException(632, upper, parser.TokenValue.RowNo, symbolNo);
				statementDescr = new ExecStatementDescr();
			}
			Statement statement = statementDescr.CreateStatement(this, parent, parser, id);
			int length = (parser.TokenValue.SymbolNo == 0 ? parser.Text.Length : parser.TokenValue.SymbolNo) - symbolNo;
			statement.CommandText = parser.Text.Substring(symbolNo, length).TrimStart();
			return statement;
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				foreach (IDisposable disposable in queries.Values)
					disposable.Dispose();
				queries.Clear();
				poolOfTables.Dispose();
				activeTriggers.Clear();
				CloseDatabase();
			}
			base.Dispose(disposing);
			parentTransaction = null;
			parentConnection = null;
		}

		void IPooledSQLConnection.PrepareConnectionForPool()
		{
			parentConnection = null;
		}

		void IPooledSQLConnection.InitializeConnectionFromPool(DbConnection parentConnection)
		{
			this.parentConnection = (VistaDBConnection)parentConnection;
		}

		private class ActiveTriggers : InsensitiveHashtable
		{
			private string Key(string tableName, TriggerAction eventType)
			{
				return tableName + eventType.ToString();
			}

			internal void Register(string tableName, TriggerAction eventType)
			{
				Add(Key(tableName, eventType), null);
			}

			internal void Unregister(string tableName, TriggerAction eventType)
			{
				Remove(Key(tableName, eventType));
			}

			internal bool IsRegistered(string tableName, TriggerAction eventType)
			{
				return Contains(Key(tableName, eventType));
			}
		}

		private class TablePool : InsensitiveHashtable, IDisposable
		{
			private bool isDisposed;

			internal TablePool()
			{
			}

			private ITable this[string name]
			{
				get
				{
					return (ITable)this[(object)name];
				}
			}

			internal ITable OpenOrReuseTable(IDatabase activeDatabase, string name, bool exclusive, bool readOnly)
			{
				lock (this.SyncRoot)
				{
					ITable table = this[name];
					if (table == null)
						return (ITable)activeDatabase.OpenTable(name, exclusive, readOnly);
					Remove(name);
					if (exclusive && !table.IsExclusive || !readOnly && table.IsReadOnly)
					{
						table.Close();
						return (ITable)activeDatabase.OpenTable(name, exclusive, readOnly);
					}
					table.ResetFilter();
					table.ResetOptimizedFiltering();
					table.ResetScope();
					table.ActiveIndex = null;
					table.ClearCachedBitmaps();
					return table;
				}
			}

			internal void RemoveFromPool(string tableName)
			{
				if (string.IsNullOrEmpty(tableName))
					return;
				lock (SyncRoot)
					Remove(tableName);
			}

			internal void CloseTable(ITable table)
			{
				if (table == null || table.IsClosed)
					return;
				lock (SyncRoot)
				{
					string name = table.Name;
					if (this[name] == table)
						Remove(name);
					table.Close();
				}
			}

			internal void AddToPool(ITable table)
			{
				if (table == null || table.IsClosed)
					return;
				if (!table.AllowPooling)
				{
					table.Close();
				}
				else
				{
					string name = table.Name;
					lock (SyncRoot)
					{
						ITable table1 = this[name];
						if (table1 == null)
						{
							Add(name, table);
						}
						else
						{
							bool flag1 = table.IsExclusive || table1.IsExclusive;
							bool flag2 = table.IsReadOnly && table1.IsReadOnly;
							if (flag1 == table1.IsExclusive && flag2 == table1.IsReadOnly)
							{
								table.Close();
							}
							else
							{
								table1.Close();
								this[(object)name] = table;
							}
						}
					}
				}
			}

			internal void CloseAllPooled()
			{
				lock (SyncRoot)
				{
					foreach (IVistaDBTable vistaDbTable in Values)
						vistaDbTable.Close();
					Clear();
				}
			}

			internal bool IsIndexExisting(string tableName, string indexName)
			{
				lock (SyncRoot)
				{
					ITable table = this[tableName];
					return table != null && table.KeyStructure(indexName) != null;
				}
			}

			public void Dispose()
			{
				if (isDisposed)
					return;
				isDisposed = true;
				CloseAllPooled();
				GC.SuppressFinalize(this);
			}
		}
	}
}
