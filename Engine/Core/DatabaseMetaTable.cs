using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security;
using System.Xml;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
	internal class DatabaseMetaTable : Table, IDatabase, IVistaDBDatabase, IVistaDBTable, IDisposable
	{
		private IVistaDBDatabase temporaryDatabase;
		private bool isDisposed;

		internal static DatabaseMetaTable CreateInstance(string fileName, DirectConnection parentConnection, EncryptionKey cryptoKey, int pageSize, int LCID, bool caseSensitive, bool toPack)
		{
			if (pageSize <= 0)
				pageSize = parentConnection.PageSize;
			int num = 16;
			if (pageSize > num)
				pageSize = num;
			pageSize *= StorageHandle.DEFAULT_SIZE_OF_PAGE;
			return new DatabaseMetaTable(Database.CreateInstance(fileName, parentConnection, cryptoKey, pageSize, LCID, caseSensitive, toPack));
		}

		private DatabaseMetaTable(Database database)
		  : base(database, null)
		{
			LocalSQLConnection localSQLConnection = (LocalSQLConnection)database.ParentConnection.ParentEngine.OpenSQLConnection(null, this);
			database.AssignSQLContext(localSQLConnection);
		}

		internal void Create(bool exclusiveAccess, bool memoryBased, bool isolated)
		{
			Rowset.DeclareNewStorage(null);
			Rowset.CreateStorage(memoryBased ? new StorageHandle.StorageMode(FileMode.CreateNew, FileShare.None, FileAccess.ReadWrite, FileAttributes.Temporary, true, true, isolated) : new StorageHandle.StorageMode(FileMode.CreateNew, !exclusiveAccess, false, FileAccess.ReadWrite, true, isolated), 0UL, true);
		}

		internal void Open(bool exclusive, bool readOnly, bool readOnlyShared, bool isolated)
		{
			Rowset.OpenStorage(new StorageHandle.StorageMode(FileMode.Open, !exclusive, readOnlyShared, readOnly ? FileAccess.Read : FileAccess.ReadWrite, true, isolated), 0UL);
		}

		internal void Pack()
		{
			Pack(Database.Encryption == null ? null : Database.Encryption.EncryptionKeyString.Key, Database.PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE, Database.Culture.LCID, Database.CaseSensitive);
		}

		private static string FixBadDate(string expression)
		{
			int startIndex = 0;
			while (true)
			{
				int length = expression.ToUpperInvariant().IndexOf("DATE()", startIndex);
				if (length >= startIndex)
				{
					if (length > 0 && char.IsLetterOrDigit(expression[length - 1]))
					{
						startIndex = length + 6;
					}
					else
					{
						expression = expression.Substring(0, length) + "GETDATE()" + expression.Substring(length + 6);
						startIndex = length + 6;
					}
				}
				else
					break;
			}
			return expression;
		}

		internal void Pack(string newEncryptionKeyString, int newPageSize, int newLCID, bool newCaseSensitive)
		{
			if (temporaryDatabase != null)
			{
				temporaryDatabase.Dispose();
				temporaryDatabase = null;
			}
			bool noReadAheadCache = Database.Handle.NoReadAheadCache;
			Database.Handle.NoReadAheadCache = true;
			bool forcedCollectionMode = Database.ForcedCollectionMode;
			Database.ForcedCollectionMode = true;
			bool flag1 = false;
			string str;
			try
			{
				str = Path.GetTempFileName();
				File.Delete(str);
			}
			catch (SecurityException)
            {
				str = Path.Combine(Path.GetDirectoryName(Database.Handle.Name), Path.GetFileNameWithoutExtension(Database.Handle.Name) + "." + Guid.NewGuid().ToString() + ".tmp");
			}
			try
			{
				if (Database.IsReadOnly)
					throw new VistaDBException(337, Database.Name);
				if (Database.IsShared)
					throw new VistaDBException(338, Database.Name);
				bool exclusive1 = false;
				bool exclusive2 = true;
				string strA = Database.Encryption?.EncryptionKeyString.Key;
				bool flag2 = newEncryptionKeyString != null && string.Compare(strA, newEncryptionKeyString, StringComparison.Ordinal) != 0;
				if (newPageSize == 0)
				{
					newPageSize = Database.PageSize;
					newPageSize /= StorageHandle.DEFAULT_SIZE_OF_PAGE;
				}
				while (true)
				{
					if (File.Exists(str))
						File.Delete(str);
					DatabaseMetaTable database = (DatabaseMetaTable)ParentConnection.CreateDatabase(str, true, newEncryptionKeyString, newPageSize, newLCID == 0 ? Database.Culture.LCID : newLCID, newCaseSensitive, false, false);
					database.Database.operationDelegate = Database.operationDelegate;
					database.Database.RepairMode = Database.RepairMode;
					try
					{
						if (database.Database.PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE != newPageSize)
							throw new VistaDBException(53, newPageSize.ToString());
						IVistaDBDatabase vistaDbDatabase = this;
						database.Database.Header.SchemaVersionGuid = vistaDbDatabase.VersionGuid;
						IVistaDBTableNameCollection tableNames = vistaDbDatabase.GetTableNames();
						if (Database.operationDelegate != null)
						{
							int count = tableNames.Count;
							foreach (string name in tableNames)
							{
								IVistaDBTableSchema vistaDbTableSchema = vistaDbDatabase.TableSchema(name);
								if (vistaDbTableSchema.Indexes.Count > 0)
									++count;
								count += vistaDbTableSchema.ForeignKeys.Count;
							}
							database.Database.TotalOperationStatusLoops = (uint)count;
						}
						List<string> stringList1 = new List<string>();
						foreach (string name1 in tableNames)
						{
							IVistaDBTableSchema schema = vistaDbDatabase.TableSchema(name1);
							if (flag2)
							{
								foreach (IVistaDBColumnAttributes columnAttributes in schema)
									schema.DefineColumnAttributes(columnAttributes.Name, columnAttributes.AllowNull, columnAttributes.ReadOnly, true, columnAttributes.Packed, columnAttributes.Description);
							}
							if (Database.UpgradeDatatypesMode)
							{
								foreach (IVistaDBColumnAttributes columnAttributes in (ArrayList)((ArrayList)schema).Clone())
								{
									if (columnAttributes.Type == (VistaDBType.NChar | VistaDBType.SmallMoney))
										schema.AlterColumnType(columnAttributes.Name, VistaDBType.SmallDateTime);
									else if (columnAttributes.Type == (VistaDBType.VarChar | VistaDBType.NVarChar))
										schema.AlterColumnType(columnAttributes.Name, VistaDBType.TinyInt);
									else if (columnAttributes.Type == VistaDBType.NChar || columnAttributes.Type == VistaDBType.NVarChar || columnAttributes.Type == VistaDBType.NText)
										schema.AlterColumnType(columnAttributes.Name, columnAttributes.Type, columnAttributes.MaxLength, NCharColumn.DefaultUnicode);
								}
							}
							List<string> stringList2 = new List<string>();
							foreach (string key in schema.Indexes.Keys)
								stringList2.Add(key);
							foreach (string name2 in stringList2)
							{
								IVistaDBIndexInformation index = schema.Indexes[name2];
								if (index.Primary && name2.Equals("Primary Key", StringComparison.OrdinalIgnoreCase) || name2.StartsWith("SYS", StringComparison.OrdinalIgnoreCase) && int.TryParse(name2.Substring(3), out int result) && name2.Equals(string.Format("sys{0}", result), StringComparison.OrdinalIgnoreCase) || (name2.StartsWith("NewIndex", StringComparison.OrdinalIgnoreCase) && int.TryParse(name2.Substring(8), out result) && name2.Equals(string.Format("NewIndex{0}", result), StringComparison.OrdinalIgnoreCase) || stringList1.Contains(name2.ToUpperInvariant())))
								{
									if (!index.Temporary && !index.FKConstraint)
									{
										schema.DropIndex(name2);
										string name3 = !index.Primary ? (!index.Unique ? string.Format("IX_{0}_{1}", schema.Name, index.KeyExpression.Replace(';', '_').Replace("(", "").Replace(")", "")) : string.Format("UN_{0}_{1}", schema.Name, index.KeyExpression.Replace(';', '_').Replace("(", "").Replace(")", ""))) : string.Format("PK_{0}", schema.Name);
										if (stringList1.Contains(name1.ToUpperInvariant()))
										{
											result = 1;
											while (stringList1.Contains(string.Format("{0}{1}", name3, result)))
												++result;
											name3 = string.Format("{0}{1}", name3, result);
										}
										IVistaDBIndexInformation indexInformation = schema.DefineIndex(name3, index.KeyExpression, index.Primary, index.Unique);
										stringList1.Add(indexInformation.Name.ToUpperInvariant());
										database.Database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.IndexOperation, name2, "Renamed index to " + name3 + " to prevent schema collisions");
									}
								}
								else if (!index.Temporary && !index.FKConstraint)
									stringList1.Add(name2.ToUpperInvariant());
							}
							stringList2.Clear();
							foreach (string key in schema.Constraints.Keys)
								stringList2.Add(key);
							foreach (string name2 in stringList2)
							{
								IVistaDBConstraintInformation constraint = schema.Constraints[name2];
								string scriptExpression = FixBadDate(constraint.Expression);
								if (!scriptExpression.Equals(constraint.Expression) || stringList1.Contains(name2.ToUpperInvariant()) || name2.StartsWith(string.Format("{0}.NewConstraint", schema.Name)) && int.TryParse(name2.Substring(schema.Name.Length + 14), out int result) && name2.Equals(string.Format("{0}.NewConstraint{1}", schema.Name, result)))
								{
									schema.DropConstraint(name2);
									string name3 = string.Format("UC_{0}", schema.Name);
									if (stringList1.Contains(name3))
									{
										result = 1;
										while (stringList1.Contains(string.Format("{0}{1}", name3, result)))
											++result;
										name3 = string.Format("{0}{1}", name3, result);
									}
									schema.DefineConstraint(name3, scriptExpression, constraint.Description, constraint.AffectsInsertion, constraint.AffectsUpdate, constraint.AffectsDelete);
									stringList1.Add(name3.ToUpperInvariant());
									database.Database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.ConstraintOperation, name2, "Renamed constraint to " + name3 + " to prevent schema collisions");
								}
							}
							foreach (IVistaDBColumn vistaDbColumn in schema)
							{
								IVistaDBDefaultValueInformation defaultValue = schema.DefaultValues[vistaDbColumn.Name];
								if (defaultValue != null)
								{
									string scriptExpression = FixBadDate(defaultValue.Expression);
									if (!scriptExpression.Equals(defaultValue.Expression))
									{
										schema.DropDefaultValue(vistaDbColumn.Name);
										schema.DefineDefaultValue(vistaDbColumn.Name, scriptExpression, defaultValue.UseInUpdate, defaultValue.Description);
										database.Database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.SchemaChangeOperation, vistaDbColumn.Name, "Corrected invalid DATE default value, replaced with GETDATE()");
									}
								}
							}
							IVistaDBTable vistaDbTable = Database.OpenTable(name1, exclusive1, true, false);
							IVistaDBTable table = database.Database.CreateTable(schema, exclusive2, false, false);
							try
							{
								((Table)table).Rowset.SuppressErrors = Database.RepairMode;
								if (vistaDbTable != null)
								{
									try
									{
										vistaDbTable.ExportData(table, null);
									}
									catch (VistaDBException)
                                    {
										if (!Database.RepairMode)
										{
											if (!flag1)
												throw;
										}
									}
								}
								try
								{
									database.Database.CreateTableObjects((Table)table, schema);
								}
								catch (VistaDBException)
                                {
									if (!Database.RepairMode && !flag1)
										throw;
									else if (flag1)
										database.SetRepairMode(true);
								}
							}
							finally
							{
								vistaDbTable?.Dispose();
								table.Dispose();
							}
						}
						database.Database.CreateRelationships((Database.RelationshipCollection)vistaDbDatabase.Relationships, stringList1);
						database.Database.Description = vistaDbDatabase.Description;
						database.Database.CopyViewsFrom(Database);
						database.Database.CopyClrProceduresAndTriggersFrom(Database);
						database.Database.CopySqlProceduresAndFunctionsFrom(Database);
						Database.Handle.CopyFrom(database.Database.Handle);
						break;
					}
					catch (VistaDBException ex)
					{
						if (ex.ErrorId.Equals(154) || (ex.ErrorId.Equals(301) || ex.Contains(154L) || ex.Contains(301L)))
						{
							database.Database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.DataExportOperation, Path.GetFileNameWithoutExtension(Database.Handle.Name), "cannot be packed as a " + newPageSize + "k database, increasing page size and restarting pack operation.");
							++newPageSize;
						}
						else
							throw;
					}
					finally
					{
						database.Dispose();
					}
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 334, Database.Name);
			}
			finally
			{
				if (File.Exists(str))
					File.Delete(str);
				Database.ForcedCollectionMode = forcedCollectionMode;
				Database.Handle.NoReadAheadCache = noReadAheadCache;
			}
		}

		internal void SetRepairMode(bool repair)
		{
			Database.RepairMode = repair;
		}

		private Database Database
		{
			get
			{
				return (Database)Rowset;
			}
		}

		private void AddCascade(InsensitiveHashtable linkedList, IVistaDBRelationshipCollection relations, string tableName, bool inserting, bool deleting)
		{
			foreach (IVistaDBRelationshipInformation relationshipInformation in relations.Values)
			{
				if (!deleting && Database.DatabaseObject.EqualNames(relationshipInformation.ForeignTable, tableName) && (!Database.DatabaseObject.EqualNames(relationshipInformation.PrimaryTable, tableName) && !linkedList.Contains(relationshipInformation.PrimaryTable)))
					linkedList.Add(relationshipInformation.PrimaryTable, null);
				if (!inserting && Database.DatabaseObject.EqualNames(relationshipInformation.PrimaryTable, tableName) && (!Database.DatabaseObject.EqualNames(relationshipInformation.ForeignTable, tableName) && !linkedList.Contains(relationshipInformation.ForeignTable)))
				{
					linkedList.Add(relationshipInformation.ForeignTable, null);
					AddCascade(linkedList, relations, relationshipInformation.ForeignTable, inserting, deleting);
				}
			}
		}

		private IVistaDBTableSchema GetModificationTableSchema(Table table)
		{
			IVistaDBTableSchema vistaDbTableSchema = new TableSchema(table.Name, TableType.Default, null, ulong.MaxValue, Database);
			foreach (IColumn column in table.Rowset.CurrentRow)
				vistaDbTableSchema.AddColumn(column.Name, column.Type, column.MaxLength, 0);
			return vistaDbTableSchema;
		}

		private Table GetModificationTable(string name)
		{
			if (name == null)
				return null;
			name = Row.Column.FixName(name);
			if (VistaDBContext.SQLChannel.IsAvailable && (Database.DatabaseObject.EqualNames(name, TriggeredDelete) || Database.DatabaseObject.EqualNames(name, TriggeredInsert)))
			{
				TriggerContext triggerContext = VistaDBContext.SQLChannel.TriggerContext;
				foreach (Table modificationTable in triggerContext.ModificationTables)
				{
					if (triggerContext != null && Database.DatabaseObject.EqualNames(name, modificationTable.Name))
						return modificationTable;
				}
			}
			return null;
		}

		IVistaDBRow IVistaDBTable.CurrentRow
		{
			get
			{
				return CurrentRow.CopyInstance();
			}
			set
			{
				throw new InvalidOperationException();
			}
		}

		IVistaDBRow IVistaDBTable.CurrentKey
		{
			get
			{
				return null;
			}
			set
			{
				throw new InvalidOperationException();
			}
		}

		IVistaDBRow IVistaDBTable.LastSessionIdentity
		{
			get
			{
				return null;
			}
		}

		IVistaDBRow IVistaDBTable.LastTableIdentity
		{
			get
			{
				return null;
			}
		}

		IVistaDBIndexCollection IVistaDBTable.TemporaryIndexes
		{
			get
			{
				return null;
			}
		}

		IVistaDBIndexCollection IVistaDBTable.RegularIndexes
		{
			get
			{
				return null;
			}
		}

		IVistaDBRow IVistaDBTable.Evaluate(string expression)
		{
			return null;
		}

		void IVistaDBTable.SetFilter(string expression, bool optimize)
		{
		}

		bool IVistaDBTable.Find(string keyEvaluationExpression, string indexName, bool partialMatching, bool softPosition)
		{
			return false;
		}

		void IVistaDBTable.SetScope(string lowKeyExpression, string highKeyExpression)
		{
		}

		void IVistaDBTable.ResetScope()
		{
		}

		void IVistaDBTable.CreateIndex(string name, string keyExpression, bool primary, bool unique)
		{
		}

		void IVistaDBTable.CreateTemporaryIndex(string name, string keyExpression, bool unique)
		{
		}

		void IVistaDBTable.DropIndex(string name)
		{
		}

		void IVistaDBTable.CreateIdentity(string columnName, string seedExpression, string stepExpression)
		{
		}

		void IVistaDBTable.DropIdentity(string columnName)
		{
		}

		void IVistaDBTable.CreateDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description)
		{
		}

		void IVistaDBTable.DropDefaultValue(string columnName)
		{
		}

		void IVistaDBTable.CreateForeignKey(string constraintName, string primaryTable, string foreignKey, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity, string description)
		{
		}

		void IVistaDBTable.DropForeignKey(string constraintName)
		{
		}

		void IVistaDBTable.Put(string columnName, IVistaDBValue columnValue)
		{
		}

		void IVistaDBTable.Put(int columnIndex, IVistaDBValue columnValue)
		{
		}

		void IVistaDBTable.PutFromFile(string columnName, string fileName)
		{
		}

		void IVistaDBTable.PutFromFile(int columnIndex, string fileName)
		{
		}

		void IVistaDBTable.Insert()
		{
		}

		void IVistaDBTable.Delete()
		{
		}

		void IVistaDBTable.Post()
		{
		}

		void IVistaDBTable.ExportData(IVistaDBTable table, string constraint)
		{
		}

		IConversion IDatabase.Conversion
		{
			get
			{
				return Database.Conversion;
			}
		}

		IColumn IDatabase.GetTableAnchor(string tableName)
		{
			return Database.GetTimestampAnchor(tableName);
		}

		CultureInfo IVistaDBDatabase.Culture
		{
			get
			{
				return Rowset.Culture;
			}
		}

		bool IVistaDBDatabase.CaseSensitive
		{
			get
			{
				return Rowset.CaseSensitive;
			}
		}

		string IVistaDBDatabase.Description
		{
			get
			{
				return Database.Description;
			}
			set
			{
				Database.Description = value;
			}
		}

		int IVistaDBDatabase.PageSize
		{
			get
			{
				return Database.PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE;
			}
		}

		VistaDBDatabaseOpenMode IVistaDBDatabase.Mode
		{
			get
			{
				if (Database.IsShared)
				{
					if (Database.IsShareReadOnly)
						return VistaDBDatabaseOpenMode.SharedReadOnly;
					return !Database.IsReadOnly ? VistaDBDatabaseOpenMode.NonexclusiveReadWrite : VistaDBDatabaseOpenMode.NonexclusiveReadOnly;
				}
				return !Database.IsReadOnly ? VistaDBDatabaseOpenMode.ExclusiveReadWrite : VistaDBDatabaseOpenMode.ExclusiveReadOnly;
			}
		}

		int IVistaDBDatabase.NestedTransactionLevel
		{
			get
			{
				return Database == null || !Database.IsTransaction ? 0 : 1;
			}
		}

		IsolationLevel IVistaDBDatabase.IsolationLevel
		{
			get
			{
				if (!Database.IsTransaction)
					return IsolationLevel.Unspecified;
				return Database.TpIsolationLevel;
			}
		}

		IVistaDBRelationshipCollection IVistaDBDatabase.Relationships
		{
			get
			{
				return Database.GetRelationships();
			}
		}

		bool IVistaDBDatabase.IsolatedStorage
		{
			get
			{
				return Database.Handle.IsolatedStorage;
			}
		}

		char IDatabase.MaximumChar
		{
			get
			{
				return Database.EvaluateMaximumChar();
			}
		}

		IColumn IDatabase.CreateEmtpyUnicodeColumn()
		{
			return Database.CreateSQLUnicodeColumnInstance();
		}

		IColumn IDatabase.CreateEmptyColumn(VistaDBType type)
		{
			return Database.CreateEmptyColumnInstance(type);
		}

		IVistaDBTableSchema IVistaDBDatabase.NewTable(string name)
		{
			return new TableSchema(name, TableType.Default, null, 0UL, Database);
		}

		IVistaDBTableSchema IVistaDBDatabase.TableSchema(string name)
		{
			Table modificationTable = GetModificationTable(name);
			if (modificationTable != null)
				return GetModificationTableSchema(modificationTable);
			if (HasTemporaryTable(name))
				return temporaryDatabase.TableSchema(name);
			return Database.GetTableSchema(name, false);
		}

		private bool HasTemporaryTable(string name)
		{
			if (temporaryDatabase == null || name == null)
				return false;
			return temporaryDatabase.ContainsTable(name);
		}

		IVistaDBTable IVistaDBDatabase.OpenTable(string name, bool exclusive, bool readOnly)
		{
			if (HasTemporaryTable(name))
				return temporaryDatabase.OpenTable(name, exclusive, false);
			Table modificationTable = GetModificationTable(name);
			if (modificationTable == null)
				return Database.OpenTable(name, exclusive, readOnly, true);
			modificationTable.AddCloneReference();
			return modificationTable;
		}

		IVistaDBTable IVistaDBDatabase.CreateTable(IVistaDBTableSchema schema, bool exclusive, bool readOnly)
		{
			return Database.CreateTable(schema, exclusive, readOnly, true);
		}

		void IVistaDBDatabase.AlterTable(string newName, IVistaDBTableSchema schema)
		{
			if (HasTemporaryTable(newName))
				temporaryDatabase.AlterTable(newName, schema);
			else
				Database.AlterTable(newName, (TableSchema)schema, false, false);
		}

		void IVistaDBTable.SetDDAEventDelegate(IVistaDBDDAEventDelegate eventDelegate)
		{
			if (eventDelegate.Type != DDAEventDelegateType.NewVersion)
				return;
			Database.SetDelegate(eventDelegate);
		}

		void IVistaDBTable.ResetEventDelegate(DDAEventDelegateType eventType)
		{
			Database.ResetDelegate(eventType);
		}

		void IVistaDBDatabase.DropTable(string name)
		{
			if (temporaryDatabase != null)
			{
				IVistaDBTableNameCollection tableNames = temporaryDatabase.GetTableNames();
				if (tableNames.Contains(name))
				{
					((DatabaseMetaTable)temporaryDatabase).Database.CloseAndDropTemporaryTable(name);
					if (tableNames.Count > 1)
						return;
					temporaryDatabase.Dispose();
					temporaryDatabase = null;
					return;
				}
			}
			Database.DropTable(name);
		}

		bool IVistaDBDatabase.ContainsTable(string Tablename)
		{
			if (((ICollection<string>)Database.GetTableIdMap()).Contains(Tablename))
				return true;
			if (temporaryDatabase == null)
				return false;
			return temporaryDatabase.GetTableNames().Contains(Tablename);
		}

		IVistaDBTableNameCollection IVistaDBDatabase.GetTableNames()
		{
			Database.TableIdMap tableIdMap = Database.GetTableIdMap();
			if (VistaDBContext.SQLChannel.IsAvailable)
			{
				TriggerContext triggerContext = VistaDBContext.SQLChannel.TriggerContext;
				if (triggerContext != null && triggerContext.ModificationTables != null)
				{
					foreach (Table modificationTable in triggerContext.ModificationTables)
						tableIdMap.Add(modificationTable.Name, modificationTable.Id);
				}
			}
			if (temporaryDatabase != null)
			{
				foreach (KeyValuePair<ulong, string> tableName in (Database.TableIdMap)temporaryDatabase.GetTableNames())
					tableIdMap.AddTable(tableName.Value);
			}
			return tableIdMap;
		}

		public IRow GetRowStructure(string tableName)
		{
			if (!HasTemporaryTable(tableName))
				return Database.GetRowStruct(tableName);
			return ((IDatabase)temporaryDatabase).GetRowStructure(tableName);
		}

		void IVistaDBDatabase.ExportXml(string xmlFileName, VistaDBXmlWriteMode mode)
		{
			DoExportXml(xmlFileName, mode);
		}

		void IVistaDBDatabase.ImportXml(string xmlFileName, VistaDBXmlReadMode mode, bool interruptOnError)
		{
			Database.ImportXml(xmlFileName, mode, interruptOnError);
		}

		void IVistaDBDatabase.ImportXml(XmlReader xmlReader, VistaDBXmlReadMode mode, bool interruptOnError)
		{
			Database.ImportXmlReader(xmlReader, mode, interruptOnError);
		}

		void IVistaDBDatabase.AddToXmlTransferList(string tableName)
		{
			Database.AddToTransferList(tableName);
		}

		void IVistaDBDatabase.ClearXmlTransferList()
		{
			Database.ClearTransferList();
		}

		InsensitiveHashtable IDatabase.GetRelationships(string tableName, bool insert, bool delete)
		{
			if (HasTemporaryTable(tableName))
				return ((IDatabase)temporaryDatabase).GetRelationships(tableName, insert, delete);
			InsensitiveHashtable linkedList = new InsensitiveHashtable();
			IVistaDBTableSchema tableSchema = Database.GetTableSchema(tableName, true);
			IVistaDBRelationshipCollection foreignKeys = tableSchema.ForeignKeys;
			AddCascade(linkedList, tableSchema.ForeignKeys, tableName, insert, delete);
			linkedList.Remove(tableName);
			return linkedList;
		}

		IVistaDBColumn IVistaDBDatabase.GetLastIdentity(string tableName, string columnName)
		{
			if (HasTemporaryTable(tableName))
				return temporaryDatabase.GetLastIdentity(tableName, columnName);
			tableName = tableName == null ? null : Row.Column.FixName(tableName);
			columnName = columnName == null ? null : Row.Column.FixName(columnName);
			return Database.GetLastIdentity(tableName, columnName);
		}

		IVistaDBColumn IVistaDBDatabase.GetLastTimestamp(string tableName)
		{
			if (HasTemporaryTable(tableName))
				return temporaryDatabase.GetLastTimestamp(tableName);
			tableName = Row.Column.FixName(tableName);
			return Database.GetLastTimestamp(tableName);
		}

		void IVistaDBDatabase.BeginTransaction()
		{
			Database.BeginTransaction(IsolationLevel.ReadCommitted);
		}

		void IVistaDBDatabase.BeginTransaction(IsolationLevel level)
		{
			Database.BeginTransaction(level);
		}

		void IVistaDBDatabase.CommitTransaction()
		{
			Database.CommitTransaction();
		}

		void IVistaDBDatabase.RollbackTransaction()
		{
			Database.RollbackTransaction();
		}

		void IDatabase.CreateViewObject(IView view)
		{
			Database.RegisterViewObject(view);
		}

		void IDatabase.DeleteViewObject(IView view)
		{
			Database.UnregisterViewObject(view);
		}

		IViewList IDatabase.EnumViews()
		{
			return Database.LoadViews();
		}

		IView IDatabase.CreateViewInstance(string name)
		{
			return new Database.ViewList.View(name);
		}

		bool IDatabase.TryGetProcedure(string procedureName, out ClrHosting.ClrProcedure procedure)
		{
			return Database.TryGetProcedure(procedureName, out procedure);
		}

		IStoredProcedureCollection IDatabase.GetStoredProcedures()
		{
			return Database.LoadSqlStoredProcedures();
		}

		IStoredProcedureInformation IDatabase.CreateStoredProcedureInstance(string name, string script, string description)
		{
			return new Database.StoredProcedureCollection.StoredProcedureInformation(name, script, description);
		}

		void IDatabase.CreateStoredProcedureObject(IStoredProcedureInformation sp)
		{
			Database.RegisterStoredProcedure(sp);
		}

		void IDatabase.DeleteStoredProcedureObject(string name)
		{
			Database.UnregisterStoredProcedure(name, true);
		}

		IUserDefinedFunctionCollection IDatabase.GetUserDefinedFunctions()
		{
			return Database.LoadSqlUserDefinedFunctions();
		}

		IUserDefinedFunctionInformation IDatabase.CreateUserDefinedFunctionInstance(string name, string script, bool scalarValued, string description)
		{
			return new Database.UdfCollection.UserDefinedFunction(name, script, scalarValued, description);
		}

		void IDatabase.CreateUserDefinedFunctionObject(IUserDefinedFunctionInformation udf)
		{
			Database.RegisterUserDefinedFunction(udf);
		}

		void IDatabase.DeleteUserDefinedFunctionObject(string name)
		{
			Database.UnregisterUserDefinedFunction(name, true);
		}

		void IVistaDBDatabase.AddAssembly(string name, string assemblyFileName, string description)
		{
			Database.CreateAssembly(name, ClrHosting.COFFImage(assemblyFileName), description, true);
		}

		void IVistaDBDatabase.UpdateAssembly(string name, string assemblyFileName, string description)
		{
			Database.UpdateAssembly(name, ClrHosting.COFFImage(assemblyFileName), description);
		}

		void IVistaDBDatabase.DropAssembly(string name, bool force)
		{
			Database.DeleteAssembly(name, force);
		}

		IVistaDBAssemblyCollection IVistaDBDatabase.GetAssemblies()
		{
			return Database.LoadAssemblies(false);
		}

		void IVistaDBDatabase.RegisterClrProcedure(string procedureName, string clrHostedProcedure, string assemblyName, string description)
		{
			Database.RegisterCLRProc(procedureName, clrHostedProcedure, assemblyName, description);
		}

		void IVistaDBDatabase.UnregisterClrProcedure(string procedureName)
		{
			Database.UnregisterCLRProc(procedureName, true, null);
		}

		IVistaDBClrProcedureCollection IVistaDBDatabase.GetClrProcedures()
		{
			return Database.LoadClrProcedureObjects();
		}

		void IVistaDBDatabase.PrepareClrContext(IVistaDBPipe pipe)
		{
			VistaDBContext.DDAChannel.ActivateContext(this, pipe);
			VistaDBContext.SQLChannel.ActivateContext(null, null);
		}

		void IVistaDBDatabase.PrepareClrContext()
		{
			VistaDBContext.DDAChannel.ActivateContext(this, null);
			VistaDBContext.SQLChannel.ActivateContext(null, null);
		}

		void IVistaDBDatabase.UnprepareClrContext()
		{
			VistaDBContext.SQLChannel.DeactivateContext();
			VistaDBContext.DDAChannel.DeactivateContext();
		}

		object IVistaDBDatabase.InvokeClrProcedure(string procedureName, params object[] parameters)
		{
			return Database.InvokeClrProcedure(procedureName, false, parameters);
		}

		object IVistaDBDatabase.InvokeClrProcedureFillRow(string procedureName, params object[] parameters)
		{
			return Database.InvokeClrProcedure(procedureName, true, parameters);
		}

		bool IVistaDBDatabase.TestDatabaseObjectName(string name, bool raiseException)
		{
			if (DirectConnection.IsCorrectAlias(name))
				return true;
			if (raiseException)
				throw new VistaDBException(152, name);
			return false;
		}

		MethodInfo IDatabase.PrepareInvoke(string clrProcedure, out MethodInfo fillRow)
		{
			ClrHosting.ClrProcedure clrProcedure1 = Database.PrepareClrProcedureInvoke(clrProcedure);
			fillRow = clrProcedure1.FillRowMethod;
			return clrProcedure1.Method;
		}

		void IVistaDBDatabase.RegisterClrTrigger(string triggerName, string clrHostedMethod, string assemblyName, string tableName, TriggerAction eventType, string description)
		{
			Database.RegisterCLRTrigger(triggerName, clrHostedMethod, assemblyName, tableName, eventType, description);
		}

		void IVistaDBDatabase.UnregisterClrTrigger(string triggerName)
		{
			Database.UnregisterCLRTrigger(triggerName, true, null);
		}

		IVistaDBClrTriggerCollection IVistaDBDatabase.GetClrTriggers()
		{
			return Database.LoadClrTriggers(null);
		}

		IVistaDBClrTriggerCollection IVistaDBDatabase.GetClrTriggers(string tableName)
		{
			if (HasTemporaryTable(tableName))
				return temporaryDatabase.GetClrTriggers(tableName);
			if (tableName == null || tableName.Length == 0)
				throw new VistaDBException(280);
			return Database.LoadClrTriggers(Row.Column.FixName(tableName));
		}

		IVistaDBTable IDatabase.CreateTemporaryTable(IVistaDBTableSchema schema)
		{
			if (temporaryDatabase == null)
			{
				ParentConnection.TemporaryPath = Path.GetDirectoryName(Database.Handle.Name);
				temporaryDatabase = ParentConnection.CreateInMemoryDatabase(Database.Encryption == null ? null : Database.Encryption.EncryptionKeyString.Key, Database.PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE, Database.Culture.LCID, Database.CaseSensitive);
			}
			return temporaryDatabase.CreateTable(schema, false, false);
		}

		void IVistaDBDatabase.ActivateSyncService(string tableName)
		{
			if (tableName != null)
			{
				Database.ActivateSyncService(tableName);
			}
			else
			{
				Database.ActivateSyncMode = true;
				try
				{
				}
				finally
				{
					Database.ActivateSyncMode = false;
				}
			}
		}

		void IVistaDBDatabase.DeactivateSyncService(string tableName)
		{
			if (tableName != null)
			{
				Database.DeactivateSyncService(tableName);
			}
			else
			{
				Database.DeactivateSyncMode = true;
				try
				{
				}
				finally
				{
					Database.DeactivateSyncMode = false;
				}
			}
		}

		Guid IVistaDBDatabase.VersionGuid
		{
			get
			{
				return Database.Originator;
			}
		}

		protected override void Put(IValue columnValue, int columnIndex)
		{
		}

		protected override IVistaDBValue Get(int columnIndex)
		{
			return base.Get(columnIndex);
		}

		internal override void DoExportXml(string xmlFileName, VistaDBXmlWriteMode mode)
		{
			Database.ExportXml(xmlFileName, mode);
		}

		internal override void DoImportXml(string xmlFileName, VistaDBXmlReadMode mode, bool interruptOnError)
		{
			Database.ImportXml(xmlFileName, mode, interruptOnError);
		}

		void IVistaDBTable.Close()
		{
			Dispose();
		}

		void IDisposable.Dispose()
		{
			Dispose();
		}

		public new void Dispose()
		{
			if (isDisposed)
				return;
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing)
		{
			if (isDisposed)
				return;
			try
			{
				if (temporaryDatabase != null)
				{
					if (disposing)
						temporaryDatabase.Dispose();
					temporaryDatabase = null;
				}
				if (!IsClosed)
					ParentConnection.UnregisterDatabase(this);
				if (disposing)
					base.Dispose();
				isDisposed = true;
			}
			catch (Exception)
            {
				throw;
			}
		}
	}
}
