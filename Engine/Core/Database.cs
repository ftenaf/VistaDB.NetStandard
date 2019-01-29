





using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Engine.Core
{
	internal class Database : ClusteredRowset
	{
		internal static string SystemSchema = "Database Schema";
		internal static string DescriptionName = "DatabaseDescription";
		internal static ulong UniqueId = 0;
		private Database.IdentityCollection lastIdentities = new Database.IdentityCollection();
		private Database.IdentityCollection lastTimeStamps = new Database.IdentityCollection();
		private Database.TableIdMap transferList = new Database.TableIdMap(10);
		private IsolationLevel tpIsolationLevel = IsolationLevel.ReadCommitted;
		private char maxChar = 'ￜ';
		private uint totalOperationStatusLoops = 1;
		private int maxTableColumns = Table.MaxColumns;
		private int maxIndexColumns = Table.MaxColumns;
		private Database.EnumCache<IViewList> _viewsCache = new Database.EnumCache<IViewList>();
		private Database.EnumCache<IStoredProcedureCollection> _spCache = new Database.EnumCache<IStoredProcedureCollection>();
		private Database.EnumCache<IUserDefinedFunctionCollection> _udfCache = new Database.EnumCache<IUserDefinedFunctionCollection>();
		private const int MaxCacheSeconds = 10;
		private Database.Descriptors descriptors;
		private CrossConversion conversion;
		private int typeIndex;
		private int foreignReferenceIndex;
		private int nameIndex;
		private int objectIdIndex;
		private int referenceIndex;
		private int optionIndex;
		private int statusIndex;
		private int scriptValueIndex;
		private int descriptionIndex;
		private int extraBinaryIndex;
		private int extraBlobDataIndex;
		private IndexParser stdIndexParser;
		private ClrHosting clrHosting;
		private ClrHosting clrHostedTriggers;
		private LocalSQLConnection sqlContext;
		private bool packOperationMode;
		private bool upgradeFileVersionMode;
		private bool upgradeFTSStructure;
		private bool activateSyncMode;
		private bool deactivateSyncMode;
		private bool forcedGarbageCollection;
		private bool creation;
		private OperationCallbackDelegate operationCallbackDelegate;
		private uint currentOperationStatusLoop;
		private bool repairMode;

		internal static Database CreateInstance(string fileName, DirectConnection parentConnection, EncryptionKey cryptoKey, int pageSize, int LCID, bool caseSensitive, bool toPack)
		{
			++Database.UniqueId;
			Database database = new Database(fileName, "DB" + Database.UniqueId.ToString(), parentConnection, new Parser(parentConnection), Encryption.CreateEncryption(cryptoKey), toPack);
			database.DoAfterConstruction(pageSize, new CultureInfo(LCID == 0 ? parentConnection.LCID : LCID));
			database.Header.SetSensitivity(caseSensitive);
			return database;
		}

		private Database(string name, string alias, DirectConnection connection, Parser parser, Encryption encryption, bool toPack)
		  : base(name, alias, null, connection, parser, encryption, null, Table.TableType.Default)
		{
			stdIndexParser = new IndexParser(connection);
			descriptors = new Database.Descriptors();
			clrHosting = new ClrHosting();
			clrHostedTriggers = new ClrHosting();
			packOperationMode = toPack;
		}

		internal Parser SqlKeyParser
		{
			get
			{
				return stdIndexParser;
			}
		}

		internal string Description
		{
			get
			{
				return GetDescription();
			}
			set
			{
				RegisterDescription(value);
			}
		}

		internal bool UpgradeDatatypesMode
		{
			get
			{
				if (upgradeFileVersionMode)
					return Header.DatabaseFileSchemaVersion < 6;
				return false;
			}
		}

		internal bool UpgradeExtensionsMode
		{
			get
			{
				if (upgradeFileVersionMode)
					return Header.DatabaseFileSchemaVersion < 7;
				return false;
			}
		}

		internal bool UpgradeFTSStructure
		{
			get
			{
				return upgradeFTSStructure;
			}
		}

		internal bool ActivateSyncMode
		{
			get
			{
				return activateSyncMode;
			}
			set
			{
				activateSyncMode = value;
			}
		}

		internal bool DeactivateSyncMode
		{
			get
			{
				return deactivateSyncMode;
			}
			set
			{
				deactivateSyncMode = value;
			}
		}

		internal bool RepairMode
		{
			get
			{
				return repairMode;
			}
			set
			{
				repairMode = value;
			}
		}

		internal override OperationCallbackDelegate operationDelegate
		{
			get
			{
				return operationCallbackDelegate;
			}
			set
			{
				operationCallbackDelegate = value;
			}
		}

		internal override uint TotalOperationStatusLoops
		{
			get
			{
				return totalOperationStatusLoops;
			}
			set
			{
				totalOperationStatusLoops = value;
				currentOperationStatusLoop = 0U;
			}
		}

		internal override uint CurrentOperationStatusLoop
		{
			get
			{
				return currentOperationStatusLoop;
			}
			set
			{
				if (value >= totalOperationStatusLoops)
					TotalOperationStatusLoops = 1U;
				else
					currentOperationStatusLoop = value;
			}
		}

		internal Database.DatabaseHeader Header
		{
			get
			{
				return (Database.DatabaseHeader)base.Header;
			}
		}

		internal override CrossConversion Conversion
		{
			get
			{
				return conversion;
			}
		}

		internal int MaxTableColumns
		{
			get
			{
				return maxTableColumns;
			}
		}

		internal int MaxIndexColumns
		{
			get
			{
				return maxIndexColumns;
			}
		}

		internal override bool CaseSensitive
		{
			get
			{
				return base.Header.CaseSensitive;
			}
		}

		internal override bool AllowPostponing
		{
			get
			{
				return false;
			}
		}

		internal override bool ForcedCollectionMode
		{
			get
			{
				return forcedGarbageCollection;
			}
			set
			{
				forcedGarbageCollection = value;
			}
		}

		internal void LockSpaceMap()
		{
			LockRow(uint.MaxValue, false);
		}

		internal void UnlockSpaceMap()
		{
			UnlockRow(uint.MaxValue, false, false);
		}

		private void LinkDescriptor(Table table)
		{
			descriptors.Add(table.Id, table);
		}

		internal void UnlinkDescriptor(Table table)
		{
			if (table == null || descriptors == null)
				return;
			descriptors.Remove(table.Id);
		}

		private void CloseTables()
		{
			if (descriptors != null)
				descriptors.Clear();
			descriptors = null;
			if (lastIdentities != null)
				lastIdentities.Clear();
			lastIdentities = null;
			if (lastTimeStamps != null)
				lastTimeStamps.Clear();
			lastTimeStamps = null;
		}

		private IVistaDBTableSchema InitializeMetaTableSchema()
		{
			IVistaDBTableSchema vistaDbTableSchema = new Table.TableSchema(Database.SystemSchema, ActivateSyncMode ? Table.TableType.Tombstone : Table.TableType.Default, null, 0UL, this);
			bool encrypted = Encryption != null;
			int num = PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE;
			int maxLen1 = 128;
			if (num < 4)
				maxLen1 = PageSize / 32;
			int maxLen2 = num * 100;
			int maxLen3 = num * 250;
			maxTableColumns = Table.MaxColumns;
			if (num < 7)
				maxTableColumns /= 2 << 6 - num;
			maxIndexColumns = maxTableColumns;
			if (num < 8)
				maxIndexColumns = maxTableColumns / 2;
			int maxLen4 = maxTableColumns * 2;
			if (num <= 4)
				maxLen4 = 128;
			typeIndex = vistaDbTableSchema.AddColumn("typeId", VistaDBType.Int).RowIndex;
			foreignReferenceIndex = vistaDbTableSchema.AddColumn("foreignReference", VistaDBType.BigInt).RowIndex;
			nameIndex = vistaDbTableSchema.AddColumn("name", VistaDBType.NChar, maxLen1, NCharColumn.Utf16CodePage).RowIndex;
			vistaDbTableSchema.DefineColumnAttributes("typeId", false, true, encrypted, false, null, null);
			vistaDbTableSchema.DefineColumnAttributes("foreignReference", false, true, encrypted, false, null, null);
			vistaDbTableSchema.DefineColumnAttributes("name", false, true, encrypted, false, null, null);
			objectIdIndex = vistaDbTableSchema.AddColumn("objectId", VistaDBType.BigInt).RowIndex;
			referenceIndex = vistaDbTableSchema.AddColumn("reference", VistaDBType.BigInt).RowIndex;
			optionIndex = vistaDbTableSchema.AddColumn("options", VistaDBType.BigInt).RowIndex;
			statusIndex = vistaDbTableSchema.AddColumn("status", VistaDBType.Bit).RowIndex;
			scriptValueIndex = vistaDbTableSchema.AddColumn("scriptValue", VistaDBType.NChar, maxLen3, NCharColumn.Utf16CodePage).RowIndex;
			descriptionIndex = vistaDbTableSchema.AddColumn("description", VistaDBType.NChar, maxLen2, NCharColumn.Utf16CodePage).RowIndex;
			extraBinaryIndex = vistaDbTableSchema.AddColumn("extraBinary", VistaDBType.VarBinary, maxLen4).RowIndex;
			extraBlobDataIndex = vistaDbTableSchema.AddColumn("extraBlob", VistaDBType.Image).RowIndex;
			if (ActivateSyncMode)
				AppendSyncStructure((Table.TableSchema)vistaDbTableSchema);
			return vistaDbTableSchema;
		}

		private void CreateIndexes(Table table, IVistaDBIndexCollection indexes)
		{
			bool flag = false;
			foreach (IVistaDBIndexInformation indexInformation in indexes.Values)
			{
				if (!indexInformation.FKConstraint)
				{
					string name = indexInformation.Name;
					string keyExpression = indexInformation.KeyExpression;
					bool primary = indexInformation.Primary;
					bool unique = indexInformation.Unique;
					bool sparse = false;
					bool fkConstraint = indexInformation.FKConstraint;
					bool fullTextSearch = indexInformation.FullTextSearch;
					bool temporary = false;
					try
					{
						table.DeclareIndex(name, keyExpression, null, primary, unique, false, sparse, fkConstraint, fullTextSearch, temporary, 0);
						flag = true;
					}
					catch (Exception ex)
					{
						if (!RepairMode)
							throw;
					}
				}
			}
			if (!flag)
				return;
			table.BuildIndexes(false);
		}

		private void CreateObjects(Table table, IVistaDBTableSchema schema)
		{
			IVistaDBTable vistaDbTable = table;
			foreach (IVistaDBDefaultValueInformation valueInformation in schema.DefaultValues.Values)
			{
				try
				{
					vistaDbTable.CreateDefaultValue(valueInformation.ColumnName, valueInformation.Expression, valueInformation.UseInUpdate, valueInformation.Description);
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
			foreach (IVistaDBIdentityInformation identityInformation in schema.Identities.Values)
			{
				try
				{
					string seedValue = identityInformation.SeedValue;
					if (string.IsNullOrEmpty(seedValue))
					{
						Row.Column column = table.Rowset.LookForColumn(identityInformation.ColumnName);
						seedValue = table.Rowset.DefaultRow[column.RowIndex].Value.ToString();
					}
					vistaDbTable.CreateIdentity(identityInformation.ColumnName, seedValue, identityInformation.StepExpression);
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
			foreach (IVistaDBConstraintInformation constraintInformation in schema.Constraints.Values)
			{
				try
				{
					vistaDbTable.CreateConstraint(constraintInformation.Name, constraintInformation.Expression, constraintInformation.Description, constraintInformation.AffectsInsertion, constraintInformation.AffectsUpdate, constraintInformation.AffectsDelete);
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
		}

		internal void ActivateIndex(Table table, IVistaDBIndexInformation info)
		{
			RowsetIndex instance = RowsetIndex.CreateInstance(string.Empty, info.Name, info.KeyExpression, info.FullTextSearch, table.Rowset, ParentConnection, this, Culture, Encryption);
			if (info.FullTextSearch)
			{
				if (UpgradeFTSStructure)
				{
					if (packOperationMode)
						return;
					throw new VistaDBException(52);
				}
			}
			try
			{
				instance.OpenStorage(table.Rowset.Handle.Mode, ((Table.TableSchema.IndexCollection.IndexInformation)info).HeaderPosition);
				table.AddOrder(instance.Alias, instance);
			}
			catch (Exception ex)
			{
				if (RepairMode)
					return;
				throw;
			}
		}

		private void ActivateIndexes(Table table, IVistaDBTableSchema schema)
		{
			IVistaDBIndexCollection indexes = schema.Indexes;
			ClusteredRowset rowset = table.Rowset;
			foreach (IVistaDBIndexInformation info in indexes.Values)
				ActivateIndex(table, info);
		}

		private void ActivateObjects(ClusteredRowset rowset, IVistaDBTableSchema schema)
		{
			foreach (IVistaDBIdentityInformation identityInformation in schema.Identities.Values)
			{
				string columnName = identityInformation.ColumnName;
				Row.Column column = rowset.LookForColumn(columnName);
				if (column == null)
					throw new VistaDBException(181, columnName);
				try
				{
					rowset.ActivateIdentity(column, identityInformation.StepExpression);
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
			foreach (IVistaDBDefaultValueInformation valueInformation in schema.DefaultValues.Values)
			{
				string columnName = valueInformation.ColumnName;
				Row.Column column = rowset.LookForColumn(columnName);
				if (column == null)
					throw new VistaDBException(181, columnName);
				try
				{
					rowset.ActivateDefaultValue(column, valueInformation.Expression, valueInformation.UseInUpdate);
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
			foreach (IVistaDBConstraintInformation constraintInformation in schema.Constraints.Values)
			{
				try
				{
					rowset.ActivateConstraint(constraintInformation.Name, constraintInformation.Expression, ((Table.TableSchema.ConstraintCollection.ConstraintInformation)constraintInformation).Option, rowset, false);
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
			try
			{
				rowset.ActivateTriggers(schema);
			}
			catch (Exception ex)
			{
				if (RepairMode)
					return;
				throw;
			}
		}

		private void ActivateRelationships(Table table, IVistaDBTableSchema schema)
		{
			ClusteredRowset rowset = table.Rowset;
			foreach (IVistaDBRelationshipInformation relationship in schema.ForeignKeys.Values)
			{
				try
				{
					if (Database.DatabaseObject.EqualNames(relationship.PrimaryTable, rowset.Name))
						rowset.ActivatePrimaryKeyReference(table, relationship.ForeignTable, relationship.Name, relationship.UpdateIntegrity, relationship.DeleteIntegrity);
					if (Database.DatabaseObject.EqualNames(relationship.ForeignTable, rowset.Name))
					{
						rowset.ActivateForeignKey(table, relationship.Name, relationship.PrimaryTable);
						if (Database.DatabaseObject.EqualNames(relationship.PrimaryTable, relationship.ForeignTable))
							rowset.SaveSelfRelationship(relationship);
					}
				}
				catch (Exception ex)
				{
					if (!RepairMode)
						throw;
				}
			}
		}

		private void ActivateReadonly(Table table, IVistaDBTableSchema schema)
		{
			foreach (Row.Column column in table.Rowset.CurrentRow)
			{
				if (column.ReadOnly && schema.Identities[column.Name] == null)
					table.Rowset.ActivateReadOnly(column);
			}
		}

		private void CreateObjectEntry(bool commit, Database.VdbObjects type, ulong foreignReference, string name, ulong objectId, ulong reference, long options, bool status, string scriptValue, string description, byte[] binary, byte[] blob)
		{
			PrepareEditStatus();
			FillRowData(SatelliteRow, type, foreignReference, name, objectId, reference, options, status, scriptValue, description, binary, blob);
			CreateRow(commit, false);
		}

		private void UpdateObjectEntry(bool commit, Database.VdbObjects type, ulong foreignReference, string name, ulong objectId, ulong reference, long options, bool status, string scriptValue, string description, byte[] binary, byte[] blob)
		{
			FillRowData(SatelliteRow, type, foreignReference, name, objectId, reference, options, status, scriptValue, description, binary, blob);
			SatelliteRow.RowId = Row.MinRowId;
			SatelliteRow.RowVersion = Row.MinVersion;
			MoveToRow(SatelliteRow);
			SatelliteRow.RowId = CurrentRow.RowId;
			SatelliteRow.RowVersion = CurrentRow.RowVersion;
			if (SatelliteRow - CurrentRow != 0)
				return;
			PrepareEditStatus();
			SaveRow();
			FillRowData(SatelliteRow, type, foreignReference, name, objectId, reference, options, status, scriptValue, description, binary, blob);
			UpdateRow(commit);
		}

		private void UpdateFullObjectEntry(bool commit, Database.VdbObjects oldType, ulong oldForeignReference, string oldName, ulong oldObjectId, ulong oldReference, long oldOptions, bool oldStatus, string oldScriptValue, string oldDescription, byte[] oldBinary, byte[] oldBlob, Database.VdbObjects newType, ulong newForeignReference, string newName, ulong newObjectId, ulong newReference, long newOptions, bool newStatus, string newScriptValue, string newDescription, byte[] newBinary, byte[] newBlob)
		{
			FillRowData(SatelliteRow, oldType, oldForeignReference, oldName, oldObjectId, oldReference, oldOptions, oldStatus, oldScriptValue, oldDescription, oldBinary, oldBlob);
			SatelliteRow.RowId = Row.MinRowId;
			SatelliteRow.RowVersion = Row.MinVersion;
			MoveToRow(SatelliteRow);
			SatelliteRow.RowId = CurrentRow.RowId;
			SatelliteRow.RowVersion = CurrentRow.RowVersion;
			if (SatelliteRow - CurrentRow != 0)
				return;
			PrepareEditStatus();
			SaveRow();
			FillRowData(SatelliteRow, newType, newForeignReference, newName, newObjectId, newReference, newOptions, newStatus, newScriptValue, newDescription, newBinary, newBlob);
			UpdateRow(commit);
		}

		private void DeleteInScope(Database.VdbObjects type, ulong referencingObject, string name, bool commit)
		{
			if (name != null)
				name = Row.Column.FixName(name);
			bool flag = false;
			try
			{
				TopRow.InitTop();
				BottomRow.InitBottom();
				TopRow[typeIndex].Value = type;
				TopRow[foreignReferenceIndex].Value = (long)referencingObject;
				TopRow[nameIndex].Value = name;
				BottomRow[typeIndex].Value = type;
				BottomRow[foreignReferenceIndex].Value = (long)referencingObject;
				if (name != null)
					BottomRow[nameIndex].Value = name;
				MoveToRow(TopRow);
				while (!EndOfSet)
					DeleteRow(false);
				flag = true;
			}
			finally
			{
				FinalizeChanges(!flag, commit);
				TopRow.InitTop();
				BottomRow.InitBottom();
			}
		}

		private ulong GetTableId(string name, out string description, out Table.TableType type, bool raiseError)
		{
			FillRowData(CurrentRow, Database.VdbObjects.Table, Row.EmptyReference, name, 0UL, Row.EmptyReference, 0L, true, null, null, null, null);
			CurrentRow.RowId = Row.MinRowId;
			CurrentRow.RowVersion = Row.MinVersion;
			MoveToRow(CurrentRow);
			ulong emptyReference = Row.EmptyReference;
			description = null;
			type = Table.TableType.Default;
			try
			{
				Row.Column column = CurrentRow[typeIndex];
				if (column.IsNull || (int)column.Value != 1 || !Database.DatabaseObject.EqualNames(name, (string)CurrentRow[nameIndex].Value))
					return emptyReference;
				emptyReference = (ulong)(long)CurrentRow[objectIdIndex].Value;
				if ((long)emptyReference == (long)Row.EmptyReference || emptyReference == 0UL)
				{
					emptyReference = Row.EmptyReference;
					return emptyReference;
				}
				description = (string)CurrentRow[descriptionIndex].Value;
				type = (Table.TableType)(long)CurrentRow[optionIndex].Value;
				return emptyReference;
			}
			finally
			{
				if ((long)emptyReference == (long)Row.EmptyReference && raiseError)
					throw new VistaDBException(126, name);
			}
		}

		private void FillRowData(Row key, Database.VdbObjects type, ulong foreignReference, string name, ulong objectId, ulong reference, long options, bool status, string scriptValue, string description, byte[] binary, byte[] blob)
		{
			if (name != null)
				name = Row.Column.FixName(name);
			key[typeIndex].Value = type;
			key[foreignReferenceIndex].Value = (long)foreignReference;
			key[nameIndex].Value = name;
			key[objectIdIndex].Value = (long)objectId;
			key[referenceIndex].Value = (long)reference;
			key[optionIndex].Value = options;
			key[statusIndex].Value = status;
			key[scriptValueIndex].Value = scriptValue;
			key[descriptionIndex].Value = description;
			key[extraBinaryIndex].Value = binary;
			key[extraBlobDataIndex].Value = blob;
		}

		private void TraverseDiskObjects(Database.VdbObjects type, ulong referencingObject, Database.TraverseJobDelegate job, params object[] hints)
		{
			try
			{
				TopRow.InitTop();
				BottomRow.InitBottom();
				TopRow[typeIndex].Value = type;
				TopRow[foreignReferenceIndex].Value = (long)referencingObject;
				BottomRow[typeIndex].Value = type;
				BottomRow[foreignReferenceIndex].Value = type == Database.VdbObjects.Relationship || type == Database.VdbObjects.CLRTrigger ? (long)hints[1] : (long)referencingObject;
				MoveToRow(TopRow);
				while (!EndOfSet && job(CurrentRow, hints))
					NextRow();
			}
			finally
			{
				TopRow.InitTop();
				BottomRow.InitBottom();
			}
		}

		private bool LoadDatabaseDescriptionJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[descriptionIndex].Value;
			((ArrayList)parms[0]).Add(str);
			return true;
		}

		private bool LoadViewsJob(Row currentRow, params object[] parms)
		{
			string viewName = (string)currentRow[nameIndex].Value;
			string script = (string)currentRow[scriptValueIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			byte[] scriptExtension = (byte[])currentRow[extraBlobDataIndex].Value;
			((Database.ViewList)parms[0]).Add(viewName, script, scriptExtension, description);
			return true;
		}

		private bool LoadSqlStoredProceduresJob(Row currentRow, params object[] parms)
		{
			string name = (string)currentRow[nameIndex].Value;
			byte[] scriptData = (byte[])currentRow[extraBlobDataIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			bool status = (bool)currentRow[statusIndex].Value;
			((Database.StoredProcedureCollection)parms[0]).AddProcedure(name, scriptData, description, status);
			return true;
		}

		private bool LoadSqlFunctionsJob(Row currentRow, params object[] parms)
		{
			string name = (string)currentRow[nameIndex].Value;
			byte[] scriptData = (byte[])currentRow[extraBlobDataIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			bool status = (bool)currentRow[statusIndex].Value;
			ulong num = (ulong)(long)currentRow[optionIndex].Value;
			((Database.UdfCollection)parms[0]).AddFunction(name, scriptData, num == 0UL, description, status);
			return true;
		}

		private bool LoadClrProceduresJob(Row currentRow, params object[] parms)
		{
			string name = (string)currentRow[nameIndex].Value;
			string fullSignature = (string)currentRow[scriptValueIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			byte[] assemblyFullName = (byte[])currentRow[extraBinaryIndex].Value;
			bool status = (bool)currentRow[statusIndex].Value;
			Database.ClrProcedureCollection parm1 = (Database.ClrProcedureCollection)parms[0];
			int parm2 = (int)parms[2];
			parm1.AddProcedure(name, fullSignature, description, assemblyFullName, status);
			return true;
		}

		private bool LoadClrTriggersJob(Row currentRow, params object[] parms)
		{
			string name = (string)currentRow[nameIndex].Value;
			string fullSignature = (string)currentRow[scriptValueIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			byte[] assemblyFullName = (byte[])currentRow[extraBinaryIndex].Value;
			bool status = (bool)currentRow[statusIndex].Value;
			Database.ClrTriggerCollection parm1 = (Database.ClrTriggerCollection)parms[0];
			int parm2 = (int)parms[2];
			long option = (long)currentRow[optionIndex].Value;
			ulong reference = (ulong)(long)currentRow[foreignReferenceIndex].Value;
			parm1.AddTrigger(name, fullSignature, description, assemblyFullName, status, reference, option);
			return true;
		}

		private bool LoadAssembliesJob(Row currentRow, params object[] parms)
		{
			string assemblyName = (string)currentRow[nameIndex].Value;
			byte[] assemblyBody = (byte[])currentRow[extraBlobDataIndex].Value;
			string script = (string)currentRow[scriptValueIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			((Database.AssemblyCollection)parms[0]).AddAssembly(assemblyName, assemblyBody, script, description, ParentConnection);
			return true;
		}

		private bool LoadTablesJob(Row currentRow, params object[] parms)
		{
			string key = (string)currentRow[nameIndex].Value;
			ulong num = (ulong)(long)currentRow[objectIdIndex].Value;
			((Database.TableIdMap)parms[0]).Add(key, num);
			return true;
		}

		private bool LoadColumnsJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[nameIndex].Value;
			ulong num1 = (ulong)(long)currentRow[optionIndex].Value;
			int num2 = (int)(long)currentRow[objectIdIndex].Value;
			string caption = (string)currentRow[scriptValueIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			bool packed = ((long)num1 & 8L) == 8L;
			bool encrypted = ((long)num1 & 4L) == 4L;
			bool readOnly = ((long)num1 & 2L) == 2L;
			bool allowNull = ((long)num1 & 1L) == 1L;
			bool flag = ((long)num1 & 16L) == 16L;
			bool syncColumn = ((long)num1 & 32L) == 32L;
			ulong num3 = num1 >> 8;
			int codePage = (int)((long)num3 & ushort.MaxValue);
			ulong num4 = num3 >> 16;
			short maxLength = (short)(int)((long)num4 & uint.MaxValue);
			VistaDBType type = (VistaDBType)(byte)(num4 >> 32 & byte.MaxValue);
			Row parm1 = (Row)parms[0];
			bool parm2 = (bool)parms[1];
			Row.Column emptyColumnInstance = CreateEmptyColumnInstance(type, maxLength, codePage, parm2, syncColumn);
			if (emptyColumnInstance == null)
				throw new VistaDBException(153, str);
			emptyColumnInstance.AssignAttributes(str, allowNull, readOnly, encrypted, packed, caption, description);
			parm1.AppendColumn(emptyColumnInstance);
			emptyColumnInstance.RowIndex = num2;
			emptyColumnInstance.Descending = flag;
			return true;
		}

		private bool LoadIdentitiesJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[nameIndex].Value;
			string stepExpression = (string)currentRow[scriptValueIndex].Value;
			Table.TableSchema.IdentityCollection parm1 = (Table.TableSchema.IdentityCollection)parms[0];
			string parm2 = (string)parms[1];
			Table.TableSchema.IdentityCollection.IdentityInformation identityInformation = new Table.TableSchema.IdentityCollection.IdentityInformation((Table.TableSchema.IdentityCollection.IdentityInformation.GetSeedValue)parms[2], parm2, str, string.Empty, stepExpression);
			parm1.Add(str, identityInformation);
			return true;
		}

		private bool LoadDefaultValuesJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[nameIndex].Value;
			string expression = (string)currentRow[scriptValueIndex].Value;
			bool useInUpdate = (long)currentRow[optionIndex].Value == 1L;
			string description = (string)currentRow[descriptionIndex].Value;
			Table.TableSchema.DefaultValueCollection.DefaultValueInformation valueInformation = new Table.TableSchema.DefaultValueCollection.DefaultValueInformation(str, expression, useInUpdate, description);
			((Dictionary<string, IVistaDBDefaultValueInformation>)parms[0]).Add(str, valueInformation);
			return true;
		}

		private bool LoadConstraintsJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[nameIndex].Value;
			string expression = (string)currentRow[scriptValueIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			int option = (int)(long)currentRow[optionIndex].Value;
			Table.TableSchema.ConstraintCollection.ConstraintInformation constraintInformation = new Table.TableSchema.ConstraintCollection.ConstraintInformation(str, expression, description, option);
			((Dictionary<string, IVistaDBConstraintInformation>)parms[0]).Add(str, constraintInformation);
			return true;
		}

		private bool LoadRelationshipsJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[nameIndex].Value;
			ulong foreignTableId = (ulong)(long)currentRow[foreignReferenceIndex].Value;
			ulong primaryTableId = (ulong)(long)currentRow[referenceIndex].Value;
			string foreignKey = (string)currentRow[scriptValueIndex].Value;
			int options = (int)(long)currentRow[optionIndex].Value;
			string description = (string)currentRow[descriptionIndex].Value;
			IVistaDBRelationshipInformation relationshipInformation = new Database.RelationshipCollection.RelationshipInformation(str, foreignTableId, foreignKey, primaryTableId, options, description);
			((Dictionary<string, IVistaDBRelationshipInformation>)parms[0]).Add(str, relationshipInformation);
			return true;
		}

		private bool LoadIndexesJob(Row currentRow, params object[] parms)
		{
			string str = (string)currentRow[nameIndex].Value;
			uint signature = (uint)(long)currentRow[optionIndex].Value;
			ulong position = (ulong)(long)currentRow[objectIdIndex].Value;
			string keyExpression = (string)currentRow[scriptValueIndex].Value;
			bool unique = VistaDB.Engine.Core.Indexing.Index.IndexHeader.IsUnique(signature);
			bool primary = VistaDB.Engine.Core.Indexing.Index.IndexHeader.IsPrimary(signature);
			bool descend = false;
			bool sparse = VistaDB.Engine.Core.Indexing.Index.IndexHeader.IsSparse(signature);
			bool fk_constraint = VistaDB.Engine.Core.Indexing.Index.IndexHeader.IsForeignKey(signature);
			bool fts = VistaDB.Engine.Core.Indexing.Index.IndexHeader.IsFts(signature);
			bool temporary = false;
			byte[] keyStruct = (byte[])currentRow[extraBinaryIndex].Value;
			Table.TableSchema.IndexCollection.IndexInformation indexInformation = new Table.TableSchema.IndexCollection.IndexInformation("", str, keyExpression, unique, primary, descend, sparse, fk_constraint, fts, temporary, position, keyStruct);
			((Dictionary<string, IVistaDBIndexInformation>)parms[0]).Add(str, indexInformation);
			return true;
		}

		internal void GetIndexes(ulong tableId, IVistaDBIndexCollection indexes)
		{
			((Dictionary<string, IVistaDBIndexInformation>)indexes).Clear();
			TraverseDiskObjects(Database.VdbObjects.Index, tableId, new Database.TraverseJobDelegate(LoadIndexesJob), (object)indexes);
		}

		private void GetIndexes(ulong tableId, IVistaDBTableSchema schema)
		{
			IVistaDBIndexCollection indexes = schema.Indexes;
			GetIndexes(tableId, indexes);
			foreach (Table.TableSchema.IndexCollection.IndexInformation indexInformation in indexes.Values)
				indexInformation.MakeUpKeyExpression(schema);
		}

		private void GetIdentities(ulong tableId, IVistaDBIdentityCollection identities, string tableName)
		{
			((Dictionary<string, IVistaDBIdentityInformation>)identities).Clear();
			TraverseDiskObjects(Database.VdbObjects.Identity, tableId, new Database.TraverseJobDelegate(LoadIdentitiesJob), identities, tableName, new Table.TableSchema.IdentityCollection.IdentityInformation.GetSeedValue(GetSeedValue));
		}

		private string GetSeedValue(string tableName, string columnName)
		{
			LockStorage();
			try
			{
				ITable tbl = null;
				try
				{
					tbl = OpenClone(tableName, IsReadOnly);
					return tbl.LastTableIdentity[columnName].ToString();
				}
				finally
				{
					ReleaseClone(tbl);
				}
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		private void GetDefaultValues(ulong tableId, Table.TableSchema.DefaultValueCollection defaults)
		{
			defaults.Clear();
			TraverseDiskObjects(Database.VdbObjects.DefaultValue, tableId, new Database.TraverseJobDelegate(LoadDefaultValuesJob), (object)defaults);
		}

		private void GetConstraints(ulong tableId, Table.TableSchema.ConstraintCollection constraints)
		{
			constraints.Clear();
			TraverseDiskObjects(Database.VdbObjects.Constraint, tableId, new Database.TraverseJobDelegate(LoadConstraintsJob), (object)constraints);
		}

		private void GetClrProcedures(ulong tableId, IVistaDBClrProcedureCollection procedures, string tableName)
		{
			procedures.Clear();
			TraverseDiskObjects(Database.VdbObjects.CLRProcedure, tableId, new Database.TraverseJobDelegate(LoadClrProceduresJob), procedures, (long)tableId, VdbObjects.CLRTrigger);
		}

		private void GetClrTriggers(ulong tableId, Database.ClrTriggerCollection triggers, string tableName)
		{
			triggers.Clear();
			TraverseDiskObjects(Database.VdbObjects.CLRTrigger, tableId, new Database.TraverseJobDelegate(LoadClrTriggersJob), triggers, (long)tableId, VdbObjects.CLRTrigger);
			foreach (Database.ClrTriggerCollection.ClrTriggerInformation triggerInformation in triggers.Values)
				triggerInformation.ParentTable = tableName;
		}

		private void GetRelationships(ulong foreignTableId, Database.RelationshipCollection relationships)
		{
			relationships.Clear();
			TraverseDiskObjects(Database.VdbObjects.Relationship, foreignTableId, new Database.TraverseJobDelegate(LoadRelationshipsJob), relationships, foreignTableId == 0UL ? long.MaxValue : (long)foreignTableId);
			CompleteRelationshipInfo(relationships);
		}

		private void CompleteRelationshipInfo(Database.RelationshipCollection relationships)
		{
			if (relationships.Count == 0)
				return;
			Database.TableIdMap tableIdMap = GetTableIdMap();
			foreach (Database.RelationshipCollection.RelationshipInformation relationshipInformation in relationships.Values)
			{
				relationshipInformation.ForeignTable = tableIdMap.GetTableNameFromID(relationshipInformation.ForeignTableId);
				relationshipInformation.PrimaryTable = tableIdMap.GetTableNameFromID(relationshipInformation.PrimaryTableId);
			}
		}

		private ulong PrepareColumnOption(IVistaDBColumnAttributes column)
		{
			ulong num = (((ulong)column.Type << 32 | (column.ExtendedType || column.FixedType ? 0UL : (uint)column.MaxLength)) << 16 | (ushort)column.CodePage) << 8;
			if (column.AllowNull)
				num |= 1UL;
			if (column.ReadOnly)
				num |= 2UL;
			if (column.Encrypted)
				num |= 4UL;
			if (column.Packed)
				num |= 8UL;
			if (((Row.Column)column).Descending)
				num |= 16UL;
			if (((Row.Column)column).IsSync)
				num |= 32UL;
			return num;
		}

		private void UpdatePkColumnsFlags(ulong storageId, List<Row.Column> pkColumns, bool commit)
		{
			for (int index = 0; index < pkColumns.Count; ++index)
			{
				if (pkColumns[index].AllowNull)
				{
					pkColumns[index].AssignAttributes(pkColumns[index].Name, false, pkColumns[index].ReadOnly, pkColumns[index].Encrypted, pkColumns[index].Packed);
					UpdateColumn(pkColumns[index], pkColumns[index], storageId, commit);
				}
			}
		}

		private DataColumn[] KeyStructure(DataTable table, IVistaDBKeyColumn[] keyStructure)
		{
			DataColumn[] dataColumnArray = new DataColumn[keyStructure.Length];
			for (int index = 0; index < dataColumnArray.Length; ++index)
			{
				IVistaDBKeyColumn vistaDbKeyColumn = keyStructure[index];
				dataColumnArray[index] = table.Columns[vistaDbKeyColumn.RowIndex];
			}
			return dataColumnArray;
		}

		private void FillOutDatabaseData(DataSet database)
		{
			database.EnforceConstraints = false;
			foreach (DataTable table in database.Tables)
			{
				if (transferList.Contains(table.TableName.ToUpper(Culture)))
				{
					ITable tbl = OpenClone(table.TableName, IsReadOnly, false);
					try
					{
						tbl.First();
						while (!tbl.EndOfTable)
						{
							IRow currentRow = tbl.CurrentRow;
							DataRow row = table.NewRow();
							foreach (Row.Column column in currentRow)
								row[column.Name] = column.IsNull ? DBNull.Value : column.Value;
							table.Rows.Add(row);
							tbl.Next();
						}
					}
					finally
					{
						ReleaseClone(tbl);
					}
				}
			}
		}

		private void FillOutDatabaseSchema(DataSet database)
		{
			Dictionary<string, DataColumn[]> dictionary1 = new Dictionary<string, DataColumn[]>(StringComparer.OrdinalIgnoreCase);
			Dictionary<string, DataColumn[]> dictionary2 = new Dictionary<string, DataColumn[]>(StringComparer.OrdinalIgnoreCase);
			string str = ":";
			database.Namespace = "VistaDB";
			database.Locale = Culture;
			PropertyCollection extendedProperties1 = database.ExtendedProperties;
			extendedProperties1.Add("sensitivity", CaseSensitive);
			extendedProperties1.Add("specification", Header.DatabaseFileSchemaVersion);
			foreach (string key in GetTableIdMap().Keys)
			{
				if (transferList.Contains(key.ToUpper(Culture)))
				{
					IVistaDBTableSchema tableSchema = GetTableSchema(key, true);
					DataTable table1 = new DataTable(key);
					database.Tables.Add(table1);
					PropertyCollection extendedProperties2 = table1.ExtendedProperties;
					if (tableSchema.Description != null)
						extendedProperties2.Add("description", tableSchema.Description);
					if (tableSchema.IsSystemTable)
						extendedProperties2.Add("tableType", ((Table.TableSchema)tableSchema).Type);
					foreach (IVistaDBColumnAttributes columnAttributes in tableSchema)
					{
						DataColumn column = new DataColumn(columnAttributes.Name, columnAttributes.SystemType);
						table1.Columns.Add(column);
						column.AllowDBNull = columnAttributes.AllowNull;
						column.ReadOnly = columnAttributes.ReadOnly;
						if (columnAttributes.Caption != null)
							column.Caption = columnAttributes.Caption;
						PropertyCollection extendedProperties3 = column.ExtendedProperties;
						extendedProperties3.Add("vdbType", columnAttributes.Type);
						if (columnAttributes.Encrypted)
							extendedProperties3.Add("encrypted", columnAttributes.Encrypted);
						if (columnAttributes.Packed)
							extendedProperties3.Add("packed", columnAttributes.Packed);
						if (columnAttributes.IsSystem)
							extendedProperties3.Add("sync", true);
						if (columnAttributes.Description != null)
							extendedProperties3.Add("description", columnAttributes.Description);
						VistaDBType type = columnAttributes.Type;
						if (((Row.Column)columnAttributes).InternalType == VistaDBType.NChar)
						{
							extendedProperties3.Add("codepage", columnAttributes.CodePage);
							int maxLength = columnAttributes.MaxLength;
							if (type == VistaDBType.Text || type == VistaDBType.NText)
								maxLength *= PageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE;
							column.MaxLength = maxLength;
						}
					}
					if (tableSchema.Indexes.Count > 0)
					{
						DataSet dataSet = new DataSet("indexes");
						dataSet.Locale = Culture;
						DataTable table2 = new DataTable(dataSet.DataSetName + str + table1.TableName);
						dataSet.Tables.Add(table2);
						bool flag = false;
						foreach (IVistaDBIndexInformation indexInformation in tableSchema.Indexes.Values)
						{
							if (!indexInformation.Temporary)
							{
								if (indexInformation.FKConstraint)
								{
									dictionary2.Add(indexInformation.Name + str + table1.TableName, KeyStructure(table1, indexInformation.KeyStructure));
								}
								else
								{
									bool primary = indexInformation.Primary;
									if (primary || indexInformation.Unique)
									{
										DataColumn[] columns = KeyStructure(table1, indexInformation.KeyStructure);
										if (primary)
											dictionary1.Add(table1.TableName, columns);
										UniqueConstraint uniqueConstraint = new UniqueConstraint(indexInformation.Name, columns, primary);
										uniqueConstraint.ExtendedProperties.Add("key", indexInformation.KeyExpression);
										table1.Constraints.Add(uniqueConstraint);
									}
									else
									{
										flag = true;
										DataColumn column = new DataColumn(indexInformation.Name, System.Type.Delimiter.GetType());
										table2.Columns.Add(column);
										PropertyCollection extendedProperties3 = column.ExtendedProperties;
										extendedProperties3.Add("key", indexInformation.KeyExpression);
										if (indexInformation.FullTextSearch)
											extendedProperties3.Add("fts", true);
										if (indexInformation.Unique)
											extendedProperties3.Add("unique", true);
									}
								}
							}
						}
						if (flag)
							extendedProperties2.Add(dataSet.DataSetName, dataSet.GetXmlSchema());
					}
					if (tableSchema.Identities.Count > 0)
					{
						DataSet dataSet = new DataSet("identities");
						dataSet.Locale = Culture;
						DataTable table2 = new DataTable(dataSet.DataSetName + str + table1.TableName);
						dataSet.Tables.Add(table2);
						foreach (IVistaDBIdentityInformation identityInformation in tableSchema.Identities.Values)
						{
							DataColumn column = new DataColumn(identityInformation.ColumnName, System.Type.Delimiter.GetType());
							table2.Columns.Add(column);
							PropertyCollection extendedProperties3 = column.ExtendedProperties;
							extendedProperties3.Add("seedExpression", "1");
							extendedProperties3.Add("stepExpression", identityInformation.StepExpression);
						}
						extendedProperties2.Add(dataSet.DataSetName, dataSet.GetXmlSchema());
					}
					if (tableSchema.Constraints.Count > 0)
					{
						DataSet dataSet = new DataSet("constraints");
						dataSet.Locale = Culture;
						DataTable table2 = new DataTable(dataSet.DataSetName + str + table1.TableName);
						dataSet.Tables.Add(table2);
						foreach (IVistaDBConstraintInformation constraint in tableSchema.Constraints)
						{
							DataColumn column = new DataColumn(constraint.Name, System.Type.Delimiter.GetType());
							table2.Columns.Add(column);
							PropertyCollection extendedProperties3 = column.ExtendedProperties;
							extendedProperties3.Add("expression", constraint.Expression);
							if (constraint.Description != null)
								extendedProperties3.Add("description", constraint.Description);
							if (constraint.AffectsInsertion)
								extendedProperties3.Add("insertion", true);
							if (constraint.AffectsUpdate)
								extendedProperties3.Add("update", true);
							if (constraint.AffectsDelete)
								extendedProperties3.Add("delete", true);
						}
						extendedProperties2.Add(dataSet.DataSetName, dataSet.GetXmlSchema());
					}
					if (tableSchema.DefaultValues.Count > 0)
					{
						DataSet dataSet = new DataSet("defaultValues");
						dataSet.Locale = Culture;
						DataTable table2 = new DataTable(dataSet.DataSetName + str + table1.TableName);
						dataSet.Tables.Add(table2);
						foreach (IVistaDBDefaultValueInformation valueInformation in tableSchema.DefaultValues.Values)
						{
							DataColumn column = new DataColumn(valueInformation.ColumnName, System.Type.Delimiter.GetType());
							table2.Columns.Add(column);
							PropertyCollection extendedProperties3 = column.ExtendedProperties;
							extendedProperties3.Add("expression", valueInformation.Expression);
							if (valueInformation.UseInUpdate)
								extendedProperties3.Add("update", true);
							if (valueInformation.Description != null)
								extendedProperties3.Add("description", valueInformation.Description);
						}
						extendedProperties2.Add(dataSet.DataSetName, dataSet.GetXmlSchema());
					}
				}
			}
			foreach (IVistaDBRelationshipInformation relationshipInformation in GetRelationships().Values)
			{
				if (transferList.Contains(relationshipInformation.PrimaryTable) && transferList.Contains(relationshipInformation.ForeignTable))
				{
					DataTable table = database.Tables[relationshipInformation.ForeignTable];
					DataColumn[] childColumns = dictionary2[relationshipInformation.Name + str + relationshipInformation.ForeignTable];
					DataColumn[] parentColumns = dictionary1[relationshipInformation.PrimaryTable];
					System.Data.ForeignKeyConstraint childKeyConstraint = database.Relations.Add(relationshipInformation.Name, parentColumns, childColumns, true).ChildKeyConstraint;
					childKeyConstraint.UpdateRule = (System.Data.Rule)relationshipInformation.UpdateIntegrity;
					childKeyConstraint.DeleteRule = (System.Data.Rule)relationshipInformation.DeleteIntegrity;
					childKeyConstraint.ExtendedProperties.Add("description", relationshipInformation.Description);
				}
			}
			DataSet dataSet1 = new DataSet("databaseObjects");
			dataSet1.Namespace = database.Namespace;
			dataSet1.Locale = database.Locale;
			IViewList viewList = LoadViews();
			if (viewList != null && viewList.Count > 0)
			{
				DataTable table = new DataTable("views");
				dataSet1.Tables.Add(table);
				table.Columns.Add(new DataColumn("name", typeof(string)));
				table.Columns.Add(new DataColumn("expression", typeof(string)));
				foreach (IView view in viewList)
				{
					DataRow row = table.NewRow();
					row[0] = view.Name;
					row[1] = view.Expression;
					table.Rows.Add(row);
				}
			}
			IVistaDBAssemblyCollection assemblyCollection = LoadAssemblies(false);
			if (assemblyCollection != null && assemblyCollection.Count > 0)
			{
				DataTable table = new DataTable("assemblies");
				dataSet1.Tables.Add(table);
				table.Columns.Add(new DataColumn("name", typeof(string)));
				table.Columns.Add(new DataColumn("fullName", typeof(string)));
				table.Columns.Add(new DataColumn("runtimeVersion", typeof(string)));
				table.Columns.Add(new DataColumn("description", typeof(string)));
				table.Columns.Add(new DataColumn("coffImage", typeof(byte[])));
				table.Columns.Add(new DataColumn("vistadbVersion", typeof(string)));
				foreach (IVistaDBAssemblyInformation assemblyInformation in assemblyCollection)
				{
					DataRow row = table.NewRow();
					row[0] = assemblyInformation.Name;
					row[1] = assemblyInformation.FullName;
					row["runtimeVersion"] = assemblyInformation.ImageRuntimeVersion;
					row[3] = assemblyInformation.Description;
					row[4] = assemblyInformation.COFFImage;
					row["vistadbVersion"] = assemblyInformation.VistaDBRuntimeVersion;
					table.Rows.Add(row);
				}
			}
			IVistaDBClrProcedureCollection procedureCollection = LoadClrProcedureObjects();
			if (procedureCollection != null && procedureCollection.Count > 0)
			{
				DataTable table = new DataTable("clrProcs");
				dataSet1.Tables.Add(table);
				table.Columns.Add(new DataColumn("name", typeof(string)));
				table.Columns.Add(new DataColumn("fullClrName", typeof(string)));
				table.Columns.Add(new DataColumn("assemblyName", typeof(string)));
				table.Columns.Add(new DataColumn("signature", typeof(string)));
				table.Columns.Add(new DataColumn("description", typeof(string)));
				foreach (IVistaDBClrProcedureInformation procedureInformation in procedureCollection)
				{
					DataRow row = table.NewRow();
					row[0] = procedureInformation.Name;
					row[1] = procedureInformation.FullHostedName;
					row[2] = procedureInformation.AssemblyName;
					row[3] = procedureInformation.Signature;
					row[4] = procedureInformation.Description;
					table.Rows.Add(row);
				}
			}
			if (dataSet1.Tables.Count <= 0)
				return;
			using (MemoryStream memoryStream = new MemoryStream())
			{
				dataSet1.WriteXml(memoryStream, XmlWriteMode.WriteSchema);
				extendedProperties1.Add(dataSet1.DataSetName, Encoding.GetEncoding(0).GetString(memoryStream.ToArray(), 0, (int)memoryStream.Length));
			}
		}

		private void FillInDatabaseSchema(DataSet database)
		{
			Dictionary<string, Table.TableSchema> dictionary = new Dictionary<string, Table.TableSchema>(StringComparer.OrdinalIgnoreCase);
			foreach (DataTable table in database.Tables)
			{
				PropertyCollection extendedProperties1 = table.ExtendedProperties;
				Table.TableType type1 = Table.ParseType((string)extendedProperties1["tableType"]);
				IVistaDBTableSchema schema = new Table.TableSchema(table.TableName, type1, (string)extendedProperties1["description"], 0UL, this);
				foreach (DataColumn column in table.Columns)
				{
					PropertyCollection extendedProperties2 = column.ExtendedProperties;
					VistaDBType type2 = Row.Column.ParseType((string)extendedProperties2["vdbType"]);
					object obj1 = extendedProperties2["encrypted"];
					bool encrypted = obj1 != null && bool.Parse((string)obj1);
					object obj2 = extendedProperties2["packed"];
					bool packed = obj2 != null && bool.Parse((string)obj2);
					bool flag = extendedProperties2["sync"] != null;
					string description = (string)extendedProperties2["description"];
					switch (type2)
					{
						case VistaDBType.Char:
						case VistaDBType.VarChar:
						case VistaDBType.Text:
							string extendedProperty1 = (string)column.ExtendedProperties["codepage"];
							schema.AddColumn(column.ColumnName, type2, column.MaxLength, extendedProperty1 == null ? 0 : int.Parse(extendedProperty1));
							break;
						case VistaDBType.NChar:
						case VistaDBType.NVarChar:
							string extendedProperty2 = (string)column.ExtendedProperties["codepage"];
							schema.AddColumn(column.ColumnName, type2, column.MaxLength, extendedProperty2 == null ? NCharColumn.DefaultUnicode : int.Parse(extendedProperty2));
							break;
						default:
							if (flag)
							{
								((Table.TableSchema)schema).AddSyncColumn(column.ColumnName, type2);
								break;
							}
							schema.AddColumn(column.ColumnName, type2);
							break;
					}
					schema.DefineColumnAttributes(column.ColumnName, column.AllowDBNull, column.ReadOnly, encrypted, packed, column.Caption, description);
				}
				foreach (string key in extendedProperties1.Keys)
				{
					DataSet dataSet = new DataSet(key);
					if (!Database.DatabaseObject.EqualNames(dataSet.DataSetName, "description") && !Database.DatabaseObject.EqualNames(dataSet.DataSetName, "tableType"))
					{
						using (Stream stream = new MemoryStream(Encoding.Unicode.GetBytes((string)extendedProperties1[key])))
						{
							int num = (int)dataSet.ReadXml(stream);
						}
						if (Database.DatabaseObject.EqualNames(dataSet.DataSetName, "indexes"))
						{
							foreach (DataColumn column in dataSet.Tables[0].Columns)
							{
								PropertyCollection extendedProperties2 = column.ExtendedProperties;
								string keyExpression = (string)extendedProperties2["key"];
								string str1 = (string)extendedProperties2["unique"];
								bool primary = str1 != null && bool.Parse(str1);
								string str2 = (string)extendedProperties2["fts"];
								if (str2 != null)
									bool.Parse(str2);
								schema.DefineIndex(column.ColumnName, keyExpression, primary, false);
							}
						}
						if (Database.DatabaseObject.EqualNames(dataSet.DataSetName, "identities"))
						{
							foreach (DataColumn column in dataSet.Tables[0].Columns)
							{
								PropertyCollection extendedProperties2 = column.ExtendedProperties;
								string str = (string)extendedProperties2["seedExpression"];
								string stepExpression = (string)extendedProperties2["stepExpression"];
								schema.DefineIdentity(column.ColumnName, str == null ? "0" : str, stepExpression);
							}
						}
						else if (Database.DatabaseObject.EqualNames(dataSet.DataSetName, "constraints"))
						{
							foreach (DataColumn column in dataSet.Tables[0].Columns)
							{
								PropertyCollection extendedProperties2 = column.ExtendedProperties;
								string str1 = (string)extendedProperties2["insertion"];
								bool insert = str1 != null && bool.Parse(str1);
								string str2 = (string)extendedProperties2["update"];
								bool update = str2 != null && bool.Parse(str2);
								string str3 = (string)extendedProperties2["delete"];
								bool delete = str3 != null && bool.Parse(str3);
								schema.DefineConstraint(column.ColumnName, (string)extendedProperties2["expression"], (string)extendedProperties2["description"], insert, update, delete);
							}
						}
						else if (Database.DatabaseObject.EqualNames(dataSet.DataSetName, "defaultValues"))
						{
							foreach (DataColumn column in dataSet.Tables[0].Columns)
							{
								PropertyCollection extendedProperties2 = column.ExtendedProperties;
								string str = (string)extendedProperties2["update"];
								bool useInUpdate = str != null && bool.Parse(str);
								schema.DefineDefaultValue(column.ColumnName, (string)extendedProperties2["expression"], useInUpdate, (string)extendedProperties2["description"]);
							}
						}
					}
				}
				foreach (System.Data.Constraint constraint in table.Constraints)
				{
					if (!(constraint is System.Data.ForeignKeyConstraint) && constraint is UniqueConstraint && Database.DatabaseObject.EqualNames(table.TableName, constraint.Table.TableName))
					{
						string extendedProperty = (string)constraint.ExtendedProperties["key"];
						schema.DefineIndex(constraint.ConstraintName, extendedProperty, ((UniqueConstraint)constraint).IsPrimaryKey, true);
					}
				}
				CreateTable(schema, !IsShared, IsReadOnly, true).Dispose();
				dictionary.Add(table.TableName, (Table.TableSchema)schema);
			}
			if (database.Relations.Count > 0)
			{
				Database.RelationshipCollection relationships = new Database.RelationshipCollection();
				foreach (DataRelation relation in database.Relations)
				{
					string tableName1 = relation.ParentTable.TableName;
					string tableName2 = relation.ChildTable.TableName;
					Table.TableSchema tableSchema1 = dictionary[tableName1];
					Table.TableSchema tableSchema2 = dictionary[tableName2];
					string foreignKey = string.Empty;
					int num = 0;
					foreach (DataColumn childColumn in relation.ChildColumns)
					{
						++num;
						foreignKey = foreignKey + childColumn.ColumnName + (num == relation.ChildColumns.Length ? "" : ";");
					}
					System.Data.ForeignKeyConstraint childKeyConstraint = relation.ChildKeyConstraint;
					relationships.Add(relation.RelationName, new Database.RelationshipCollection.RelationshipInformation(relation.RelationName, tableSchema2.HeaderPosition, foreignKey, tableSchema1.HeaderPosition, Database.RelationshipCollection.RelationshipInformation.MakeOptions((VistaDBReferentialIntegrity)childKeyConstraint.UpdateRule, (VistaDBReferentialIntegrity)childKeyConstraint.DeleteRule), (string)childKeyConstraint.ExtendedProperties["description"])
					{
						ForeignTable = tableName2,
						PrimaryTable = tableName1
					});
				}
				CreateRelationships(relationships);
			}
			PropertyCollection extendedProperties = database.ExtendedProperties;
			foreach (string key in extendedProperties.Keys)
			{
				if (Database.DatabaseObject.EqualNames(key, "databaseObjects"))
				{
					DataSet dataSet = new DataSet(key);
					using (Stream stream = new MemoryStream(Encoding.GetEncoding(0).GetBytes((string)extendedProperties[key])))
					{
						int num = (int)dataSet.ReadXml(stream);
					}
					foreach (DataTable table in dataSet.Tables)
					{
						if (Database.DatabaseObject.EqualNames(table.TableName, "views"))
						{
							foreach (DataRow row in (InternalDataCollectionBase)table.Rows)
							{
								string name = (string)row[0];
								string str = (string)row[1];
								IView view = new Database.ViewList.View(name);
								view.Expression = str;
								RegisterViewObject(view);
							}
						}
						if (Database.DatabaseObject.EqualNames(table.TableName, "assemblies"))
						{
							foreach (DataRow row in (InternalDataCollectionBase)table.Rows)
							{
								string assemblyName = (string)row[0];
								string fullName = (string)row[1];
								string runtimeVersion = (string)row[2];
								string vistadbVersion = null;
								if (row.Table.Columns.Count > 5)
									vistadbVersion = row.IsNull(5) ? null : (string)row[5];
								string description = row.IsNull(3) ? null : (string)row[3];
								byte[] coffImage = (byte[])row[4];
								ImportAssembly(assemblyName, coffImage, fullName, runtimeVersion, vistadbVersion, description);
							}
						}
						if (Database.DatabaseObject.EqualNames(table.TableName, "clrProcs"))
						{
							foreach (DataRow row in (InternalDataCollectionBase)table.Rows)
							{
								string procedureName = (string)row[0];
								string clrHostedProcedure = (string)row[1];
								string assemblyName = (string)row[2];
								string signature = (string)row[3];
								string description = row.IsNull(4) ? null : (string)row[4];
								ImportCLRProc(procedureName, clrHostedProcedure, assemblyName, description, signature);
							}
						}
					}
				}
			}
		}

		private void FillInDatabaseData(DataSet database)
		{
			foreach (DataTable table in database.Tables)
			{
				IVistaDBTable vistaDbTable = OpenClone(table.TableName, IsReadOnly);
				vistaDbTable.EnforceConstraints = false;
				vistaDbTable.EnforceIdentities = false;
				try
				{
					foreach (DataRow row in (InternalDataCollectionBase)table.Rows)
					{
						IVistaDBRow currentRow = vistaDbTable.CurrentRow;
						vistaDbTable.Insert();
						foreach (IVistaDBColumn vistaDbColumn in currentRow)
						{
							object obj = row[vistaDbColumn.Name];
							vistaDbColumn.Value = obj == DBNull.Value ? null : obj;
						}
						vistaDbTable.CurrentRow = currentRow;
						((ITable)vistaDbTable).Post();
					}
				}
				finally
				{
					vistaDbTable.EnforceConstraints = true;
					vistaDbTable.EnforceIdentities = true;
					ReleaseClone((ITable)vistaDbTable);
				}
			}
		}

		private void CreateTransactionLog()
		{
		}

		private void OpenTransactionLog()
		{
		}

		private AlterList AnalyzeSchema(Table.TableSchema oldSchema, Table.TableSchema newSchema, out bool metaChanges)
		{
			AlterList alterList = new AlterList(oldSchema, newSchema);
			metaChanges = alterList.AnalyzeChanges();
			return alterList;
		}

		private bool RenameTable(Table.TableSchema oldSchema, Table.TableSchema newSchema, bool commit)
		{
			if (Database.DatabaseObject.EqualNames(oldSchema.Name, newSchema.Name, false) && string.Compare(oldSchema.Description, newSchema.Description, StringComparison.OrdinalIgnoreCase) == 0)
				return false;
			ulong headerPosition = oldSchema.HeaderPosition;
			if ((long)headerPosition == (long)Row.EmptyReference)
				throw new VistaDBException(126, oldSchema.Name);
			long type1 = (long)oldSchema.Type;
			long type2 = (long)newSchema.Type;
			UpdateFullObjectEntry(commit, Database.VdbObjects.Table, Row.EmptyReference, oldSchema.Name, headerPosition, Row.EmptyReference, type1, true, null, oldSchema.Description, null, null, Database.VdbObjects.Table, Row.EmptyReference, newSchema.Name, headerPosition, Row.EmptyReference, type2, true, null, newSchema.Description, null, null);
			return true;
		}

		private bool UpdateMetaObjects(ClusteredRowset rowset, AlterList alterList, bool commit, Row defaultSeedRow)
		{
			bool flag = false;
			Table.TableSchema newSchema = alterList.NewSchema;
			long headerPosition = (long)alterList.OldSchema.HeaderPosition;
			foreach (string key in alterList.DroppedDefaults.Keys)
			{
				UnregisterDefaultValue(rowset, key, true);
				flag = true;
			}
			foreach (string key in alterList.UpdatedDefaults.Keys)
			{
				IVistaDBDefaultValueInformation updatedDefault = alterList.UpdatedDefaults[key];
				UnregisterDefaultValue(rowset, key, true);
				RegisterDefaultValue(rowset, updatedDefault.ColumnName, updatedDefault.Expression, updatedDefault.UseInUpdate, updatedDefault.Description);
				flag = true;
			}
			foreach (IVistaDBDefaultValueInformation valueInformation in alterList.NewDefaults.Values)
			{
				RegisterDefaultValue(rowset, valueInformation.ColumnName, valueInformation.Expression, valueInformation.UseInUpdate, valueInformation.Description);
				flag = true;
			}
			foreach (string key in alterList.DroppedIdentities.Keys)
			{
				UnregisterIdentity(rowset, key, true);
				flag = true;
			}
			foreach (string key in alterList.UpdatedIdentities.Keys)
			{
				IVistaDBIdentityInformation updatedIdentity = alterList.UpdatedIdentities[key];
				int rowIndex = alterList.OldSchema[key].RowIndex;
				Row.Column column = rowset.DefaultRow[rowIndex];
				Conversion.Convert(defaultSeedRow[column.RowIndex], rowset.DefaultRow[column.RowIndex]);
				++rowset.DefaultRow.RowVersion;
				UnregisterIdentity(rowset, key, true);
				RegisterIdentity(rowset, updatedIdentity.ColumnName, updatedIdentity.StepExpression);
				flag = true;
			}
			foreach (IVistaDBIdentityInformation identityInformation in alterList.NewIdentities.Values)
			{
				int rowIndex = alterList.NewSchema[identityInformation.ColumnName].RowIndex;
				Row.Column column = rowset.DefaultRow[rowIndex];
				Conversion.Convert(defaultSeedRow[column.RowIndex], rowset.DefaultRow[column.RowIndex]);
				++rowset.DefaultRow.RowVersion;
				rowset.UpdateSchemaVersion();
				RegisterIdentity(rowset, identityInformation.ColumnName, identityInformation.StepExpression);
				flag = true;
			}
			foreach (string key in alterList.DroppedConstraints.Keys)
			{
				UnregisterConstraint(rowset, key, true);
				flag = true;
			}
			foreach (string key in alterList.UpdatedConstraints.Keys)
			{
				IVistaDBConstraintInformation updatedConstraint = alterList.UpdatedConstraints[key];
				RegisterConstraint(rowset, updatedConstraint.Name, updatedConstraint.Expression, Constraint.MakeStatus(updatedConstraint.AffectsInsertion, updatedConstraint.AffectsUpdate, updatedConstraint.AffectsDelete), updatedConstraint.Description);
				flag = true;
			}
			foreach (IVistaDBConstraintInformation constraintInformation in alterList.NewConstraints.Values)
			{
				RegisterConstraint(rowset, constraintInformation.Name, constraintInformation.Expression, Constraint.MakeStatus(constraintInformation.AffectsInsertion, constraintInformation.AffectsUpdate, constraintInformation.AffectsDelete), constraintInformation.Description);
				flag = true;
			}
			return flag;
		}

		private bool UpdateColumns(AlterList alterList, bool commit)
		{
			ulong headerPosition = alterList.OldSchema.HeaderPosition;
			bool flag = false;
			foreach (AlterList.AlterInformation alterInformation in alterList.Values)
			{
				if (alterInformation.PersistentChanges)
				{
					flag = true;
					Row.Column newColumn = (Row.Column)alterInformation.NewColumn;
					if (alterInformation.PrimaryKeyAffected && newColumn.AllowNull)
						newColumn.AssignAttributes(newColumn.Name, false, newColumn.ReadOnly, newColumn.Encrypted, newColumn.Packed);
					UpdateColumn(alterInformation.OldColumn, newColumn, headerPosition, commit);
				}
			}
			return flag;
		}

		private bool UpdateIndexesAndRelations(Table srcTable, AlterList alterList, bool commit)
		{
			bool flag = false;
			foreach (string key in alterList.DroppedIndexes.Keys)
			{
				IVistaDBIndexInformation droppedIndex = alterList.DroppedIndexes[key];
				if (droppedIndex.Primary)
				{
					string name = alterList.OldSchema.Name;
					string relation = null;
					if (IsReferencedPK(name, ref relation))
						throw new VistaDBException(321, name);
				}
				if (droppedIndex.FKConstraint)
				{
					srcTable.DropForeignKey(key, false);
					alterList.NewSchema.ForeignKeys.Remove(key);
				}
				else
					srcTable.DropIndex(key, false);
				flag = true;
			}
			foreach (string key in alterList.UpdatedIndexes.Keys)
			{
				IVistaDBIndexInformation updatedIndex = alterList.UpdatedIndexes[key];
				RowsetIndex index = (RowsetIndex)srcTable[key];
				if (updatedIndex.FKConstraint)
				{
					Database.RelationshipCollection.RelationshipInformation foreignKey = (Database.RelationshipCollection.RelationshipInformation)alterList.OldSchema.ForeignKeys[key];
					Database.RelationshipCollection.RelationshipInformation newRelation = new Database.RelationshipCollection.RelationshipInformation(foreignKey.Name, foreignKey.ForeignTableId, updatedIndex.KeyExpression, foreignKey.PrimaryTableId, foreignKey.Options, foreignKey.Description);
					if (!newRelation.Equals(foreignKey))
					{
						srcTable.Rowset.UpdateIndexInformation(index, updatedIndex, false);
						flag = true;
						UpdateRelationship(foreignKey, newRelation, false);
					}
				}
			}
			return flag;
		}

		private void UpdatePersistentObjects(Table table, AlterList alterList)
		{
			Table.TableSchema.IndexCollection indexes1 = alterList.OldSchema.Indexes;
			Table.TableSchema.IndexCollection indexes2 = alterList.NewSchema.Indexes;
			Row defaultRow = table.Rowset.DefaultRow;
			foreach (Table.TableSchema.IndexCollection.IndexInformation indexInformation1 in indexes1.Values)
			{
				string name1 = indexInformation1.Name;
				if (indexes2.TryGetValue(name1, out IVistaDBIndexInformation indexInformation2) && indexInformation2 != null && alterList.PersistentIndexes.ContainsKey(name1))
				{
					if (indexInformation2.KeyStructure == null)
					{
						alterList.PersistentIndexes.Remove(name1);
						if (table.ContainsKey(name1) && !alterList.DroppedIndexes.ContainsKey(name1))
							alterList.DroppedIndexes.Add(name1, indexInformation1);
						alterList.NewIndexes.Add(name1, indexInformation2);
					}
					else
					{
						bool flag = false;
						string keyExpression = string.Empty;
						foreach (IVistaDBKeyColumn vistaDbKeyColumn in indexInformation2.KeyStructure)
						{
							string name2 = defaultRow[vistaDbKeyColumn.RowIndex].Name;
							if (alterList.TryGetValue(name2, out AlterList.AlterInformation alterInformation))
							{
								IVistaDBColumnAttributes newColumn = alterInformation.NewColumn;
								if (newColumn != null)
								{
									string str = newColumn.Name;
									if (vistaDbKeyColumn.Descending)
										str = "DESC" + '(' + str + ')';
									keyExpression = keyExpression + str + ";";
									if (alterInformation.Renamed)
										flag = flag || alterInformation.Renamed;
								}
							}
						}
						string str1 = VistaDB.Engine.Core.Indexing.Index.FixKeyExpression(keyExpression);
						if (str1.Length == 0)
						{
							alterList.PersistentIndexes.Remove(name1);
							indexes2.Remove(name1);
							alterList.DroppedIndexes.Add(name1, indexInformation1);
						}
						else
						{
							if (flag)
							{
								alterList.PersistentIndexes.Remove(name1);
								alterList.UpdatedIndexes.Add(name1, indexInformation2);
							}
						  ((Table.TableSchema.IndexCollection.IndexInformation)indexInformation2).KeyExpression = str1;
						}
					}
				}
			}
			IVistaDBConstraintCollection constraints1 = alterList.OldSchema.Constraints;
			IVistaDBConstraintCollection constraints2 = alterList.NewSchema.Constraints;
			foreach (IVistaDBConstraintInformation constraintInformation1 in constraints1.Values)
			{
				string name1 = constraintInformation1.Name;
				if (constraints2.TryGetValue(name1, out IVistaDBConstraintInformation constraintInformation2) && alterList.PersistentConstraints.ContainsKey(name1))
				{
					string original = constraintInformation2.Expression;
					bool flag = false;
					foreach (IVistaDBColumnAttributes columnAttributes in alterList.OldSchema)
					{
						string name2 = columnAttributes.Name;
						if (alterList.TryGetValue(name2, out AlterList.AlterInformation alterInformation) && alterInformation.NewColumn != null)
						{
							string name3 = alterInformation.NewColumn.Name;
							if (!Database.DatabaseObject.EqualNames(name2, name3, true))
							{
								original = Utilities.ReplaceStringEx(original, name2, name3, StringComparison.OrdinalIgnoreCase, -1);
								flag = true;
							}
						}
					}
					if (flag)
					{
						((Table.TableSchema.ConstraintCollection.ConstraintInformation)constraintInformation2).Expression = original;
						alterList.PersistentConstraints.Remove(name1);
						alterList.UpdatedConstraints.Add(name1, constraintInformation2);
					}
				}
			}
		}

		private Database.RelationshipCollection UpdateRelationships(AlterList alterList, Table.TableSchema modifiedSchema, bool commit)
		{
			string name = alterList.OldSchema.Name;
			Dictionary<string, Database.RelationshipCollection.RelationshipInformation> dictionary1 = new Dictionary<string, Database.RelationshipCollection.RelationshipInformation>();
			Dictionary<string, Database.RelationshipCollection.RelationshipInformation> dictionary2 = new Dictionary<string, Database.RelationshipCollection.RelationshipInformation>();
			Database.RelationshipCollection relationships = (Database.RelationshipCollection)GetRelationships();
			Database.RelationshipCollection foreignKeys = alterList.NewSchema.ForeignKeys;
			foreach (Database.RelationshipCollection.RelationshipInformation relationshipInformation1 in relationships.Values)
			{
				if (alterList.DroppedIndexes.TryGetValue(relationshipInformation1.Name, out IVistaDBIndexInformation indexInformation) && indexInformation.FKConstraint)
					dictionary1.Add(relationshipInformation1.Name, relationshipInformation1);
				else if (Database.DatabaseObject.EqualNames(name, relationshipInformation1.PrimaryTable))
				{
					Database.RelationshipCollection.RelationshipInformation relationshipInformation2 = new Database.RelationshipCollection.RelationshipInformation(relationshipInformation1.Name, relationshipInformation1.ForeignTableId, relationshipInformation1.ForeignKey, modifiedSchema.HeaderPosition, relationshipInformation1.Options, relationshipInformation1.Description);
					if (!relationshipInformation2.Equals(relationshipInformation1))
						dictionary2.Add(relationshipInformation1.Name, relationshipInformation2);
				}
				else if (Database.DatabaseObject.EqualNames(name, relationshipInformation1.ForeignTable))
				{
					IVistaDBIndexInformation index = modifiedSchema.Indexes[relationshipInformation1.Name];
					ulong headerPosition = modifiedSchema.HeaderPosition;
					string keyExpression = index.KeyExpression;
					if (!dictionary2.TryGetValue(relationshipInformation1.Name, out RelationshipCollection.RelationshipInformation relationshipInformation2))
					{
						relationshipInformation2 = new Database.RelationshipCollection.RelationshipInformation(relationshipInformation1.Name, headerPosition, keyExpression, relationshipInformation1.PrimaryTableId, relationshipInformation1.Options, relationshipInformation1.Description);
					}
					else
					{
						relationshipInformation2.ForeignTableId = headerPosition;
						relationshipInformation2.ForeignKey = keyExpression;
					}
					if (!relationshipInformation2.Equals(relationshipInformation1))
						dictionary2.Add(relationshipInformation1.Name, relationshipInformation2);
				}
			}
			foreach (Database.RelationshipCollection.RelationshipInformation relationshipInformation in dictionary1.Values)
				DeleteInScope(Database.VdbObjects.Relationship, relationshipInformation.ForeignTableId, relationshipInformation.Name, false);
			foreignKeys.Clear();
			foreach (Database.RelationshipCollection.RelationshipInformation newRelation in dictionary2.Values)
			{
				Database.RelationshipCollection.RelationshipInformation oldRelation = (Database.RelationshipCollection.RelationshipInformation)relationships[newRelation.Name];
				ulong headerPosition = modifiedSchema.HeaderPosition;
				if ((long)newRelation.PrimaryTableId != (long)newRelation.ForeignTableId && (long)newRelation.PrimaryTableId == (long)headerPosition)
				{
					UpdateRelationship(oldRelation, newRelation, false);
				}
				else
				{
					DeleteInScope(Database.VdbObjects.Relationship, oldRelation.ForeignTableId, oldRelation.Name, false);
					foreignKeys.Add(newRelation.Name, newRelation);
				}
			}
			return foreignKeys;
		}

		private Row CheckSchemaValidity(Table srcTable, AlterList alterList, IVistaDBTableSchema modifiedSchema)
		{
			bool flag1 = false;
			try
			{
				ITable table = null;
				try
				{
					table = CreateTable(modifiedSchema, true, IsReadOnly, true);
					flag1 = true;
					Row row = ((Table)table).Rowset.DefaultRow.CopyInstance();
					IVistaDBIndexCollection indexes1 = alterList.OldSchema.Indexes;
					IVistaDBIndexCollection indexes2 = modifiedSchema.Indexes;
					IVistaDBIndexInformation indexInformation1 = null;
					IVistaDBIndexInformation indexInformation2 = null;
					foreach (IVistaDBIndexInformation indexInformation3 in indexes1.Values)
					{
						if (indexInformation3.Primary)
						{
							indexInformation1 = indexInformation3;
							break;
						}
					}
					if (indexInformation1 == null)
						return row;
					foreach (IVistaDBIndexInformation indexInformation3 in indexes2.Values)
					{
						if (indexInformation3.Primary)
						{
							indexInformation2 = indexInformation3;
							break;
						}
					}
					if (indexInformation2 == null)
						return row;
					RowsetIndex rowsetIndex = (RowsetIndex)((Dictionary<string, VistaDB.Engine.Core.Indexing.Index>)table)[indexInformation2.Name];
					IVistaDBKeyColumn[] keyStructure1 = indexInformation1.KeyStructure;
					IVistaDBKeyColumn[] keyStructure2 = rowsetIndex.CollectIndexInformation().KeyStructure;
					if (keyStructure1.Length != keyStructure2.Length)
						throw new VistaDBException(321, srcTable.Rowset.Name);
					foreach (IVistaDBKeyColumn vistaDbKeyColumn1 in keyStructure1)
					{
						string name1 = srcTable.Rowset.DefaultRow[vistaDbKeyColumn1.RowIndex].Name;
						string name2 = alterList[name1].NewColumn.Name;
						bool flag2 = false;
						foreach (IVistaDBKeyColumn vistaDbKeyColumn2 in keyStructure2)
						{
							if (Database.DatabaseObject.EqualNames(((Table)table).Rowset.DefaultRow[vistaDbKeyColumn2.RowIndex].Name, name2))
							{
								flag2 = true;
								break;
							}
						}
						if (!flag2)
							throw new VistaDBException(321, srcTable.Rowset.Name);
					}
					return row;
				}
				finally
				{
					table?.Close();
				}
			}
			finally
			{
				if (flag1)
					DropTable(modifiedSchema.Name, true, false, false, false);
			}
		}

		private void UnregisterTable(string name, ulong headerPosition, bool commit)
		{
			DeleteInScope(Database.VdbObjects.Identity, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.DefaultValue, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.Constraint, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.Relationship, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.Index, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.Column, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.CLRTrigger, headerPosition, null, commit);
			DeleteInScope(Database.VdbObjects.Table, Row.EmptyReference, name, commit);
			lastIdentities.DeactivateTableIdentities(name);
			lastTimeStamps.DeactivateTableIdentities(name);
		}

		private void UnregisterTable(string name, bool commit, bool raiseNonExistence, bool testPKRefernces, bool raiseOtherErrors)
		{
			if (descriptors.TableInstance(name) != null)
				throw new VistaDBException(122, name);
			try
			{
				ulong tableId1 = GetTableId(name, out string description, out Table.TableType type, raiseNonExistence);
				if ((long)tableId1 == (long)Row.EmptyReference)
					return;
				if (testPKRefernces && IsOuterReferencedPK(name))
					throw new VistaDBException(144);
				if (raiseOtherErrors)
				{
					if (type != Table.TableType.Default)
						throw new VistaDBException(209, name);
					string name1 = ClusteredRowset.SyncExtension.TombstoneTablenamePrefix + name;
					ulong tableId2 = GetTableId(ClusteredRowset.SyncExtension.TombstoneTablenamePrefix + name, out description, out type, false);
					if (type == Table.TableType.Tombstone && (long)tableId2 != (long)Row.EmptyReference)
						UnregisterTable(name1, tableId2, false);
				}
				UnregisterTable(name, tableId1, commit);
			}
			catch (VistaDBException ex)
			{
				if (ex.ErrorId == 126)
					DeleteInScope(Database.VdbObjects.Table, Row.EmptyReference, name, commit);
				throw ex;
			}
		}

		private void UpdateColumn(IVistaDBColumnAttributes oldColumn, IVistaDBColumnAttributes newColumn, ulong storageId, bool commit)
		{
			ulong num = PrepareColumnOption(newColumn);
			UpdateFullObjectEntry(commit, Database.VdbObjects.Column, storageId, oldColumn.Name, (ulong)oldColumn.RowIndex, Row.EmptyReference, (long)num, true, oldColumn.Caption, oldColumn.Description, null, null, Database.VdbObjects.Column, storageId, newColumn.Name, (ulong)newColumn.RowIndex, Row.EmptyReference, (long)num, true, newColumn.Caption, newColumn.Description, null, null);
		}

		private bool IsRelationshipRegistered(ClusteredRowset fkRowset, string constraintName)
		{
			return ((Dictionary<string, IVistaDBRelationshipInformation>)GetRelationships()).ContainsKey(constraintName);
		}

		private void UpdateRelationship(Database.RelationshipCollection.RelationshipInformation oldRelation, Database.RelationshipCollection.RelationshipInformation newRelation, bool commit)
		{
			UpdateFullObjectEntry(commit, Database.VdbObjects.Relationship, oldRelation.ForeignTableId, oldRelation.Name, 0UL, oldRelation.PrimaryTableId, oldRelation.Options, true, ((IVistaDBRelationshipInformation)oldRelation).ForeignKey, oldRelation.Description, null, null, Database.VdbObjects.Relationship, newRelation.ForeignTableId, newRelation.Name, 0UL, newRelation.PrimaryTableId, newRelation.Options, true, ((IVistaDBRelationshipInformation)newRelation).ForeignKey, newRelation.Description, null, null);
		}

		private void ImportCLRProc(string procedureName, string clrHostedProcedure, string assemblyName, string description, string signature)
		{
			bool commit = false;
			LockStorage();
			try
			{
				CreateObjectEntry(false, Database.VdbObjects.CLRProcedure, Row.EmptyReference, procedureName, Row.EmptyReference, Row.EmptyReference, 0L, true, Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(clrHostedProcedure, signature), description, Encoding.Unicode.GetBytes(assemblyName), null);
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 386, procedureName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		private ClrHosting.ClrProcedure PrepareInvoke(string procedureName, ClrHosting list, Database.VdbObjects clrType)
		{
			ClrHosting.ClrProcedure clrProcedure = list[procedureName];
			if (clrProcedure != null)
				return clrProcedure;
			string assemblyName;
			string clrHostedProcedure;
			switch (clrType)
			{
				case Database.VdbObjects.CLRProcedure:
					Database.ClrProcedureCollection.ClrProcedureInformation clrProcedureObject = (Database.ClrProcedureCollection.ClrProcedureInformation)LoadClrProcedureObjects()[procedureName];
					if (clrProcedureObject == null)
						throw new VistaDBException(383, procedureName);
					assemblyName = clrProcedureObject.AssemblyName;
					clrHostedProcedure = clrProcedureObject.FullHostedName;
					break;
				case Database.VdbObjects.CLRTrigger:
					Database.ClrTriggerCollection.ClrTriggerInformation clrTriggerObject = (Database.ClrTriggerCollection.ClrTriggerInformation)LoadClrTriggerObjects()[procedureName];
					if (clrTriggerObject == null)
						throw new VistaDBException(397, procedureName);
					assemblyName = clrTriggerObject.AssemblyName;
					clrHostedProcedure = clrTriggerObject.FullHostedName;
					break;
				default:
					assemblyName = null;
					clrHostedProcedure = null;
					break;
			}
			if (assemblyName == null || clrHostedProcedure == null)
				throw new VistaDBException(clrType == Database.VdbObjects.CLRTrigger ? 397 : 383, procedureName);
			Database.AssemblyCollection.AssemblyInformation assemblyInformation = LocateAssembly(assemblyName, true);
			if (assemblyInformation == null)
			{
				if (!IsReadOnly)
					DeleteInScope(clrType, Row.EmptyReference, procedureName, true);
				throw new VistaDBException(clrType == Database.VdbObjects.CLRTrigger ? 397 : 383, procedureName);
			}
			ClrHosting.ClrProcedure method = ClrHosting.GetMethod(clrHostedProcedure, assemblyInformation.RegisteredAssembly);
			list.AddProcedure(procedureName, method);
			return method;
		}

		private void ImportAssembly(string assemblyName, byte[] coffImage, string fullName, string runtimeVersion, string vistadbVersion, string description)
		{
			LockStorage();
			bool flag = false;
			try
			{
				CreateObjectEntry(false, Database.VdbObjects.Assembly, Row.EmptyReference, assemblyName, Row.EmptyReference, Row.EmptyReference, 0L, true, Database.AssemblyCollection.AssemblyInformation.CompileScriptValue(fullName, runtimeVersion, vistadbVersion), description, null, coffImage);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 391, assemblyName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, true);
			}
		}

		private Database.AssemblyCollection.AssemblyInformation LocateAssembly(string assemblyName, bool createInstance)
		{
			return (Database.AssemblyCollection.AssemblyInformation)LoadAssemblies(createInstance)[assemblyName];
		}

		private void ImportCLRTrigger(string triggerName, string clrHostedTrigger, string assemblyName, string tableName, TriggerAction eventType, string description, string signature)
		{
			bool commit = false;
			LockStorage();
			try
			{
				CreateObjectEntry(false, Database.VdbObjects.CLRTrigger, GetTableId(tableName, out string description1, out Table.TableType type, true), triggerName, Row.EmptyReference, Row.EmptyReference, (long)eventType, true, Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(clrHostedTrigger, signature), description, Encoding.Unicode.GetBytes(assemblyName), null);
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 386, triggerName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal char EvaluateMaximumChar()
		{
			return maxChar;
		}

		protected override bool OnNotifyChangedEnvironment(Connection.Settings variable, object newValue)
		{
			return descriptors.NotifyChangedEnvironment(variable, newValue);
		}

		protected override void OnLockRow(uint rowId, bool userLock, ref bool actualLock)
		{
			if (rowId != uint.MaxValue)
				return;
			base.OnLockRow(rowId, userLock, ref actualLock);
			if (!actualLock || creation)
				return;
			Handle.ResetCachedLength();
		}

		protected override void OnUnlockRow(uint rowId, bool userLock, bool instantly)
		{
			if (rowId != uint.MaxValue)
				return;
			base.OnUnlockRow(rowId, userLock, instantly);
		}

		protected override void OnLowLevelLockRow(uint rowId)
		{
			base.OnLowLevelLockRow(rowId - byte.MaxValue);
		}

		protected override void OnLowLevelUnlockRow(uint rowId)
		{
			base.OnLowLevelUnlockRow(rowId - byte.MaxValue);
		}

		internal override int DoSplitPolicy(int oldCount)
		{
			return SplitPolicy_3_4(oldCount);
		}

		protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
		{
			conversion = new CrossConversion(culture);
			return Database.DatabaseHeader.CreateInstance(this, pageSize, culture);
		}

		protected override void OnActivateHeader(ulong position)
		{
			base.OnActivateHeader(position);
			conversion = new CrossConversion(Culture);
		}

		protected override void OnCreateHeader(ulong position)
		{
			Header.EncryptionKeyMd5 = Encryption != null ? EncryptionKey.Md5Signature : Md5.Signature.EmptySignature;
			base.OnCreateHeader(position);
		}

		protected override void OnDeclareNewStorage(object hint)
		{
			base.OnDeclareNewStorage(InitializeMetaTableSchema());
		}

		protected override void OnCreateStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
		{
			creation = true;
			base.OnCreateStorage(openMode, headerPosition);
			creation = false;
		}

		protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
		{
			InitializeMetaTableSchema().Dispose();
			int pageSize = PageSize;
			base.OnOpenStorage(openMode, headerPosition);
			if (PageSize == pageSize)
				return;
			InitializeMetaTableSchema().Dispose();
		}

		protected override void Destroy()
		{
			CloseTables();
			conversion = null;
			if (transferList != null)
				transferList.Clear();
			transferList = null;
			if (clrHosting != null)
				clrHosting.Clear();
			clrHosting = null;
			if (sqlContext != null)
				//this.sqlContext.Dispose();
				sqlContext = null;
			if (stdIndexParser != null)
				stdIndexParser.Dispose();
			stdIndexParser = null;
			if (descriptors != null)
				descriptors.Clear();
			descriptors = null;
			base.Destroy();
		}

		protected override ulong OnGetFreeCluster(int pageCount)
		{
			LockSpaceMap();
			try
			{
				return base.OnGetFreeCluster(pageCount);
			}
			finally
			{
				UnlockSpaceMap();
			}
		}

		protected override void OnSetFreeCluster(ulong clusterId, int pageCount)
		{
			LockSpaceMap();
			try
			{
				base.OnSetFreeCluster(clusterId, pageCount);
			}
			finally
			{
				UnlockSpaceMap();
			}
		}

		internal Table.TableSchema GetTableSchema(string tableName, bool fullRelations)
		{
			if (tableName == null)
				return (Table.TableSchema)InitializeMetaTableSchema();
			LockStorage();
			try
			{
				ulong tableId = GetTableId(tableName, out string description, out Table.TableType type, true);
				if ((long)tableId == (long)Row.EmptyReference)
					return null;
				Table.TableSchema tableSchema = new Table.TableSchema(tableName, type, description, tableId, this);
				foreach (Row.Column column in AllocateRowsetSchema(tableId, CreateEmptyRowInstance()))
					tableSchema.AddNewColumn(column.Duplicate(false));
				tableSchema.FixInitCounter();
				GetIndexes(tableId, tableSchema);
				GetIdentities(tableId, tableSchema.Identities, tableName);
				GetDefaultValues(tableId, tableSchema.Defaults);
				GetConstraints(tableId, tableSchema.Constraints);
				GetClrTriggers(tableId, tableSchema.Triggers, tableName);
				GetRelationships(fullRelations ? 0UL : tableId, tableSchema.ForeignKeys);
				return tableSchema;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal bool ContainsPrimaryKey(string tableName)
		{
			ulong tableId = GetTableId(tableName, out string description, out Table.TableType type, true);
			IVistaDBIndexCollection indexes = new Table.TableSchema.IndexCollection();
			GetIndexes(tableId, indexes);
			foreach (IVistaDBIndexInformation indexInformation in indexes)
			{
				if (indexInformation.Primary)
					return true;
			}
			return false;
		}

		internal void ActivateTableObjects(Table table, IVistaDBTableSchema schema)
		{
			ActivateIndexes(table, schema);
			ActivateObjects(table.Rowset, schema);
			ActivateRelationships(table, schema);
			ActivateReadonly(table, schema);
		}

		internal void ActivateLastIdentity(string tableName, Row.Column column)
		{
			lastIdentities.ActivateTableIdentity(tableName, column);
		}

		internal IColumn GetLastIdentity(string tableName, string columnName)
		{
			return lastIdentities.GetLastIdentity(tableName, columnName);
		}

		internal void SetLastIdentity(string tableName, Row row)
		{
			lastIdentities.SetLastIdentity(tableName, row);
		}

		internal void ActivateLastTimestamp(string tableName, Row.Column column)
		{
			lastTimeStamps.ActivateTableIdentity(tableName, column);
		}

		internal IColumn GetLastTimestamp(string tableName)
		{
			return lastTimeStamps.GetLastIdentity(tableName, null);
		}

		internal void SetLastTimeStamp(string tableName, Row row)
		{
			lastTimeStamps.SetLastIdentity(tableName, row);
		}

		internal ITable OpenTable(string name, bool exclusive, bool readOnly, bool activateObjects)
		{
			descriptors.CheckExclusiveInstance(name, exclusive);
			Table table = null;
			StorageHandle.StorageMode openMode = new StorageHandle.StorageMode(FileMode.Open, !exclusive, false, readOnly ? FileAccess.Read : FileAccess.ReadWrite, Handle.Transacted, Handle.IsolatedStorage);
			LockStorage();
			try
			{
				if (string.IsNullOrEmpty(name))
					throw new VistaDBException(152);
				Table.TableSchema tableSchema = GetTableSchema(name, true);
				ClusteredRowset clonedRowset = null;
				table = Table.CreateInstance(name, this, clonedRowset, tableSchema.Type);
				ClusteredRowset rowset = table.Rowset;
				LinkDescriptor(table);
				rowset.OpenStorage(openMode, tableSchema.HeaderPosition);
				if (activateObjects)
					ActivateTableObjects(table, tableSchema);
				return table;
			}
			catch (Exception ex)
			{
				UnlinkDescriptor(table);
				if (!RepairMode)
					throw new VistaDBException(ex, 125, name);
				return null;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void CreateTableObjects(Table table, IVistaDBTableSchema schema)
		{
			CreateIndexes(table, schema.Indexes);
			CreateObjects(table, schema);
			ActivateReadonly(table, schema);
		}

		internal void CreateRelationships(Database.RelationshipCollection relationships)
		{
			CreateRelationships(relationships, new List<string>());
		}

		internal void CreateRelationships(Database.RelationshipCollection relationships, IList<string> globalNames)
		{
			if (relationships.Count == 0)
				return;
			Dictionary<string, ITable> dictionary = new Dictionary<string, ITable>(StringComparer.OrdinalIgnoreCase);
			try
			{
				foreach (IVistaDBRelationshipInformation relationshipInformation in relationships.Values)
				{
					try
					{
						string foreignTable = relationshipInformation.ForeignTable;
						if (!dictionary.TryGetValue(foreignTable, out ITable table))
						{
							table = OpenClone(foreignTable, IsReadOnly);
							dictionary.Add(foreignTable, table);
						}
						string constraintName = relationshipInformation.Name;
						if (globalNames.Contains(constraintName.ToUpperInvariant()) || constraintName.StartsWith("sys", StringComparison.OrdinalIgnoreCase) && int.TryParse(constraintName.Substring(3), out int result) && constraintName.Equals(string.Format("sys{0}", result), StringComparison.OrdinalIgnoreCase) || constraintName.StartsWith("ForeignKey", StringComparison.OrdinalIgnoreCase) && int.TryParse(constraintName.Substring(10), out result) && constraintName.Equals(string.Format("ForeignKey{0}", result), StringComparison.OrdinalIgnoreCase))
						{
							constraintName = string.Format("FK_{0}_{1}", relationshipInformation.PrimaryTable, relationshipInformation.ForeignTable);
							if (globalNames.Contains(constraintName.ToUpperInvariant()))
							{
								result = 1;
								while (globalNames.Contains(string.Format("{0}{1}", constraintName, result)))
									++result;
								constraintName = string.Format("{0}{1}", constraintName, result);
							}
							globalNames.Add(constraintName.ToUpperInvariant());
							CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.ConstraintOperation, relationshipInformation.Name, "Renamed foreign key to " + constraintName + " to prevent schema collisions");
						}
						string foreignKey1;
						if (relationshipInformation.ForeignKey.Contains("["))
						{
							StringBuilder stringBuilder = new StringBuilder();
							bool flag = true;
							string foreignKey2 = relationshipInformation.ForeignKey;
							char[] chArray = new char[1] { ';' };
							foreach (string str in foreignKey2.Split(chArray))
							{
								if (flag)
									flag = false;
								else
									stringBuilder.Append(';');
								if (str[0] == '[' && str[str.Length - 1] == ']')
									stringBuilder.Append(str.TrimStart('[').TrimEnd(']').Trim());
								else
									stringBuilder.Append(str);
							}
							foreignKey1 = stringBuilder.ToString();
							CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.ConstraintOperation, relationshipInformation.Name, "Removed invalid '[]'s from key names.");
						}
						else
							foreignKey1 = relationshipInformation.ForeignKey;
						table.CreateForeignKey(constraintName, foreignKey1, relationshipInformation.PrimaryTable, relationshipInformation.UpdateIntegrity, relationshipInformation.DeleteIntegrity, relationshipInformation.Description);
					}
					catch (Exception ex)
					{
						if (!RepairMode)
							throw;
					}
				}
			}
			finally
			{
				foreach (ITable tbl in dictionary.Values)
					ReleaseClone(tbl);
				dictionary.Clear();
			}
		}

		internal ITable CreateTable(IVistaDBTableSchema schema, bool exclusive, bool readOnly, bool createObjects)
		{
			string name = schema.Name;
			Table table = null;
			if (schema.ColumnCount == 0)
				throw new VistaDBException(124, name);
			if (schema.ColumnCount > maxTableColumns)
				throw new VistaDBException(154, maxTableColumns.ToString());
			if (((Table.TableSchema)schema).MinRowDataSize >= PageSize)
				throw new VistaDBException(167, ((Table.TableSchema)schema).MinRowDataSize.ToString() + " bytes");
			StorageHandle.StorageMode accessMode = new StorageHandle.StorageMode(FileMode.CreateNew, !exclusive, false, readOnly ? FileAccess.Read : FileAccess.ReadWrite, Handle.Transacted, Handle.IsolatedStorage);
			LockStorage();
			bool flag = false;
			try
			{
				ulong tableId = GetTableId(name, out string description, out Table.TableType type, false);
				flag = true;
				if ((long)tableId != (long)Row.EmptyReference)
					throw new VistaDBException(145, name);
				ulong freeCluster = GetFreeCluster(1);
				table = Table.CreateInstance(name, this, null, ((Table.TableSchema)schema).Type);
				ClusteredRowset rowset = table.Rowset;
				LinkDescriptor(table);
				rowset.DeclareNewStorage(schema);
				rowset.CreateStorage(accessMode, freeCluster, true);
				((Table.TableSchema)schema).HeaderPosition = rowset.StorageId;
				if (createObjects)
					CreateTableObjects(table, schema);
				return table;
			}
			catch (VistaDBException ex)
			{
				UnlinkDescriptor(table);
				if (!flag && ex.ErrorId == 301)
					throw new VistaDBException(ex, 119, name + " name exceeds maximum of " + CurrentRow[nameIndex].MaxLength.ToString() + " characters for this database.");
				if (ex.Contains(301L))
				{
					VistaDBException vistaDbException = ex;
					while (vistaDbException != null && vistaDbException.ErrorId != 301)
						vistaDbException = vistaDbException.InnerException as VistaDBException;
					if (vistaDbException != null)
					{
						object obj = vistaDbException.Data["Column"];
						if (obj != null)
						{
							if (obj.ToString().Equals(CurrentRow[nameIndex].Name, StringComparison.OrdinalIgnoreCase))
								throw new VistaDBException(ex, 119, name + " column name " + vistaDbException.Data["Value"].ToString() + " exceeds maximum of " + CurrentRow[nameIndex].MaxLength.ToString() + " characters for this database.");
							if (obj.ToString().Equals(CurrentRow[descriptionIndex].Name, StringComparison.OrdinalIgnoreCase))
								throw new VistaDBException(ex, 119, name + " description exceeds maximum of " + CurrentRow[descriptionIndex].MaxLength.ToString() + " characters for this database.");
						}
					}
				}
				throw new VistaDBException(ex, 119, name);
			}
			catch (Exception ex)
			{
				UnlinkDescriptor(table);
				throw new VistaDBException(ex, 119, name);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void AlterTable(string oldName, Table.TableSchema newSchema, bool syncService, bool deactivateSync)
		{
			string name1 = string.Format(ClusteredRowset.TemporaryName, oldName);
			oldName = Row.Column.FixName(oldName);
			string name2 = newSchema.Name;
			bool flag = false;
			bool commit = false;
			DropTable(name1, true, false, false, false);
			LockStorage();
			ITable table1 = null;
			try
			{
				Database.RelationshipCollection relationships = null;
				Table table2 = (Table)OpenClone(oldName, IsReadOnly);
				ClusteredRowset rowset = table2.Rowset;
				if (!syncService && (rowset.IsSystemTable || rowset.ActiveSyncService))
					throw new VistaDBException(208, rowset.Name);
				Table.TableSchema.IndexCollection indexCollection = null;
				rowset.LockStorage();
				try
				{
					if (syncService && Header.SchemaVersionGuid == Guid.Empty)
						Header.SchemaVersionGuid = Guid.NewGuid();
					Table.TableSchema tableSchema1 = GetTableSchema(oldName, true);
					AlterList alterList = AnalyzeSchema(tableSchema1, newSchema, out bool metaChanges);
					Table.TableSchema tableSchema2 = new Table.TableSchema(name1, Table.TableType.Default, newSchema.Description, 0UL, this);
					bool newTableDecision = alterList.TakeNewTableDecision(false, this);
					UpdatePersistentObjects(table2, alterList);
					if (!newTableDecision && alterList.UpdatedIndexes.Count == 0)
					{
						Row defaultSeedRow = null;
						if (metaChanges)
						{
							alterList.FillTemporarySchema(tableSchema2);
							defaultSeedRow = CheckSchemaValidity(table2, alterList, tableSchema2);
						}
						commit = UpdateMetaObjects(rowset, alterList, false, defaultSeedRow);
						commit = UpdateColumns(alterList, false) || commit;
						commit = UpdateIndexesAndRelations(table2, alterList, false) || commit;
						flag = RenameTable(tableSchema1, newSchema, false);
						if ((flag || commit) && (lastIdentities.ContainsKey(tableSchema1.Name) || lastTimeStamps.ContainsKey(tableSchema1.Name)))
						{
							lastIdentities.DeactivateTableIdentities(tableSchema1.Name);
							lastTimeStamps.DeactivateTableIdentities(tableSchema1.Name);
							commit = true;
						}
						if (alterList.NewIndexes.Count > 0)
						{
							indexCollection = alterList.NewSchema.Indexes;
							indexCollection.Clear();
							foreach (IVistaDBIndexInformation indexInformation in alterList.NewIndexes.Values)
								indexCollection.Add(indexInformation.Name, indexInformation);
						}
						if (!commit)
							return;
						rowset.UpdateSchemaVersion();
						return;
					}
					alterList.FillTemporarySchema(tableSchema2);
					CheckSchemaValidity(table2, alterList, tableSchema2);
					tableSchema2.TemporarySchema = false;
					using (Table table3 = (Table)CreateTable(tableSchema2, true, IsReadOnly, false))
					{
						table2.Rowset.AlterList = alterList;
						table2.ExportToTable(table3, null, !syncService, false);
						CreateTableObjects(table3, tableSchema2);
						relationships = UpdateRelationships(alterList, tableSchema2, false);
					}
					UnregisterTable(oldName, rowset.StorageId, false);
					RenameTable(tableSchema2, newSchema, false);
					rowset.UpdateSchemaVersion();
					commit = true;
				}
				finally
				{
					rowset.UnlockStorage(false);
					bool rollback = !commit && !flag;
					rowset.FinalizeChanges(rollback, commit);
					if (rollback)
						ReactivateIndex();
					table1?.Dispose();
					ReleaseClone(table2);
					if (indexCollection != null)
					{
						Table table3 = (Table)OpenClone(name2, IsReadOnly);
						try
						{
							CreateIndexes(table3, indexCollection);
						}
						finally
						{
							ReleaseClone(table3);
						}
					}
				}
				if (relationships != null)
				{
					CompleteRelationshipInfo(relationships);
					CreateRelationships(relationships);
				}
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 120, name2);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, true);
				if (!flag)
					ReactivateIndex();
				DropTable(name1, true, false, false, false);
			}
		}

		internal void CloseAndDropTemporaryTable(string name)
		{
			descriptors.TableInstance(name)?.Dispose();
			DropTable(name, true, false, false, false);
		}

		internal void DropTable(string name)
		{
			LockStorage();
			try
			{
				DropTable(name, true, false, true, true);
				if (DropAnchorTable())
					return;
				DeleteAnchorRow(name);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 121, name);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void DropTable(string name, bool commit, bool raiseNonExistence, bool testPkReferences, bool raiseOtherErrors)
		{
			LockStorage();
			try
			{
				UnregisterTable(name, commit, raiseNonExistence, testPkReferences, raiseOtherErrors);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 121, name);
			}
			finally
			{
				UnlockStorage(commit);
			}
		}

		internal TableIdMap GetTableIdMap()
		{
			LockStorage();
			try
			{
				TableIdMap tableIdMap = new TableIdMap(Culture);
				TraverseDiskObjects(VdbObjects.Table, Row.EmptyReference, new TraverseJobDelegate(LoadTablesJob), (object)tableIdMap);
				return tableIdMap;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal IVistaDBRelationshipCollection GetRelationships()
		{
			LockStorage();
			try
			{
				RelationshipCollection relationships = new RelationshipCollection();
				GetRelationships(0UL, relationships);
				return relationships;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal string GetDescription()
		{
			LockStorage();
			try
			{
				ArrayList arrayList = new ArrayList();
				TraverseDiskObjects(VdbObjects.Description, Row.EmptyReference, new TraverseJobDelegate(LoadDatabaseDescriptionJob), (object)arrayList);
				return arrayList.Count == 0 ? null : (string)arrayList[0];
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void RegisterDescription(string description)
		{
			LockStorage();
			try
			{
				DeleteInScope(Database.VdbObjects.Description, Row.EmptyReference, null, false);
				if (description == null || description.Length == 0)
					return;
				CreateObjectEntry(true, Database.VdbObjects.Description, Row.EmptyReference, Database.DescriptionName, Row.EmptyReference, Row.EmptyReference, 0L, true, null, description, null, null);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void ReactivateObjects(ClusteredRowset rowset)
		{
			foreach (Row.Column column in rowset.CurrentRow)
			{
				rowset.DeactivateIdentity(column);
				rowset.DeactivateDefaultValue(column);
			}
			rowset.DeactivateTriggers();
			ActivateObjects(rowset, GetTableSchema(rowset.Name, true));
		}

		internal IRow GetRowStruct(string tableName)
		{
			LockStorage();
			try
			{
				return AllocateRowsetSchema(GetTableId(tableName, out string description, out Table.TableType type, true), CreateEmptyRowInstance());
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void RegisterRowsetSchema(DataStorage rowset, IVistaDBTableSchema schema)
		{
			long type = (long)((Table.TableSchema)schema).Type;
			CreateObjectEntry(false, Database.VdbObjects.Table, Row.EmptyReference, rowset.Name, rowset.StorageId, Row.EmptyReference, type, true, null, schema.Description, null, null);
			int num1 = 0;
			foreach (IVistaDBColumnAttributes column in schema)
			{
				++num1;
				ulong num2 = PrepareColumnOption(column);
				CreateObjectEntry(false, Database.VdbObjects.Column, rowset.StorageId, column.Name, (ulong)column.RowIndex, Row.EmptyReference, (long)num2, true, null, column.Description, null, null);
			}
		}

		internal bool IsReferencedPK(string pkRowsetName, ref string relation)
		{
			Database.RelationshipCollection relationships = new Database.RelationshipCollection();
			GetRelationships(0UL, relationships);
			foreach (Database.RelationshipCollection.RelationshipInformation relationshipInformation in relationships.Values)
			{
				if (Database.DatabaseObject.EqualNames(relationshipInformation.PrimaryTable, pkRowsetName))
				{
					relation = relationshipInformation.Name;
					return true;
				}
			}
			return false;
		}

		internal bool IsOuterReferencedPK(string pkRowsetName)
		{
			Database.RelationshipCollection relationships = new Database.RelationshipCollection();
			GetRelationships(0UL, relationships);
			foreach (IVistaDBRelationshipInformation relationshipInformation in relationships.Values)
			{
				if (Database.DatabaseObject.EqualNames(relationshipInformation.PrimaryTable, pkRowsetName) && !Database.DatabaseObject.EqualNames(relationshipInformation.ForeignTable, pkRowsetName))
					return true;
			}
			return false;
		}

		internal void UpdateRegisteredIndex(RowsetIndex index, IVistaDBIndexInformation newIndex, bool commit)
		{
			index.ParentRowset.UpdateSchemaVersion();
			UpdateFullObjectEntry(commit, Database.VdbObjects.Index, index.ParentRowset.StorageId, index.Alias, index.StorageId, Row.EmptyReference, index.Header.Signature, index.Header.Descend, index.KeyExpression, null, index.RowKeyStructure, null, Database.VdbObjects.Index, index.ParentRowset.StorageId, newIndex.Name, index.StorageId, Row.EmptyReference, index.Header.Signature, index.Header.Descend, newIndex.KeyExpression, null, index.RowKeyStructure, null);
			if (!(index.IsPrimary ^ newIndex.Primary))
				return;
			UpdatePkColumnsFlags(index.ParentRowset.StorageId, index.KeyPCode.EnumColumns(), false);
		}

		internal void RegisterIndex(RowsetIndex index)
		{
			index.ParentRowset.UpdateSchemaVersion();
			int maxLength = DefaultRow[scriptValueIndex].MaxLength;
			string scriptValue = index.KeyExpression;
			if (scriptValue.Length > maxLength)
				scriptValue = scriptValue.Substring(0, maxLength);
			CreateObjectEntry(false, Database.VdbObjects.Index, index.ParentRowset.StorageId, index.Alias, index.StorageId, Row.EmptyReference, index.Header.Signature, index.Header.Descend, scriptValue, null, index.RowKeyStructure, null);
			if (!index.IsPrimary)
				return;
			UpdatePkColumnsFlags(index.ParentRowset.StorageId, index.KeyPCode.EnumColumns(), false);
		}

		internal void UnregisterIndex(RowsetIndex index)
		{
			IVistaDBIndexCollection indexes = new Table.TableSchema.IndexCollection();
			GetIndexes(index.ParentRowset.StorageId, indexes);
			if (!indexes.ContainsKey(index.Alias))
				throw new VistaDBException(sbyte.MaxValue, index.Alias);
			string relation = null;
			if (index.IsPrimary && IsReferencedPK(index.ParentRowset.Name, ref relation))
				throw new VistaDBException(199, relation);
			index.ParentRowset.UpdateSchemaVersion();
			DeleteInScope(Database.VdbObjects.Index, index.ParentRowset.StorageId, index.Alias, false);
		}

		internal bool IsIdentityRegistered(ClusteredRowset rowset, string columnName)
		{
			Table.TableSchema.IdentityCollection identityCollection = new Table.TableSchema.IdentityCollection();
			GetIdentities(rowset.StorageId, identityCollection, rowset.Name);
			return identityCollection.ContainsKey(columnName);
		}

		internal void RegisterIdentity(ClusteredRowset rowset, string columnName, string stepExpression)
		{
			if (IsIdentityRegistered(rowset, columnName))
				UnregisterIdentity(rowset, columnName, false);
			if (IsDefaultValueRegistered(rowset, columnName))
				UnregisterDefaultValue(rowset, columnName, false);
			rowset.UpdateSchemaVersion();
			CreateObjectEntry(false, Database.VdbObjects.Identity, rowset.StorageId, columnName, 0UL, Row.EmptyReference, 0L, true, stepExpression, null, null, null);
		}

		internal void UnregisterIdentity(ClusteredRowset rowset, string columnName, bool checkRegistered)
		{
			if (checkRegistered && !IsIdentityRegistered(rowset, columnName))
				throw new VistaDBException(185, columnName);
			rowset.UpdateSchemaVersion();
			DeleteInScope(Database.VdbObjects.Identity, rowset.StorageId, columnName, false);
		}

		internal bool IsDefaultValueRegistered(ClusteredRowset rowset, string columnName)
		{
			Table.TableSchema.DefaultValueCollection defaults = new Table.TableSchema.DefaultValueCollection();
			GetDefaultValues(rowset.StorageId, defaults);
			return defaults.ContainsKey(columnName);
		}

		internal void RegisterDefaultValue(ClusteredRowset rowset, string columnName, string scriptExpression, bool useInUpdate, string description)
		{
			if (IsDefaultValueRegistered(rowset, columnName))
				UnregisterDefaultValue(rowset, columnName, false);
			rowset.UpdateSchemaVersion();
			CreateObjectEntry(false, Database.VdbObjects.DefaultValue, rowset.StorageId, columnName, 0UL, Row.EmptyReference, useInUpdate ? 1L : 0L, true, scriptExpression, description, null, null);
		}

		internal void UnregisterDefaultValue(ClusteredRowset rowset, string columnName, bool checkRegistered)
		{
			if (checkRegistered && !IsDefaultValueRegistered(rowset, columnName))
				throw new VistaDBException(192, columnName);
			rowset.UpdateSchemaVersion();
			DeleteInScope(Database.VdbObjects.DefaultValue, rowset.StorageId, columnName, false);
		}

		internal bool IsConstraintRegistered(ClusteredRowset rowset, string name)
		{
			Table.TableSchema.ConstraintCollection constraints = new Table.TableSchema.ConstraintCollection();
			GetConstraints(rowset.StorageId, constraints);
			return constraints.ContainsKey(name);
		}

		internal void RegisterConstraint(ClusteredRowset rowset, string name, string expression, int options, string description)
		{
			if (IsConstraintRegistered(rowset, name))
				UnregisterConstraint(rowset, name, false);
			rowset.UpdateSchemaVersion();
			CreateObjectEntry(false, Database.VdbObjects.Constraint, rowset.StorageId, name, 0UL, Row.EmptyReference, options, false, expression, description, null, null);
		}

		internal void UnregisterConstraint(ClusteredRowset rowset, string name, bool checkRegistered)
		{
			if (checkRegistered && !IsConstraintRegistered(rowset, name))
				throw new VistaDBException(195, name);
			rowset.UpdateSchemaVersion();
			DeleteInScope(Database.VdbObjects.Constraint, rowset.StorageId, name, false);
		}

		internal void RegisterForeignKey(string constraintName, ClusteredRowset fkRowset, ClusteredRowset pkRowset, string foreignKey, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity, string description)
		{
			if (IsRelationshipRegistered(fkRowset, constraintName))
				throw new VistaDBException(204, constraintName);
			fkRowset.UpdateSchemaVersion();
			int num = Database.RelationshipCollection.RelationshipInformation.MakeOptions(updateIntegrity, deleteIntegrity);
			CreateObjectEntry(false, Database.VdbObjects.Relationship, fkRowset.StorageId, constraintName, 0UL, pkRowset.StorageId, num, true, foreignKey, description, null, null);
		}

		internal void UnregisterForeignKey(string constraintName, ClusteredRowset fkRowset)
		{
			if (!IsRelationshipRegistered(fkRowset, constraintName))
				throw new VistaDBException(205, constraintName);
			fkRowset.UpdateSchemaVersion();
			DeleteInScope(Database.VdbObjects.Relationship, fkRowset.StorageId, constraintName, false);
		}

		internal Row AllocateRowsetSchema(ulong storageId, Row rowInstance)
		{
			bool flag = !CaseSensitive;
			TraverseDiskObjects(Database.VdbObjects.Column, storageId, new Database.TraverseJobDelegate(LoadColumnsJob), rowInstance, flag);
			rowInstance.ReorderByIndex();
			return rowInstance;
		}

		internal bool LookForReferencedKey(ClusteredRowset lookingRowset, Row key, string tableName, string indexName)
		{
			Table triggeredTable = (Table)OpenClone(tableName, IsReadOnly);
			try
			{
				if (lookingRowset != triggeredTable.Rowset)
					lookingRowset.WrapperDatabase.AddTriggeredDependence(triggeredTable, !triggeredTable.IsClone);
				return triggeredTable.FindReference(key, indexName == null ? triggeredTable.PKIndex : indexName);
			}
			finally
			{
				ReleaseClone(triggeredTable);
			}
		}

		internal void ModifyForeignTable(bool update, VistaDB.Engine.Core.Indexing.Index primaryIndex, string tableName, string indexName, VistaDBReferentialIntegrity integrity)
		{
			ClusteredRowset parentRowset = primaryIndex.ParentRowset;
			Table triggeredTable = (Table)OpenClone(tableName, IsReadOnly);
			try
			{
				if (triggeredTable.Rowset != parentRowset)
					parentRowset.WrapperDatabase.AddTriggeredDependence(triggeredTable, !triggeredTable.IsClone);
				if (!update && integrity == VistaDBReferentialIntegrity.Cascade)
					triggeredTable.CascadeDeleteForeignTable(primaryIndex, indexName);
				else
					triggeredTable.CascadeUpdateForeignTable(primaryIndex, indexName, integrity);
			}
			finally
			{
				ReleaseClone(triggeredTable);
			}
		}

		internal ITable OpenClone(string name, bool readOnly, bool activateObjects)
		{
			Table table = descriptors.TableInstance(name, readOnly);
			if (table == null)
				table = (Table)OpenTable(name, false, readOnly, activateObjects);
			else
				table.AddCloneReference();
			return table;
		}

		internal ITable OpenClone(string name, bool readOnly)
		{
			return OpenClone(name, readOnly, true);
		}

		internal void ReleaseClone(ITable tbl)
		{
			if (tbl == null)
				return;
			Table table = (Table)tbl;
			if (!table.RemoveCloneReference() || table.Rowset.PostponedClosing)
				return;
			table.Dispose();
		}

		internal void ExportXml(string xmlFileName, VistaDBXmlWriteMode mode)
		{
			FileInfo fileInfo = new FileInfo(Name);
			string dataSetName = fileInfo.Name;
			if (dataSetName.EndsWith(fileInfo.Extension))
				dataSetName = dataSetName.Remove(0, dataSetName.Length - fileInfo.Extension.Length);
			DataSet database = new DataSet(dataSetName);
			FillOutDatabaseSchema(database);
			if (mode == VistaDBXmlWriteMode.SchemaOnly)
			{
				database.WriteXml(xmlFileName, XmlWriteMode.WriteSchema);
			}
			else
			{
				FillOutDatabaseData(database);
				database.WriteXml(xmlFileName, mode == VistaDBXmlWriteMode.DataOnly ? XmlWriteMode.IgnoreSchema : XmlWriteMode.WriteSchema);
			}
		}

		internal void ImportXml(string xmlFileName, VistaDBXmlReadMode mode, bool interruptOnError)
		{
			if (IsReadOnly)
				throw new VistaDBException(337, Name);
			DataSet database = new DataSet();
			if (mode == VistaDBXmlReadMode.DataOnly)
			{
				database.EnforceConstraints = false;
				FillOutDatabaseSchema(database);
				int num = (int)database.ReadXml(xmlFileName, XmlReadMode.IgnoreSchema);
			}
			else
			{
				int num = (int)database.ReadXml(xmlFileName, XmlReadMode.ReadSchema);
				FillInDatabaseSchema(database);
			}
			FillInDatabaseData(database);
		}

		internal void ImportXmlReader(XmlReader reader, VistaDBXmlReadMode mode, bool interruptOnError)
		{
			DataSet database = new DataSet();
			if (mode == VistaDBXmlReadMode.DataOnly)
			{
				database.EnforceConstraints = false;
				FillOutDatabaseSchema(database);
				int num = (int)database.ReadXml(reader, XmlReadMode.IgnoreSchema);
			}
			else
			{
				int num = (int)database.ReadXml(reader, XmlReadMode.ReadSchema);
				FillInDatabaseSchema(database);
			}
			FillInDatabaseData(database);
		}

		internal void AddToTransferList(string tableName)
		{
			string upper = tableName.ToUpper(Culture);
			if (transferList.Contains(upper))
				return;
			transferList.Add(upper, (ulong)transferList.Count);
		}

		internal void ClearTransferList()
		{
			transferList.Clear();
		}

		internal void RegisterViewObject(IView view)
		{
			LockStorage();
			try
			{
				int maxLength = DefaultRow[scriptValueIndex].MaxLength;
				string scriptValue;
				string s;
				if (view.Expression.Length > maxLength)
				{
					string str = view.Expression.Substring(0, maxLength);
					scriptValue = str.TrimEnd();
					s = view.Expression.Substring(maxLength);
					int count = str.Length - scriptValue.Length;
					if (count > 0)
						s = new string(' ', count) + s;
				}
				else
				{
					scriptValue = view.Expression;
					s = null;
				}
				DeleteInScope(Database.VdbObjects.View, Row.EmptyReference, view.Name, false);
				CreateObjectEntry(true, Database.VdbObjects.View, Row.EmptyReference, view.Name, Row.EmptyReference, Row.EmptyReference, 0L, true, scriptValue, view.Description, null, s == null ? null : Encoding.Unicode.GetBytes(s));
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void UnregisterViewObject(IView view)
		{
			LockStorage();
			try
			{
				DeleteInScope(Database.VdbObjects.View, Row.EmptyReference, view.Name, true);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal IViewList LoadViews()
		{
			LockStorage();
			try
			{
				if (_viewsCache.IsValid(Header))
					return _viewsCache.List;
				Database.ViewList viewList = new Database.ViewList();
				TraverseDiskObjects(Database.VdbObjects.View, Row.EmptyReference, new Database.TraverseJobDelegate(LoadViewsJob), (object)viewList);
				_viewsCache.Update(Header, viewList);
				return viewList;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void TestProcedureNameUniquness(string name)
		{
			if (LoadClrProcedureObjects().ContainsKey(name))
				throw new VistaDBException(381, name);
			if (LoadSqlStoredProcedures().ContainsKey(name))
				throw new VistaDBException(400, name);
			if (LoadSqlUserDefinedFunctions().ContainsKey(name))
				throw new VistaDBException(400, name);
		}

		internal void RegisterCLRProc(string procedureName, string clrHostedProcedure, string assemblyName, string description)
		{
			bool commit = false;
			LockStorage();
			try
			{
				if (clrHosting.IsProcedureActive(procedureName))
					throw new VistaDBException(381, procedureName);
				TestProcedureNameUniquness(procedureName);
				Database.AssemblyCollection.AssemblyInformation assemblyInformation = LocateAssembly(assemblyName, true);
				if (assemblyInformation == null)
					throw new VistaDBException(390, assemblyName);
				ClrHosting.ClrProcedure method = ClrHosting.GetMethod(clrHostedProcedure, assemblyInformation.RegisteredAssembly);
				CreateObjectEntry(false, Database.VdbObjects.CLRProcedure, Row.EmptyReference, procedureName, Row.EmptyReference, Row.EmptyReference, 0L, true, Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(clrHostedProcedure, method.Method.ToString()), description, Encoding.Unicode.GetBytes(assemblyName), null);
				clrHosting.AddProcedure(procedureName, method);
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 386, procedureName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal void UnregisterCLRProc(string procedureName, bool commit, IVistaDBClrProcedureInformation clrProc)
		{
			bool flag = false;
			LockStorage();
			try
			{
				if (clrHosting[procedureName] != null)
					clrHosting.Unregister(procedureName);
				else if (clrProc == null)
				{
					clrProc = LoadClrProcedureObjects()[procedureName];
					if (clrProc == null)
						throw new VistaDBException(383, procedureName);
				}
				DeleteInScope(Database.VdbObjects.CLRProcedure, Row.EmptyReference, procedureName, false);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 387, procedureName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal Database.ClrProcedureCollection LoadClrProcedureObjects()
		{
			LockStorage();
			try
			{
				Database.ClrProcedureCollection procedureCollection = new Database.ClrProcedureCollection();
				TraverseDiskObjects(Database.VdbObjects.CLRProcedure, Row.EmptyReference, new Database.TraverseJobDelegate(LoadClrProceduresJob), procedureCollection, Row.EmptyReference, VdbObjects.CLRProcedure);
				return procedureCollection;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal Database.ClrTriggerCollection LoadClrTriggerObjects()
		{
			LockStorage();
			try
			{
				Database.ClrTriggerCollection triggerCollection = new Database.ClrTriggerCollection();
				TraverseDiskObjects(Database.VdbObjects.CLRTrigger, 0UL, new Database.TraverseJobDelegate(LoadClrTriggersJob), triggerCollection, long.MaxValue, VdbObjects.CLRTrigger);
				return triggerCollection;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal Database.ClrTriggerCollection LoadClrTriggers(string tableName)
		{
			LockStorage();
			try
			{
				if (tableName == null)
				{
					Database.ClrTriggerCollection triggerCollection = LoadClrTriggerObjects();
					Database.TableIdMap tableIdMap = GetTableIdMap();
					triggerCollection.AssignTableReferences(tableIdMap, tableName);
					return triggerCollection;
				}
				ulong tableId = GetTableId(tableName, out string description, out Table.TableType type, true);
				Database.ClrTriggerCollection triggers = new Database.ClrTriggerCollection();
				GetClrTriggers(tableId, triggers, tableName);
				return triggers;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal bool TryGetProcedure(string procedureName, out ClrHosting.ClrProcedure procedure)
		{
			object obj = clrHosting[procedureName];
			if (obj != null)
			{
				procedure = (ClrHosting.ClrProcedure)obj;
				return true;
			}
			procedure = null;
			return false;
		}

		internal ClrHosting.ClrProcedure PrepareClrProcedureInvoke(string procedureName)
		{
			return PrepareInvoke(procedureName, clrHosting, Database.VdbObjects.CLRProcedure);
		}

		internal object InvokeClrProcedure(string procedureName, bool fillRow, params object[] parameters)
		{
			try
			{
				ClrHosting.ClrProcedure clrProcedure = PrepareClrProcedureInvoke(procedureName);
				return fillRow ? clrProcedure.ExecFillRow(parameters) : clrProcedure.Execute(parameters);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 388, procedureName);
			}
		}

		internal void CreateAssembly(string assemblyName, byte[] coffImage, string description, bool commit)
		{
			LockStorage();
			bool flag = false;
			try
			{
				if (clrHosting.IsAssemblyActive(assemblyName))
					throw new VistaDBException(389, assemblyName);
				if (LoadAssemblies(false).ContainsKey(assemblyName))
					throw new VistaDBException(389, assemblyName);
				Assembly assembly;
				try
				{
					assembly = ClrHosting.ActivateAssembly(coffImage, ParentConnection);
				}
				catch (FileNotFoundException ex)
				{
					assembly = Assembly.ReflectionOnlyLoad(coffImage);
				}
				string empty = string.Empty;
				foreach (AssemblyName referencedAssembly in assembly.GetReferencedAssemblies())
				{
					if (referencedAssembly.Name.Equals("VistaDB.4", StringComparison.OrdinalIgnoreCase) || referencedAssembly.Name.Equals("VistaDB.NET20", StringComparison.OrdinalIgnoreCase))
					{
						empty = referencedAssembly.Version.ToString();
						break;
					}
				}
				CreateObjectEntry(false, Database.VdbObjects.Assembly, Row.EmptyReference, assemblyName, Row.EmptyReference, Row.EmptyReference, 0L, true, Database.AssemblyCollection.AssemblyInformation.CompileScriptValue(assembly.FullName, assembly.ImageRuntimeVersion, empty), description, null, coffImage);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 391, assemblyName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal void UpdateAssembly(string assemblyName, byte[] coffImage, string description)
		{
			LockStorage();
			bool commit = false;
			try
			{
				if (!LoadAssemblies(false).ContainsKey(assemblyName))
				{
					CreateAssembly(assemblyName, coffImage, description, false);
					commit = true;
				}
				else
				{
					Assembly assembly;
					try
					{
						assembly = ClrHosting.ActivateAssembly(coffImage, ParentConnection);
					}
					catch (FileNotFoundException ex)
					{
						assembly = Assembly.ReflectionOnlyLoad(coffImage);
					}
					catch (Exception ex)
					{
						throw new VistaDBException(ex, 392, assemblyName);
					}
					string empty = string.Empty;
					foreach (AssemblyName referencedAssembly in assembly.GetReferencedAssemblies())
					{
						if (referencedAssembly.Name.Equals("VistaDB.4", StringComparison.OrdinalIgnoreCase) || referencedAssembly.Name.Equals("VistaDB.NET20", StringComparison.OrdinalIgnoreCase))
						{
							empty = referencedAssembly.Version.ToString();
							break;
						}
					}
					foreach (IVistaDBClrProcedureInformation clrProcedureObject in (IEnumerable<IVistaDBClrProcedureInformation>)LoadClrProcedureObjects())
					{
						if (ClrHosting.EqualNames(clrProcedureObject.AssemblyName, assemblyName))
							ClrHosting.GetMethod(clrProcedureObject.FullHostedName, assembly);
					}
					foreach (IVistaDBClrTriggerInformation clrTriggerObject in (IEnumerable<IVistaDBClrTriggerInformation>)LoadClrTriggerObjects())
					{
						if (ClrHosting.EqualNames(clrTriggerObject.AssemblyName, assemblyName))
							ClrHosting.GetMethod(clrTriggerObject.FullHostedName, assembly);
					}
					UpdateObjectEntry(false, Database.VdbObjects.Assembly, Row.EmptyReference, assemblyName, Row.EmptyReference, Row.EmptyReference, 0L, true, Database.AssemblyCollection.AssemblyInformation.CompileScriptValue(assembly.FullName, assembly.ImageRuntimeVersion, empty), description, null, coffImage);
					commit = true;
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 392, assemblyName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal void DeleteAssembly(string assemblyName, bool force)
		{
			LockStorage();
			bool commit = false;
			try
			{
				foreach (IVistaDBClrProcedureInformation clrProcedureObject in (IEnumerable<IVistaDBClrProcedureInformation>)LoadClrProcedureObjects())
				{
					if (ClrHosting.EqualNames(clrProcedureObject.AssemblyName, assemblyName))
					{
						if (!force)
							throw new VistaDBException(385, clrProcedureObject.Name);
						UnregisterCLRProc(clrProcedureObject.Name, false, clrProcedureObject);
					}
				}
				foreach (IVistaDBClrTriggerInformation clrTriggerObject in (IEnumerable<IVistaDBClrTriggerInformation>)LoadClrTriggerObjects())
				{
					if (ClrHosting.EqualNames(clrTriggerObject.AssemblyName, assemblyName))
					{
						if (!force)
							throw new VistaDBException(398, clrTriggerObject.Name);
						UnregisterCLRTrigger(clrTriggerObject.Name, false, clrTriggerObject);
					}
				}
				if (LocateAssembly(assemblyName, false) == null)
					throw new VistaDBException(390, assemblyName);
				DeleteInScope(Database.VdbObjects.Assembly, Row.EmptyReference, assemblyName, false);
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 393, assemblyName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal IVistaDBAssemblyCollection LoadAssemblies(bool instantiate)
		{
			LockStorage();
			try
			{
				Database.AssemblyCollection assemblyCollection = new Database.AssemblyCollection(instantiate);
				TraverseDiskObjects(Database.VdbObjects.Assembly, Row.EmptyReference, new Database.TraverseJobDelegate(LoadAssembliesJob), (object)assemblyCollection);
				return assemblyCollection;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void CopyViewsFrom(Database database)
		{
			foreach (IView loadView in database.LoadViews())
			{
				bool flag = true;
				if (database.sqlContext != null)
				{
					try
					{
						using (BatchStatement batchStatement = database.sqlContext.CreateBatchStatement(loadView.Expression, 0L))
						{
							int num = (int)batchStatement.PrepareQuery();
						}
					}
					catch (VistaDBException ex)
					{
						database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.ViewOperation, loadView.Name, "Unable to create view due to: " + ex.Message);
						flag = false;
					}
					catch (Exception ex)
					{
						database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.ViewOperation, loadView.Name, "Unable to create view due to: " + ex.Message);
						flag = false;
					}
				}
				if (flag)
					RegisterViewObject(loadView);
			}
		}

		internal void CopySqlProceduresAndFunctionsFrom(Database Database)
		{
			foreach (IStoredProcedureInformation sqlStoredProcedure in Database.LoadSqlStoredProcedures())
				RegisterStoredProcedure(sqlStoredProcedure);
			foreach (IUserDefinedFunctionInformation userDefinedFunction in Database.LoadSqlUserDefinedFunctions())
				RegisterUserDefinedFunction(userDefinedFunction);
		}

		internal void CopyClrProceduresAndTriggersFrom(Database Database)
		{
			IVistaDBAssemblyCollection assemblyCollection = Database.LoadAssemblies(false);
			foreach (IVistaDBAssemblyInformation assemblyInformation in assemblyCollection)
			{
				string vistadbVersion = string.Empty;
				if (assemblyInformation.VistaDBRuntimeVersion == null)
				{
					foreach (AssemblyName referencedAssembly in Assembly.ReflectionOnlyLoad(assemblyInformation.COFFImage).GetReferencedAssemblies())
					{
						if (referencedAssembly.Name.Equals("VistaDB.4", StringComparison.OrdinalIgnoreCase) || referencedAssembly.Name.Equals("VistaDB.NET20", StringComparison.OrdinalIgnoreCase))
						{
							vistadbVersion = referencedAssembly.Version.ToString();
							break;
						}
					}
				}
				else
					vistadbVersion = assemblyInformation.VistaDBRuntimeVersion;
				if (!string.IsNullOrEmpty(vistadbVersion) && !vistadbVersion.StartsWith("4.1."))
					Database.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.SqlProcOperation, assemblyInformation.Name, "Assembly is bound to a different verison of VistaDB [" + vistadbVersion + "] and will need to be replaced to run in this version [4.1].");
				ImportAssembly(assemblyInformation.Name, ((Database.AssemblyCollection.AssemblyInformation)assemblyInformation).COFFImage, assemblyInformation.FullName, assemblyInformation.ImageRuntimeVersion, vistadbVersion, assemblyInformation.Description);
			}
			foreach (IVistaDBClrProcedureInformation clrProcedureObject in (IEnumerable<IVistaDBClrProcedureInformation>)Database.LoadClrProcedureObjects())
			{
				if (assemblyCollection[clrProcedureObject.AssemblyName] != null)
					ImportCLRProc(clrProcedureObject.Name, clrProcedureObject.FullHostedName, clrProcedureObject.AssemblyName, clrProcedureObject.Description, clrProcedureObject.Signature);
			}
			foreach (IVistaDBClrTriggerInformation loadClrTrigger in (IEnumerable<IVistaDBClrTriggerInformation>)Database.LoadClrTriggers(null))
			{
				if (assemblyCollection[loadClrTrigger.AssemblyName] != null)
					ImportCLRTrigger(loadClrTrigger.Name, loadClrTrigger.FullHostedName, loadClrTrigger.AssemblyName, loadClrTrigger.TableName, loadClrTrigger.TriggerAction, loadClrTrigger.Description, loadClrTrigger.Signature);
			}
		}

		internal void RegisterCLRTrigger(string triggerName, string clrHostedTrigger, string assemblyName, string tableName, TriggerAction eventType, string description)
		{
			if ((eventType & TriggerAction.AfterDelete) != TriggerAction.AfterDelete && (eventType & TriggerAction.AfterUpdate) != TriggerAction.AfterUpdate && (eventType & TriggerAction.AfterInsert) != TriggerAction.AfterInsert)
				throw new VistaDBException(399, eventType.ToString());
			bool commit = false;
			LockStorage();
			try
			{
				ulong tableId = GetTableId(tableName, out string description1, out Table.TableType type, true);
				if (clrHostedTriggers.IsProcedureActive(triggerName))
					throw new VistaDBException(394, triggerName);
				if (LoadClrTriggerObjects().ContainsKey(triggerName))
					throw new VistaDBException(394, triggerName);
				Database.AssemblyCollection.AssemblyInformation assemblyInformation = LocateAssembly(assemblyName, true);
				if (assemblyInformation == null)
					throw new VistaDBException(390, assemblyName);
				ClrHosting.ClrProcedure method = ClrHosting.GetMethod(clrHostedTrigger, assemblyInformation.RegisteredAssembly);
				CreateObjectEntry(false, Database.VdbObjects.CLRTrigger, tableId, triggerName, Row.EmptyReference, Row.EmptyReference, (long)eventType, true, Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(clrHostedTrigger, method.Method.ToString()), description, Encoding.Unicode.GetBytes(assemblyName), null);
				clrHostedTriggers.AddProcedure(triggerName, method);
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 395, triggerName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal void UnregisterCLRTrigger(string triggerName, bool commit, IVistaDBClrTriggerInformation clrTrigger)
		{
			bool flag = false;
			LockStorage();
			try
			{
				if (clrHostedTriggers[triggerName] != null)
					clrHostedTriggers.Unregister(triggerName);
				if (clrTrigger == null)
				{
					IVistaDBClrTriggerCollection triggerCollection = LoadClrTriggerObjects();
					if (!triggerCollection.ContainsKey(triggerName))
						throw new VistaDBException(397, triggerName);
					clrTrigger = triggerCollection[triggerName];
				}
				DeleteInScope(Database.VdbObjects.CLRTrigger, ((Database.ClrTriggerCollection.ClrTriggerInformation)clrTrigger).ParentTableId, triggerName, false);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 396, triggerName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal ClrHosting.ClrProcedure PrepareCLRTriggerInvoke(string triggerName)
		{
			return PrepareInvoke(triggerName, clrHostedTriggers, Database.VdbObjects.CLRTrigger);
		}

		internal void InvokeCLRTrigger(string triggerName)
		{
			try
			{
				PrepareCLRTriggerInvoke(triggerName).Execute();
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 388, triggerName);
			}
		}

		internal void RegisterStoredProcedure(IStoredProcedureInformation sp)
		{
			bool commit = false;
			LockStorage();
			try
			{
				TestProcedureNameUniquness(sp.Name);
				CreateObjectEntry(false, Database.VdbObjects.StoredProcedure, Row.EmptyReference, sp.Name, Row.EmptyReference, Row.EmptyReference, 0L, true, null, sp.Description, null, sp.Serialize());
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 401, sp.Name);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal void UnregisterStoredProcedure(string procedureName, bool commit)
		{
			bool flag = false;
			LockStorage();
			try
			{
				DeleteInScope(Database.VdbObjects.StoredProcedure, Row.EmptyReference, procedureName, false);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 387, procedureName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal IStoredProcedureCollection LoadSqlStoredProcedures()
		{
			LockStorage();
			try
			{
				if (_spCache.IsValid(Header))
					return _spCache.List;
				Database.StoredProcedureCollection procedureCollection = new Database.StoredProcedureCollection();
				TraverseDiskObjects(Database.VdbObjects.StoredProcedure, Row.EmptyReference, new Database.TraverseJobDelegate(LoadSqlStoredProceduresJob), (object)procedureCollection);
				_spCache.Update(Header, procedureCollection);
				return procedureCollection;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void RegisterUserDefinedFunction(IUserDefinedFunctionInformation udf)
		{
			bool commit = false;
			LockStorage();
			try
			{
				TestProcedureNameUniquness(udf.Name);
				CreateObjectEntry(false, Database.VdbObjects.UDF, Row.EmptyReference, udf.Name, Row.EmptyReference, Row.EmptyReference, udf.ScalarValued ? 0L : 1L, true, null, udf.Description, null, udf.Serialize());
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 401, udf.Name);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal void UnregisterUserDefinedFunction(string udfName, bool commit)
		{
			bool flag = false;
			LockStorage();
			try
			{
				DeleteInScope(Database.VdbObjects.UDF, Row.EmptyReference, udfName, false);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 402, udfName);
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal IUserDefinedFunctionCollection LoadSqlUserDefinedFunctions()
		{
			LockStorage();
			try
			{
				if (_udfCache.IsValid(Header))
					return _udfCache.List;
				Database.UdfCollection udfCollection = new Database.UdfCollection();
				TraverseDiskObjects(Database.VdbObjects.UDF, Row.EmptyReference, new Database.TraverseJobDelegate(LoadSqlFunctionsJob), (object)udfCollection);
				_udfCache.Update(Header, udfCollection);
				return udfCollection;
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void AssignSQLContext(LocalSQLConnection localSQLConnection)
		{
			sqlContext = localSQLConnection;
		}

		internal LocalSQLConnection SQLContext
		{
			get
			{
				return sqlContext;
			}
		}

		internal TemporaryTable[] ActivateModificationTable(ClusteredRowset originRowset, TriggerAction triggerAction)
		{
			TemporaryTable[] temporaryTableArray = new TemporaryTable[2];
			int index = 0;
			string[] strArray = new string[2] { Table.TriggeredInsert, Table.TriggeredDelete };
			foreach (string name in strArray)
			{
				TemporaryTable instance = TemporaryTable.CreateInstance(name, this, originRowset.DefaultRow.CopyInstance());
				StorageHandle.StorageMode accessMode = new StorageHandle.StorageMode(FileMode.CreateNew, false, false, FileAccess.ReadWrite, false, true);
				instance.Rowset.CreateStorage(accessMode, 0UL, true);
				temporaryTableArray[index] = instance;
				++index;
			}
			return temporaryTableArray;
		}

		private List<Table> OpenLogClones()
		{
			IVistaDBTableNameCollection tableIdMap = GetTableIdMap();
			List<Table> tableList = new List<Table>(tableIdMap.Count);
			foreach (string name in tableIdMap)
			{
				Table table = (Table)OpenClone(name, false);
				if (table.Rowset.TransactionLog == null)
				{
					ReleaseClone(table);
				}
				else
				{
					tableList.Add(table);
					table.Rowset.LockStorage();
				}
			}
			return tableList;
		}

		private void ReleaseLogClones(List<Table> instances)
		{
			foreach (Table instance in instances)
			{
				instance.Rowset.UnlockStorage(true);
				ReleaseClone(instance);
			}
		}

		private void ExecCommitTransaction()
		{
			List<Table> instances = OpenLogClones();
			try
			{
				LockStorage();
				bool commit = false;
				try
				{
					foreach (Table table in instances)
					{
						TransactionLogRowset transactionLog = table.Rowset.TransactionLog;
						int extraRows = 0;
						if (transactionLog.Commit(TransactionId, ref extraRows))
							table.Rowset.UpdateCommitedState(false, extraRows);
					}
					if (TransactionLog != null)
					{
						int extraRows = 0;
						if (TransactionLog.Commit(TransactionId, ref extraRows))
							UpdateCommitedState(false, extraRows);
					}
					Header.CommitTransaction();
					FlushStorageVersion();
					commit = true;
				}
				finally
				{
					UnlockStorage(false);
					FinalizeChanges(!commit, commit);
				}
			}
			finally
			{
				ReleaseLogClones(instances);
			}
		}

		private void ExecRollbackTransaction()
		{
			List<Table> instances = OpenLogClones();
			try
			{
				LockStorage();
				bool commit = false;
				try
				{
					foreach (Table table in instances)
					{
						table.Rowset.TransactionLog.Rollback(TransactionId);
						table.Rowset.UpdateRollbackedState(false);
					}
					if (TransactionLog != null)
						TransactionLog.Rollback(TransactionId);
					Header.RollbackTransaction();
					FlushStorageVersion();
					commit = true;
				}
				finally
				{
					UnlockStorage(false);
					FinalizeChanges(!commit, commit);
				}
			}
			finally
			{
				ReleaseLogClones(instances);
			}
		}

		private void CreateThisTPLock(uint transactionId)
		{
		}

		private void DropThisTPLock(uint transactionId)
		{
		}

		private void ClearOrphanTransactions()
		{
		}

		internal void BeginTransaction(IsolationLevel level)
		{
			if (IsReadOnly)
				return;
			LockStorage();
			bool commit = false;
			try
			{
				TpIsolationLevel = level;
				if (Header.CurrentTransactionId != 0U)
					throw new VistaDBException(457);
				Header.BeginTransaction();
				CreateThisTPLock(TransactionId);
				FlushStorageVersion();
				commit = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 450);
			}
			finally
			{
				if (!commit)
					TpIsolationLevel = IsolationLevel.ReadCommitted;
				UnlockStorage(false);
				FinalizeChanges(!commit, commit);
			}
		}

		internal void CommitTransaction()
		{
			if (IsReadOnly)
				return;
			LockStorage();
			uint transactionId = TransactionId;
			try
			{
				if (!IsTransaction)
					throw new VistaDBException(459);
				ExecCommitTransaction();
				DropThisTPLock(transactionId);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 451);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void RollbackTransaction()
		{
			if (IsReadOnly)
				return;
			LockStorage();
			uint transactionId = TransactionId;
			try
			{
				if (!IsTransaction)
					throw new VistaDBException(458);
				ExecRollbackTransaction();
				DropThisTPLock(transactionId);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 452);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal override uint TransactionId
		{
			get
			{
				return ((Database.DatabaseHeader)base.Header).CurrentTransactionId;
			}
		}

		internal override IsolationLevel TpIsolationLevel
		{
			get
			{
				return tpIsolationLevel;
			}
			set
			{
				if (value != IsolationLevel.ReadCommitted)
					throw new VistaDBException(456, value.ToString());
				tpIsolationLevel = value;
			}
		}

		internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
		{
			return base.DoGettingAnotherTransactionStatus(transactionId);
		}

		internal override TransactionLogRowset DoCreateTpLog(bool commit)
		{
			return CreateTransactionLogTable(this, commit);
		}

		internal override TransactionLogRowset DoOpenTpLog(ulong logHeaderPostion)
		{
			return OpenTransactionLogTable(this, logHeaderPostion);
		}

		internal TransactionLogRowset OpenTransactionLogTable(ClusteredRowset rowset, ulong logTableId)
		{
			if (logTableId == 0UL)
				return null;
			StorageHandle.StorageMode openMode = new StorageHandle.StorageMode(FileMode.Open, rowset.IsShared, false, rowset.IsReadOnly ? FileAccess.Read : FileAccess.ReadWrite, Handle.Transacted, Handle.IsolatedStorage);
			try
			{
				rowset.LockStorage();
				try
				{
					TransactionLogRowset instance = TransactionLogRowset.CreateInstance(this, rowset.Name);
					instance.OpenStorage(openMode, logTableId);
					return instance;
				}
				finally
				{
					rowset.UnlockStorage(true);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 454, rowset.Name);
			}
		}

		internal TransactionLogRowset CreateTransactionLogTable(ClusteredRowset rowset, bool commit)
		{
			if (!IsTransaction)
				return null;
			StorageHandle.StorageMode accessMode = new StorageHandle.StorageMode(FileMode.CreateNew, rowset.IsShared, false, rowset.IsReadOnly ? FileAccess.Read : FileAccess.ReadWrite, Handle.Transacted, Handle.IsolatedStorage);
			bool flag = false;
			try
			{
				rowset.LockStorage();
				try
				{
					ulong freeCluster = GetFreeCluster(1);
					TransactionLogRowset instance = TransactionLogRowset.CreateInstance(this, rowset.Name);
					instance.CreateStorage(accessMode, freeCluster, commit);
					rowset.Header.TransactionLogPosition = freeCluster;
					rowset.FlushStorageVersion();
					flag = true;
					return instance;
				}
				finally
				{
					rowset.UnlockStorage(false);
					rowset.FinalizeChanges(!flag, commit);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 453, rowset.Name);
			}
		}

		internal override Guid Originator
		{
			get
			{
				return Header.SchemaVersionGuid;
			}
		}

		private string LookForTombstone(string relatedTableName, out bool tombstone)
		{
			string name = ClusteredRowset.SyncExtension.TombstoneTablenamePrefix + relatedTableName;
			ulong tableId = GetTableId(name, out string description, out Table.TableType type, false);
			tombstone = type == Table.TableType.Tombstone;
			if ((long)tableId == (long)Row.EmptyReference)
				return null;
			return name;
		}

		private void CreateAnchorTable()
		{
			if ((long)GetTableId(ClusteredRowset.SyncExtension.AnchorTablename, out string description, out Table.TableType type, false) == (long)Row.EmptyReference)
				CreateTable(ClusteredRowset.SyncExtension.GetAnchorSchema(this), true, IsReadOnly, false).Dispose();
			else if (type != Table.TableType.Anchor)
				throw new VistaDBException(213, ClusteredRowset.SyncExtension.AnchorTablename);
		}

		private bool DropAnchorTable()
		{
			IVistaDBTableNameCollection tableIdMap = GetTableIdMap();
			Table.TableSchema tableSchema1 = null;
			foreach (string tableName in tableIdMap)
			{
				Table.TableSchema tableSchema2 = GetTableSchema(tableName, false);
				if (tableSchema2.Type == Table.TableType.Anchor)
					tableSchema1 = tableSchema2;
				else if (tableSchema2.ContainsSyncPart)
					return false;
			}
			if (tableSchema1 != null)
				DropTable(tableSchema1.Name, true, false, false, false);
			return true;
		}

		private void InsertAnchorRow(string tableName)
		{
			ITable tbl = OpenClone(ClusteredRowset.SyncExtension.AnchorTablename, IsReadOnly);
			try
			{
				tbl.SetFilter(ClusteredRowset.SyncExtension.SyncTableName + "='" + tableName + "'", true);
				tbl.First();
				if (!tbl.EndOfTable)
					return;
				tbl.Insert();
				tbl.PutString(ClusteredRowset.SyncExtension.SyncTableName, tableName);
				tbl.Post();
			}
			finally
			{
				ReleaseClone(tbl);
			}
		}

		private void DeleteAnchorRow(string tableName)
		{
			ITable tbl = OpenClone(ClusteredRowset.SyncExtension.AnchorTablename, IsReadOnly);
			if (tbl == null)
				return;
			((Table)tbl).Rowset.AllowSyncEdit = true;
			try
			{
				tbl.SetFilter(ClusteredRowset.SyncExtension.SyncTableName + "='" + tableName + "'", true);
				tbl.First();
				while (!tbl.EndOfTable)
					tbl.Delete();
			}
			finally
			{
				((Table)tbl).Rowset.AllowSyncEdit = false;
				ReleaseClone(tbl);
			}
		}

		internal void ActivateSyncService(string tableName)
		{
			LockStorage();
			try
			{
				Table.TableSchema tableSchema = GetTableSchema(tableName, false);
				if (tableSchema.Type != Table.TableType.Default)
					throw new VistaDBException(208, tableName);
				string oldName = LookForTombstone(tableName, out bool tombstone);
				if (oldName != null && !tombstone)
					throw new VistaDBException(212, tableName);
				if (AppendSyncStructure(tableSchema))
					AlterTable(tableSchema.Name, tableSchema, true, false);
				Table.TableSchema newSchema = new Table.TableSchema(ClusteredRowset.SyncExtension.TombstoneTablenamePrefix + tableSchema.Name, Table.TableType.Tombstone, ClusteredRowset.SyncExtension.TombstoneTableDescription + tableSchema.Name, 0UL, this);
				foreach (Row.Column column in tableSchema)
					newSchema.AddNewColumn(column.Duplicate(false));
				if (oldName != null)
					AlterTable(oldName, newSchema, true, false);
				else
					CreateTable(newSchema, true, IsReadOnly, false).Dispose();
				CreateAnchorTable();
				InsertAnchorRow(tableName);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 210, tableName);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal void DeactivateSyncService(string tableName)
		{
			LockStorage();
			try
			{
				Table.TableSchema tableSchema = GetTableSchema(tableName, false);
				if (tableSchema.Type != Table.TableType.Default)
					throw new VistaDBException(208, tableName);
				if (DeleteSyncStructure(tableSchema))
					AlterTable(tableSchema.Name, tableSchema, true, false);
				string name = LookForTombstone(tableName, out bool tombstone);
				if (name != null && tombstone)
					DropTable(name, true, false, false, false);
				if (DropAnchorTable())
					return;
				DeleteAnchorRow(tableName);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 211, tableName);
			}
			finally
			{
				UnlockStorage(true);
			}
		}

		internal IColumn GetTimestampAnchor(string tableName)
		{
			Table table = (Table)OpenClone(tableName, true);
			if (table == null)
				return null;
			try
			{
				if (!table.Rowset.DefaultRow.HasTimestamp || !table.Rowset.ActiveSyncService)
					return null;
				Row.Column emptyColumnInstance = CreateEmptyColumnInstance(VistaDBType.Timestamp);
				emptyColumnInstance.Value = (long)table.Rowset.Header.CurrentTimestampId;
				return emptyColumnInstance;
			}
			finally
			{
				ReleaseClone(table);
			}
		}

		internal enum VdbObjects
		{
			Table = 1,
			Index = 2,
			Column = 3,
			Constraint = 4,
			DefaultValue = 5,
			Identity = 6,
			Relationship = 7,
			Trigger = 8,
			Description = 9,
			View = 10, // 0x0000000A
			CLRProcedure = 11, // 0x0000000B
			Assembly = 12, // 0x0000000C
			CLRTrigger = 13, // 0x0000000D
			StoredProcedure = 14, // 0x0000000E
			UDF = 15, // 0x0000000F
		}

		internal class DatabaseObject : IVistaDBDatabaseObject
		{
			private Database.VdbObjects id;
			private string name;
			private string description;

			internal DatabaseObject(Database.VdbObjects id, string name, string description)
			{
				this.id = id;
				this.name = name;
				this.description = description;
			}

			internal static bool EqualNames(string name1, string name2)
			{
				return Database.DatabaseObject.EqualNames(name1, name2, true);
			}

			internal static bool EqualNames(string name1, string name2, bool ignoreCase)
			{
				if (name1 != null)
				{
					name1 = name1.TrimEnd(char.MinValue, ' ');
					if (name1[0] == '[')
					{
						int length = name1[name1.Length - 1] == ']' ? name1.Length - 2 : name1.Length - 1;
						name1 = name1.Substring(1, length);
						name1 = name1.TrimEnd(char.MinValue, ' ');
					}
				}
				if (name2 != null)
				{
					name2 = name2.TrimEnd(char.MinValue, ' ');
					if (name2[0] == '[')
					{
						int length = name2[name2.Length - 1] == ']' ? name2.Length - 2 : name2.Length - 1;
						name2 = name2.Substring(1, length);
						name2 = name2.TrimEnd(char.MinValue, ' ');
					}
				}
				return string.Compare(name1, name2, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal) == 0;
			}

			public string Name
			{
				get
				{
					return name;
				}
				set
				{
					name = value;
				}
			}

			public string Description
			{
				get
				{
					return description;
				}
				set
				{
					description = value == null || value.Length == 0 ? null : value;
				}
			}

			public override bool Equals(object obj)
			{
				return Database.DatabaseObject.EqualNames(name, ((Database.DatabaseObject)obj).Name);
			}

			public override int GetHashCode()
			{
				return base.GetHashCode();
			}
		}

		private class Descriptors : Dictionary<ulong, Table>
		{
			internal Descriptors()
			{
			}

			internal Table TableInstance(string name, bool readOnly)
			{
				ulong num = 0;
				Table table1 = null;
				foreach (Table table2 in Values)
				{
					if (!table2.IsClosed && Database.DatabaseObject.EqualNames(table2.Name, name) && table2.ModificationVersion >= num && (!table2.IsReadOnly || readOnly))
					{
						table1 = table2;
						num = table2.ModificationVersion;
					}
				}
				return table1;
			}

			internal Table TableInstance(string name)
			{
				foreach (Table table in Values)
				{
					if (!table.IsClosed && Database.DatabaseObject.EqualNames(table.Name, name))
						return table;
				}
				return null;
			}

			internal void CheckExclusiveInstance(string name, bool exclusive)
			{
				Table table = TableInstance(name);
				if (table != null && (exclusive || !table.Rowset.IsShared))
					throw new VistaDBException(109, name);
			}

			internal bool NotifyChangedEnvironment(Connection.Settings variable, object newValue)
			{
				foreach (Table table in Values)
				{
					if (table != null && table.Rowset != null && !table.NotifyChangedEnvironment(variable, newValue))
						return false;
				}
				return true;
			}
		}

		private class IdentityCollection : Dictionary<string, List<Row.Column>>
		{
			private string lastTable;

			internal IdentityCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal Row.Column GetLastIdentity(string tableName, string columnName)
			{
				if (tableName == null)
				{
					if (lastTable == null)
						return null;
					tableName = lastTable;
				}
				if (!ContainsKey(tableName))
					return null;
				List<Row.Column> columnList = this[tableName];
				if (columnList == null)
					return null;
				if (columnName == null && columnList.Count > 0)
					return columnList[0];
				foreach (Row.Column column in columnList)
				{
					if (Database.DatabaseObject.EqualNames(column.Name, columnName))
						return column;
				}
				return null;
			}

			internal void SetLastIdentity(string tableName, Row row)
			{
				if (tableName == null || !ContainsKey(tableName))
					return;
				foreach (Row.Column column in this[tableName])
					column.Value = row[column.RowIndex].Value;
				lastTable = tableName;
			}

			internal void ActivateTableIdentity(string tableName, Row.Column column)
			{
				List<Row.Column> columnList;
				if (ContainsKey(tableName))
				{
					columnList = this[tableName];
				}
				else
				{
					columnList = new List<Row.Column>();
					Add(tableName, columnList);
				}
				foreach (Row.Column column1 in columnList)
				{
					if (column1.RowIndex == column.RowIndex)
						return;
				}
				columnList.Add(column.Duplicate(false));
			}

			internal void DeactivateTableIdentities(string tableName)
			{
				Remove(tableName);
			}

			public new void Clear()
			{
				lastTable = null;
				base.Clear();
			}
		}

		internal class ViewList : InsensitiveHashtable, IViewList, IDictionary, ICollection, IEnumerable
		{
			internal ViewList()
			{
			}

			internal void Add(string viewName, string script, byte[] scriptExtension, string description)
			{
				Add(viewName, new Database.ViewList.View(viewName, script, scriptExtension, description));
			}

			internal class View : Database.DatabaseObject, IView, IVistaDBDatabaseObject
			{
				private string expression;

				internal View(string name)
				  : base(Database.VdbObjects.View, name, null)
				{
				}

				internal View(string name, string script, byte[] scriptExtension, string description)
				  : base(Database.VdbObjects.View, name, description)
				{
					expression = scriptExtension == null ? script : script + Encoding.Unicode.GetString(scriptExtension, 0, scriptExtension.Length);
				}

				public string Expression
				{
					get
					{
						return expression;
					}
					set
					{
						expression = value;
					}
				}

				public override bool Equals(object obj)
				{
					if (base.Equals(obj))
						return string.Compare(expression, ((Database.ViewList.View)obj).expression, StringComparison.OrdinalIgnoreCase) == 0;
					return false;
				}

				public override int GetHashCode()
				{
					return base.GetHashCode();
				}
			}
		}

		internal class TableIdMap : IEnumerable<KeyValuePair<ulong, string>>, IVistaDBTableNameCollection, ICollection<string>, IEnumerable<string>, IEnumerable
		{
			private Dictionary<ulong, string> id2Name;
			private Dictionary<string, string> names;
			private CultureInfo culture;

			internal TableIdMap(CultureInfo cultureInfo, int capacity)
			{
				culture = cultureInfo;
				id2Name = new Dictionary<ulong, string>(capacity);
				names = new Dictionary<string, string>(capacity);
			}

			internal TableIdMap(CultureInfo cultureInfo)
			  : this(cultureInfo, 25)
			{
			}

			internal TableIdMap(int capacity)
			  : this(CultureInfo.InvariantCulture, capacity)
			{
			}

			internal TableIdMap()
			  : this(CultureInfo.InvariantCulture)
			{
			}

			internal ICollection<string> Keys
			{
				get
				{
					return id2Name.Values;
				}
			}

			internal string GetTableNameFromID(ulong ID)
			{
				if (!id2Name.TryGetValue(ID, out string str))
					return null;
				return str;
			}

			internal void Add(string key, ulong value)
			{
				id2Name.Add(value, key);
				names.Add(culture.TextInfo.ToUpper(key), key);
			}

			internal void AddTable(string tableName)
			{
				if (string.IsNullOrEmpty(tableName))
					return;
				names.Add(culture.TextInfo.ToUpper(tableName), tableName);
			}

			internal void Clear()
			{
				id2Name.Clear();
				names.Clear();
			}

			internal bool Contains(string key)
			{
				return id2Name.ContainsValue(key);
			}

			internal bool ContainsID(ulong ID)
			{
				return id2Name.ContainsKey(ID);
			}

			internal int Count
			{
				get
				{
					return id2Name.Count;
				}
			}

			void ICollection<string>.Add(string item)
			{
				throw new NotSupportedException();
			}

			void ICollection<string>.Clear()
			{
				throw new NotSupportedException();
			}

			bool ICollection<string>.Contains(string item)
			{
				return names.ContainsKey(culture.TextInfo.ToUpper(item));
			}

			void ICollection<string>.CopyTo(string[] array, int arrayIndex)
			{
				names.Values.CopyTo(array, arrayIndex);
			}

			int ICollection<string>.Count
			{
				get
				{
					return names.Count;
				}
			}

			bool ICollection<string>.IsReadOnly
			{
				get
				{
					return true;
				}
			}

			bool ICollection<string>.Remove(string item)
			{
				return false;
			}

			IEnumerator<string> IEnumerable<string>.GetEnumerator()
			{
				return names.Values.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return names.Values.GetEnumerator();
			}

			public IEnumerator<KeyValuePair<ulong, string>> GetEnumerator()
			{
				return id2Name.GetEnumerator();
			}
		}

		private class EnumCache<T> where T : IEnumerable
		{
			public int Version { get; private set; }

			public DateTime ForceRefreshAfter { get; private set; }

			public T List { get; private set; }

			public bool IsValid(Database.DatabaseHeader header)
			{
				return List != null && (Version.Equals(header.LastVersion) && !(DateTime.Now > ForceRefreshAfter));
			}

			public void Update(Database.DatabaseHeader header, T list)
			{
				Version = header.LastVersion;
				ForceRefreshAfter = DateTime.Now.AddSeconds(10.0);
				List = list;
			}
		}

		private class EnumKeyedCache<T> : Dictionary<ulong, Database.EnumKeyedCache<T>.Entry> where T : IDictionary
		{
			public bool TryFillFromCache(ulong key, Database.DatabaseHeader header, T list)
			{
				if (!TryGetValue(key, out Entry entry) || !entry.Version.Equals(header.LastVersion) || DateTime.Now.Subtract(header.LastRefresh).TotalSeconds > 10.0)
					return false;
				entry.CopyInto(list);
				return true;
			}

			public bool AddToCache(ulong key, Database.DatabaseHeader header, T list)
			{
				if (ContainsKey(key))
					this[key] = new Database.EnumKeyedCache<T>.Entry(header.LastVersion, list);
				else
					Add(key, new Database.EnumKeyedCache<T>.Entry(header.LastVersion, list));
				return true;
			}

			internal struct Entry
			{
				private int _version;
				private T _list;

				public Entry(int version, T list)
				{
					_version = version;
					_list = list;
				}

				public int Version
				{
					get
					{
						return _version;
					}
				}

				public T List
				{
					get
					{
						return _list;
					}
				}

				public void CopyInto(T list)
				{
					foreach (object key in _list.Keys)
						list.Add(key, this._list[key]);
				}
			}
		}

		internal class StoredProcedureCollection : VistaDBKeyedCollection<string, IStoredProcedureInformation>, IStoredProcedureCollection, IVistaDBKeyedCollection<string, IStoredProcedureInformation>, ICollection<IStoredProcedureInformation>, IEnumerable<IStoredProcedureInformation>, IEnumerable
		{
			internal StoredProcedureCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal void AddProcedure(string name, byte[] scriptData, string description, bool status)
			{
				Add(name, new Database.StoredProcedureCollection.StoredProcedureInformation(name, scriptData, description, status));
			}

			internal class StoredProcedureInformation : Database.DatabaseObject, IStoredProcedureInformation, IVistaDBDatabaseObject
			{
				private string script;

				protected StoredProcedureInformation(Database.VdbObjects type, string name, string script, string description)
				  : base(type, name, description)
				{
					this.script = script;
				}

				internal StoredProcedureInformation(string name, byte[] scriptData, string description, bool status)
				  : this(Database.VdbObjects.StoredProcedure, name, Encoding.Unicode.GetString(scriptData, 0, scriptData.Length), description)
				{
				}

				internal StoredProcedureInformation(string name, string script, string description)
				  : this(Database.VdbObjects.StoredProcedure, name, script, description)
				{
				}

				string IStoredProcedureInformation.Statement
				{
					get
					{
						return script;
					}
				}

				byte[] IStoredProcedureInformation.Serialize()
				{
					return Encoding.Unicode.GetBytes(script);
				}
			}
		}

		internal class UdfCollection : VistaDBKeyedCollection<string, IUserDefinedFunctionInformation>, IUserDefinedFunctionCollection, IVistaDBKeyedCollection<string, IUserDefinedFunctionInformation>, ICollection<IUserDefinedFunctionInformation>, IEnumerable<IUserDefinedFunctionInformation>, IEnumerable
		{
			internal UdfCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal void AddFunction(string name, byte[] scriptData, bool scalarValued, string description, bool status)
			{
				Add(name, new Database.UdfCollection.UserDefinedFunction(name, scriptData, scalarValued, description, status));
			}

			internal class UserDefinedFunction : Database.StoredProcedureCollection.StoredProcedureInformation, IUserDefinedFunctionInformation, IStoredProcedureInformation, IVistaDBDatabaseObject
			{
				private bool scalarValued = true;

				internal UserDefinedFunction(string name, byte[] scriptData, bool scalarValued, string description, bool status)
				  : base(Database.VdbObjects.UDF, name, Encoding.Unicode.GetString(scriptData, 0, scriptData.Length), description)
				{
					this.scalarValued = scalarValued;
				}

				internal UserDefinedFunction(string name, string script, bool scalarValued, string description)
				  : base(Database.VdbObjects.UDF, name, script, description)
				{
					this.scalarValued = scalarValued;
				}

				bool IUserDefinedFunctionInformation.ScalarValued
				{
					get
					{
						return scalarValued;
					}
				}
			}
		}

		internal class ClrProcedureCollection : VistaDBKeyedCollection<string, IVistaDBClrProcedureInformation>, IVistaDBClrProcedureCollection, IVistaDBKeyedCollection<string, IVistaDBClrProcedureInformation>, ICollection<IVistaDBClrProcedureInformation>, IEnumerable<IVistaDBClrProcedureInformation>, IEnumerable
		{
			private bool activeList;

			internal ClrProcedureCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal bool InAction
			{
				get
				{
					return activeList;
				}
				set
				{
					activeList = value;
				}
			}

			internal void AddProcedure(string name, string fullSignature, string description, byte[] assemblyFullName, bool status)
			{
				Add(name, new Database.ClrProcedureCollection.ClrProcedureInformation(name, fullSignature, description, assemblyFullName, status));
			}

			internal void DropByFullName(string fullName)
			{
				foreach (IVistaDBClrProcedureInformation procedureInformation in Values)
				{
					if (ClrHosting.EqualNames(procedureInformation.FullHostedName, fullName))
					{
						Remove(procedureInformation.Name);
						break;
					}
				}
			}

			internal class ClrProcedureInformation : Database.DatabaseObject, IVistaDBClrProcedureInformation, IVistaDBDatabaseObject
			{
				private string signature;
				private string fullCLRProcedureName;
				private bool activeStatus;
				private string assemblyName;

				internal ClrProcedureInformation(string name, string fullSignature, string description, byte[] assemblyName, bool status)
				  : this(Database.VdbObjects.CLRProcedure, name, fullSignature, description, assemblyName, status)
				{
				}

				protected ClrProcedureInformation(Database.VdbObjects type, string name, string fullSignature, string description, byte[] assemblyName, bool status)
				  : base(type, name, description)
				{
					DecompileSignature(fullSignature);
					this.assemblyName = Encoding.Unicode.GetString(assemblyName, 0, assemblyName.Length);
					activeStatus = status;
				}

				public string Signature
				{
					get
					{
						return signature;
					}
				}

				public string AssemblyName
				{
					get
					{
						return assemblyName;
					}
				}

				public string FullHostedName
				{
					get
					{
						return fullCLRProcedureName;
					}
				}

				internal static string CompileSignature(string fullCLRProcedureName, string signature)
				{
					return fullCLRProcedureName + char.MinValue + signature;
				}

				private void DecompileSignature(string fullSignature)
				{
					int length = fullSignature.IndexOf(char.MinValue);
					fullCLRProcedureName = fullSignature.Substring(0, length);
					signature = fullSignature.Substring(length + 1);
				}

				internal bool Active
				{
					get
					{
						return activeStatus;
					}
				}

				public override bool Equals(object obj)
				{
					if (base.Equals(obj) && string.Compare(signature, ((Database.ClrProcedureCollection.ClrProcedureInformation)obj).signature, StringComparison.OrdinalIgnoreCase) == 0 && ClrHosting.EqualNames(assemblyName, ((Database.ClrProcedureCollection.ClrProcedureInformation)obj).assemblyName))
						return string.Compare(fullCLRProcedureName, ((Database.ClrProcedureCollection.ClrProcedureInformation)obj).fullCLRProcedureName, StringComparison.OrdinalIgnoreCase) == 0;
					return false;
				}

				public override int GetHashCode()
				{
					return base.GetHashCode();
				}
			}
		}

		internal class ClrTriggerCollection : VistaDBKeyedCollection<string, IVistaDBClrTriggerInformation>, IVistaDBClrTriggerCollection, IVistaDBKeyedCollection<string, IVistaDBClrTriggerInformation>, ICollection<IVistaDBClrTriggerInformation>, IEnumerable<IVistaDBClrTriggerInformation>, IEnumerable
		{
			private bool activeList;

			internal ClrTriggerCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal bool InAction
			{
				get
				{
					return activeList;
				}
				set
				{
					activeList = value;
				}
			}

			internal void AddTrigger(string name, string fullSignature, string description, byte[] assemblyFullName, bool status, ulong reference, long option)
			{
				Add(name, new Database.ClrTriggerCollection.ClrTriggerInformation(name, fullSignature, description, assemblyFullName, status, reference, option));
			}

			internal void AddTrigger(Database.ClrTriggerCollection.ClrTriggerInformation trigger)
			{
				Add(trigger.Name, new Database.ClrTriggerCollection.ClrTriggerInformation(trigger.Name, Database.ClrProcedureCollection.ClrProcedureInformation.CompileSignature(trigger.FullHostedName, trigger.Signature), trigger.Description, Encoding.Unicode.GetBytes(trigger.AssemblyName), trigger.Active, trigger.ParentTableId, (long)((IVistaDBClrTriggerInformation)trigger).TriggerAction));
			}

			internal void DropByFullName(string fullName)
			{
				foreach (Database.ClrTriggerCollection.ClrTriggerInformation triggerInformation in Values)
				{
					if (ClrHosting.EqualNames(triggerInformation.FullHostedName, fullName))
					{
						Remove(triggerInformation.Name);
						break;
					}
				}
			}

			internal void AssignTableReferences(Database.TableIdMap tables, string tableName)
			{
				List<string> stringList = new List<string>();
				foreach (Database.ClrTriggerCollection.ClrTriggerInformation triggerInformation in Values)
				{
					string tableNameFromId = tables.GetTableNameFromID(triggerInformation.ParentTableId);
					if (tableName != null && !Database.DatabaseObject.EqualNames(tableName, tableNameFromId))
						stringList.Add(triggerInformation.Name);
					else
						triggerInformation.ParentTable = tableNameFromId;
				}
				foreach (string key in stringList)
					Remove(key);
			}

			internal class ClrTriggerInformation : Database.DatabaseObject, IVistaDBClrTriggerInformation, IVistaDBDatabaseObject
			{
				private string signature;
				private string fullCLRProcedureName;
				private bool activeStatus;
				private string assemblyName;
				private ulong reference;
				private string parentTable;
				private long triggerEvent;

				internal ClrTriggerInformation(string name, string fullSignature, string description, byte[] assemblyName, bool status, ulong reference, long options)
				  : this(Database.VdbObjects.CLRTrigger, name, fullSignature, description, assemblyName, status)
				{
					this.reference = reference;
					triggerEvent = options;
				}

				protected ClrTriggerInformation(Database.VdbObjects type, string name, string fullSignature, string description, byte[] assemblyName, bool status)
				  : base(type, name, description)
				{
					DecompileSignature(fullSignature);
					this.assemblyName = Encoding.Unicode.GetString(assemblyName, 0, assemblyName.Length);
					activeStatus = status;
				}

				string IVistaDBClrTriggerInformation.Name
				{
					get
					{
						return Name;
					}
				}

				string IVistaDBClrTriggerInformation.Signature
				{
					get
					{
						return signature;
					}
				}

				string IVistaDBClrTriggerInformation.AssemblyName
				{
					get
					{
						return assemblyName;
					}
				}

				string IVistaDBClrTriggerInformation.FullHostedName
				{
					get
					{
						return fullCLRProcedureName;
					}
				}

				string IVistaDBClrTriggerInformation.TableName
				{
					get
					{
						return parentTable;
					}
				}

				TriggerAction IVistaDBClrTriggerInformation.TriggerAction
				{
					get
					{
						return (TriggerAction)triggerEvent;
					}
				}

				string IVistaDBDatabaseObject.Name
				{
					get
					{
						return Name;
					}
				}

				string IVistaDBDatabaseObject.Description
				{
					get
					{
						return Description;
					}
				}

				internal ulong ParentTableId
				{
					get
					{
						return reference;
					}
				}

				internal string ParentTable
				{
					set
					{
						parentTable = value;
					}
				}

				public string Signature
				{
					get
					{
						return signature;
					}
				}

				public string AssemblyName
				{
					get
					{
						return assemblyName;
					}
				}

				public string FullHostedName
				{
					get
					{
						return fullCLRProcedureName;
					}
				}

				internal static string CompileSignature(string fullCLRProcedureName, string signature)
				{
					return fullCLRProcedureName + char.MinValue + signature;
				}

				private void DecompileSignature(string fullSignature)
				{
					int length = fullSignature.IndexOf(char.MinValue);
					fullCLRProcedureName = fullSignature.Substring(0, length);
					signature = fullSignature.Substring(length + 1);
				}

				internal bool Active
				{
					get
					{
						return activeStatus;
					}
				}

				public override bool Equals(object obj)
				{
					if (base.Equals(obj) && string.Compare(signature, ((Database.ClrTriggerCollection.ClrTriggerInformation)obj).signature, StringComparison.OrdinalIgnoreCase) == 0 && ClrHosting.EqualNames(assemblyName, ((Database.ClrTriggerCollection.ClrTriggerInformation)obj).assemblyName))
						return string.Compare(fullCLRProcedureName, ((Database.ClrTriggerCollection.ClrTriggerInformation)obj).fullCLRProcedureName, StringComparison.OrdinalIgnoreCase) == 0;
					return false;
				}

				public override int GetHashCode()
				{
					return base.GetHashCode();
				}
			}
		}

		internal class AssemblyCollection : VistaDBKeyedCollection<string, IVistaDBAssemblyInformation>, IVistaDBAssemblyCollection, IVistaDBKeyedCollection<string, IVistaDBAssemblyInformation>, ICollection<IVistaDBAssemblyInformation>, IEnumerable<IVistaDBAssemblyInformation>, IEnumerable
		{
			private bool instantiateAssemblies;

			internal AssemblyCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal AssemblyCollection(bool instantiateAssemblies)
			  : base(StringComparer.OrdinalIgnoreCase)
			{
				this.instantiateAssemblies = instantiateAssemblies;
			}

			internal void AddAssembly(string assemblyName, byte[] assemblyBody, string script, string description, DirectConnection parentConnection)
			{
				Add(assemblyName, new Database.AssemblyCollection.AssemblyInformation(assemblyName, assemblyBody, instantiateAssemblies, script, description, parentConnection));
			}

			internal class AssemblyInformation : Database.DatabaseObject, IVistaDBAssemblyInformation, IVistaDBDatabaseObject
			{
				private string fullName;
				private string runtimeVersion;
				private string vistaDBVersion;
				private DirectConnection parentConnection;
				private IVistaDBClrProcedureCollection procedures;
				private IVistaDBClrTriggerCollection triggers;
				private Assembly assembly;
				private byte[] image;

				internal AssemblyInformation(string name, byte[] assemblyBody, bool instantiate, string scriptValue, string description, DirectConnection parentConnection)
				  : base(Database.VdbObjects.Assembly, name, description)
				{
					this.parentConnection = parentConnection;
					DecompileScriptValue(scriptValue);
					image = assemblyBody;
					if (instantiate && !string.IsNullOrEmpty(vistaDBVersion) && !vistaDBVersion.StartsWith("4.1."))
						throw new VistaDBException(390, "Assembly " + Name + " references as different version of VistaDB and cannot be loaded.");
					assembly = instantiate ? ClrHosting.ActivateAssembly(assemblyBody, parentConnection) : null;
				}

				internal Assembly RegisteredAssembly
				{
					get
					{
						return assembly;
					}
				}

				public byte[] COFFImage
				{
					get
					{
						byte[] numArray = new byte[image.Length];
						image.CopyTo(numArray, 0);
						return numArray;
					}
				}

				public string FullName
				{
					get
					{
						return fullName;
					}
				}

				public string ImageRuntimeVersion
				{
					get
					{
						return runtimeVersion;
					}
				}

				public string VistaDBRuntimeVersion
				{
					get
					{
						return vistaDBVersion;
					}
				}

				public IVistaDBClrProcedureCollection Procedures
				{
					get
					{
						if (procedures != null)
							return procedures;
						if (assembly == null)
						{
							if (!string.IsNullOrEmpty(vistaDBVersion) && !vistaDBVersion.StartsWith("4.1."))
								throw new VistaDBException(390, "Assembly " + Name + " references a different version of VistaDB and cannot be loaded. Recompile your assembly against this version of VistaDB.");
							assembly = ClrHosting.ActivateAssembly(image, parentConnection);
						}
						procedures = ClrHosting.ListClrProcedures(Name, assembly);
						return procedures;
					}
				}

				public IVistaDBClrTriggerCollection Triggers
				{
					get
					{
						if (triggers != null)
							return triggers;
						if (assembly == null)
						{
							if (!string.IsNullOrEmpty(vistaDBVersion) && !vistaDBVersion.StartsWith("4.1."))
								throw new VistaDBException(390, "Assembly " + Name + " references as different version of VistaDB and cannot be loaded.");
							assembly = ClrHosting.ActivateAssembly(image, parentConnection);
						}
						triggers = ClrHosting.ListClrTriggers(Name, assembly);
						return triggers;
					}
				}

				internal static string CompileScriptValue(string fullName, string runtimeVersion, string vistaDBVersion)
				{
					if (vistaDBVersion == null)
						return fullName + char.MinValue + runtimeVersion;
					return fullName + char.MinValue + runtimeVersion + char.MinValue + vistaDBVersion;
				}

				private void DecompileScriptValue(string script)
				{
					string[] strArray = script.Split(new char[1]);
					fullName = strArray[0];
					runtimeVersion = strArray[1];
					if (strArray.Length <= 2)
						return;
					vistaDBVersion = strArray[2];
				}

				public override bool Equals(object obj)
				{
					if (base.Equals(obj) && string.Compare(fullName, ((Database.AssemblyCollection.AssemblyInformation)obj).fullName, StringComparison.OrdinalIgnoreCase) == 0)
						return string.Compare(runtimeVersion, ((Database.AssemblyCollection.AssemblyInformation)obj).runtimeVersion, StringComparison.OrdinalIgnoreCase) == 0;
					return false;
				}

				public override int GetHashCode()
				{
					return base.GetHashCode();
				}
			}
		}

		internal class RelationshipCollection : VistaDBKeyedCollection<string, IVistaDBRelationshipInformation>, IVistaDBRelationshipCollection, IVistaDBKeyedCollection<string, IVistaDBRelationshipInformation>, ICollection<IVistaDBRelationshipInformation>, IEnumerable<IVistaDBRelationshipInformation>, IEnumerable
		{
			public RelationshipCollection()
			  : base(StringComparer.OrdinalIgnoreCase)
			{
			}

			internal class RelationshipInformation : Database.DatabaseObject, IVistaDBRelationshipInformation, IVistaDBDatabaseObject
			{
				private ulong foreignTableId;
				private string foreignTable;
				private string foreignKey;
				private ulong primaryTableId;
				private string primaryTable;
				private string primaryKey;
				private int options;

				internal RelationshipInformation(string name, ulong foreignTableId, string foreignKey, ulong primaryTableId, int options, string description)
				  : base(Database.VdbObjects.Relationship, name, description)
				{
					this.foreignTableId = foreignTableId;
					foreignTable = null;
					this.foreignKey = foreignKey;
					this.primaryTableId = primaryTableId;
					primaryTable = null;
					primaryKey = null;
					this.options = options;
				}

				internal static int MakeOptions(VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity)
				{
					return (int)(updateIntegrity | (VistaDBReferentialIntegrity)((int)deleteIntegrity << 8));
				}

				internal string ForeignTable
				{
					get
					{
						return foreignTable;
					}
					set
					{
						foreignTable = value;
					}
				}

				internal string PrimaryTable
				{
					get
					{
						return primaryTable;
					}
					set
					{
						primaryTable = value;
					}
				}

				internal string PrimaryKey
				{
					get
					{
						return primaryKey;
					}
					set
					{
						primaryKey = value;
					}
				}

				internal ulong PrimaryTableId
				{
					get
					{
						return primaryTableId;
					}
				}

				internal ulong ForeignTableId
				{
					get
					{
						return foreignTableId;
					}
					set
					{
						foreignTableId = value;
					}
				}

				internal string ForeignKey
				{
					get
					{
						return foreignKey;
					}
					set
					{
						foreignKey = value;
					}
				}

				internal int Options
				{
					get
					{
						return options;
					}
				}

				string IVistaDBRelationshipInformation.ForeignTable
				{
					get
					{
						return foreignTable;
					}
				}

				string IVistaDBRelationshipInformation.PrimaryTable
				{
					get
					{
						return primaryTable;
					}
				}

				string IVistaDBRelationshipInformation.ForeignKey
				{
					get
					{
						return foreignKey;
					}
				}

				VistaDBReferentialIntegrity IVistaDBRelationshipInformation.UpdateIntegrity
				{
					get
					{
						return (VistaDBReferentialIntegrity)(options & 15);
					}
				}

				VistaDBReferentialIntegrity IVistaDBRelationshipInformation.DeleteIntegrity
				{
					get
					{
						return (VistaDBReferentialIntegrity)(options >> 8 & 15);
					}
				}

				public override bool Equals(object obj)
				{
					if (base.Equals(obj) && options == ((Database.RelationshipCollection.RelationshipInformation)obj).options && ((long)foreignTableId == (long)((Database.RelationshipCollection.RelationshipInformation)obj).foreignTableId && (long)primaryTableId == (long)((Database.RelationshipCollection.RelationshipInformation)obj).primaryTableId))
						return string.Compare(foreignKey, ((Database.RelationshipCollection.RelationshipInformation)obj).foreignKey, StringComparison.OrdinalIgnoreCase) == 0;
					return false;
				}

				public override int GetHashCode()
				{
					return base.GetHashCode();
				}
			}
		}

		internal class DatabaseHeader : ClusteredRowset.ClusteredRowsetHeader
		{
			internal static readonly string FileCopyrightString = "VistaDB " + string.Format("{0}.{1}", "4", "3") + " (C) 2009 VistaDB Software, Inc. All rights reserved. Provider: " + string.Format("{0}.{1}.{2}", "4", "3", "34");
			private Guid schemaVersionGuid = Guid.Empty;
			private const short CurrentFileVersion = 10;
			private const int EndianStamp = 1945218526;
			private int databaseFileSchemaVersionHeaderPosition;
			private int NumberColumnsHeaderPosition;
			private int encryptionKeyMd5HeaderPosition;
			private int freeSpaceMapLocationHeaderPosition;
			private int schemaVersionGuidHeaderPosition;
			private int transactionIdHeaderPosition;
			private uint currentTransactionIdValue;
			private int numHeaderColumns;
			private int endianPosition;
			private int _initHeaderCount;

			internal static Database.DatabaseHeader CreateInstance(DataStorage parentDatabase, int pageSize, CultureInfo culture)
			{
				return new Database.DatabaseHeader(parentDatabase, VistaDB.Engine.Core.Header.HeaderId.DATABASE_HEADER, VistaDB.Engine.Core.Indexing.Index.Type.Unique, pageSize, culture);
			}

			protected DatabaseHeader(DataStorage parentDatabase, VistaDB.Engine.Core.Header.HeaderId id, VistaDB.Engine.Core.Indexing.Index.Type type, int pageSize, CultureInfo culture)
			  : base(parentDatabase, id, type, pageSize, culture)
			{
				OnDatabaseHeaderInit();
				AppendColumn(new SmallIntColumn(0));
				encryptionKeyMd5HeaderPosition = AppendColumn(new BigIntColumn(0L));
				AppendColumn(new BigIntColumn(0L));
				databaseFileSchemaVersionHeaderPosition = AppendColumn(new SmallIntColumn(10));
				NumberColumnsHeaderPosition = AppendColumn(new SmallIntColumn(0));
				freeSpaceMapLocationHeaderPosition = AppendColumn(new BigIntColumn(0L));
				AppendColumn(new CharColumn(Database.DatabaseHeader.FileCopyrightString, 120, 1252, CultureInfo.InvariantCulture, false));
				schemaVersionGuidHeaderPosition = AppendColumn(new BigIntColumn(0L));
				AppendColumn(new BigIntColumn(0L));
				endianPosition = AppendColumn(new IntColumn(1945218526));
				transactionIdHeaderPosition = AppendColumn(new BigIntColumn(0L));
				numHeaderColumns = Count;
			}

			internal ushort DatabaseFileSchemaVersion
			{
				get
				{
					return (ushort)(short)this[databaseFileSchemaVersionHeaderPosition].Value;
				}
			}

			internal Md5.Signature EncryptionKeyMd5
			{
				get
				{
					return new Md5.Signature((long)this[encryptionKeyMd5HeaderPosition].Value, (long)this[encryptionKeyMd5HeaderPosition + 1].Value);
				}
				set
				{
					this[encryptionKeyMd5HeaderPosition].Value = value.Low;
					this[encryptionKeyMd5HeaderPosition + 1].Value = value.High;
					Modified = true;
				}
			}

			internal int ColumnCount
			{
				get
				{
					return (short)this[NumberColumnsHeaderPosition].Value;
				}
				set
				{
					Modified = ColumnCount != value;
					this[NumberColumnsHeaderPosition].Value = (short)value;
				}
			}

			internal ulong FreeSpaceMapLocation
			{
				get
				{
					return (ulong)(long)this[freeSpaceMapLocationHeaderPosition].Value;
				}
				set
				{
					this[freeSpaceMapLocationHeaderPosition].Value = (long)value;
					Modified = true;
				}
			}

			internal int NameSpace
			{
				get
				{
					return PageSize / 4;
				}
			}

			internal uint CurrentTransactionId
			{
				get
				{
					return currentTransactionIdValue;
				}
			}

			internal Guid SchemaVersionGuid
			{
				get
				{
					if (schemaVersionGuid != Guid.Empty)
						return schemaVersionGuid;
					byte[] numArray = new byte[16];
					VdbBitConverter.GetBytes((ulong)(long)this[schemaVersionGuidHeaderPosition].Value, numArray, 0, 8);
					VdbBitConverter.GetBytes((ulong)(long)this[schemaVersionGuidHeaderPosition + 1].Value, numArray, 8, 8);
					schemaVersionGuid = new Guid(numArray);
					return schemaVersionGuid;
				}
				set
				{
					schemaVersionGuid = value;
					byte[] byteArray = value.ToByteArray();
					this[schemaVersionGuidHeaderPosition].Value = BitConverter.ToInt64(byteArray, 0);
					this[schemaVersionGuidHeaderPosition + 1].Value = BitConverter.ToInt64(byteArray, 8);
				}
			}

			internal bool EndianMatches
			{
				get
				{
					if (endianPosition < 0)
						return true;
					return (int)this[endianPosition].Value == 1945218526;
				}
			}

			private void AppendColumnInfo(byte type, short maxLength, int codePage, bool allowNull, bool packed, bool readOnly, bool encrypted, string name, bool syncColumn)
			{
				bool allowNull1 = true;
				bool readOnly1 = true;
				bool encrypted1 = ParentStorage.Encryption != null;
				bool packed1 = false;
				TinyIntColumn emptyColumnInstance1 = (TinyIntColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.TinyInt);
				emptyColumnInstance1.Value = type;
				AppendColumn(emptyColumnInstance1);
				SmallIntColumn emptyColumnInstance2 = (SmallIntColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.SmallInt);
				emptyColumnInstance2.Value = maxLength;
				AppendColumn(emptyColumnInstance2);
				IntColumn emptyColumnInstance3 = (IntColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.Int);
				emptyColumnInstance3.Value = codePage;
				AppendColumn(emptyColumnInstance3);
				BitColumn emptyColumnInstance4 = (BitColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.Bit);
				emptyColumnInstance4.Value = allowNull;
				AppendColumn(emptyColumnInstance4);
				BitColumn emptyColumnInstance5 = (BitColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.Bit);
				emptyColumnInstance5.Value = packed;
				AppendColumn(emptyColumnInstance5);
				BitColumn emptyColumnInstance6 = (BitColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.Bit);
				emptyColumnInstance6.Value = readOnly;
				AppendColumn(emptyColumnInstance6);
				BitColumn emptyColumnInstance7 = (BitColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.Bit);
				emptyColumnInstance7.Value = encrypted;
				AppendColumn(emptyColumnInstance7);
				BigIntColumn emptyColumnInstance8 = (BigIntColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.BigInt);
				AppendColumn(emptyColumnInstance8);
				emptyColumnInstance8.AssignAttributes(null, allowNull1, readOnly1, encrypted1, packed1);
				if (syncColumn)
					emptyColumnInstance8.Value = 1;
				NCharColumn emptyColumnInstance9 = (NCharColumn)ParentStorage.CreateEmptyColumnInstance(VistaDBType.NChar, (short)NameSpace, NCharColumn.Utf16CodePage, true, false);
				emptyColumnInstance9.Value = name;
				emptyColumnInstance9.AssignAttributes(null, allowNull1, readOnly1, encrypted1, packed1);
				AppendColumn(emptyColumnInstance9);
			}

			private uint CurrentTp
			{
				get
				{
					return (uint)(long)this[transactionIdHeaderPosition].Value;
				}
				set
				{
					this[transactionIdHeaderPosition].Value = value | (long)TpCounter << 32;
					Modified = true;
				}
			}

			internal uint TpCounter
			{
				get
				{
					return (uint)((long)this[transactionIdHeaderPosition].Value >> 32);
				}
				set
				{
					this[transactionIdHeaderPosition].Value = CurrentTp | (long)value << 32;
					Modified = true;
				}
			}

			internal void BeginTransaction()
			{
				currentTransactionIdValue = CurrentTp == 0U ? 1U : CurrentTp + 2U;
				CurrentTp = currentTransactionIdValue;
				++TpCounter;
			}

			internal void CommitTransaction()
			{
				currentTransactionIdValue = 0U;
				--TpCounter;
			}

			internal void RollbackTransaction()
			{
				currentTransactionIdValue = 0U;
				--TpCounter;
			}

			protected override void OnActivate(ulong position)
			{
				Position = 0UL;
				try
				{
					base.OnActivate(position);
				}
				catch (VistaDBException ex)
				{
					bool handled = false;
					OnActivateError(position, ex, ref handled);
					if (!handled)
						throw;
				}
				catch
				{
					throw;
				}
				ReinitializeCulture();
			}

			protected override Row OnAllocateDefaultRow(Row rowInstance)
			{
				int num1 = 0;
				int num2 = numHeaderColumns;
				for (; num1 < ColumnCount; ++num1)
				{
					int index1 = num2;
					int num3 = index1 + 1;
					Row.Column column1 = this[index1];
					int index2 = num3;
					int num4 = index2 + 1;
					Row.Column column2 = this[index2];
					int index3 = num4;
					int num5 = index3 + 1;
					Row.Column column3 = this[index3];
					int index4 = num5;
					int num6 = index4 + 1;
					Row.Column column4 = this[index4];
					int index5 = num6;
					int num7 = index5 + 1;
					Row.Column column5 = this[index5];
					int index6 = num7;
					int num8 = index6 + 1;
					Row.Column column6 = this[index6];
					int index7 = num8;
					int num9 = index7 + 1;
					Row.Column column7 = this[index7];
					int index8 = num9;
					int num10 = index8 + 1;
					Row.Column column8 = this[index8];
					bool syncColumn = ((column8.IsNull ? 0L : (long)column8.Value) & 1L) == 1L;
					int index9 = num10;
					num2 = index9 + 1;
					Row.Column column9 = this[index9];
					Row.Column emptyColumnInstance = ParentStorage.CreateEmptyColumnInstance((VistaDBType)(byte)column1.Value, (short)column2.Value, (int)column3.Value, true, syncColumn);
					emptyColumnInstance.AssignAttributes((string)column9.Value, (bool)column4.Value, (bool)column6.Value, (bool)column7.Value, (bool)column5.Value);
					rowInstance.AppendColumn(emptyColumnInstance);
				}
				rowInstance.InstantiateComparingMask();
				int[] comparingMask = rowInstance.ComparingMask;
				comparingMask[0] = 1;
				comparingMask[1] = 2;
				comparingMask[2] = 3;
				return rowInstance;
			}

			protected override void OnCreateSchema(IVistaDBTableSchema schema)
			{
				ColumnCount = schema.ColumnCount;
				foreach (Row.Column column in schema)
					AppendColumnInfo((byte)column.Type, column.ExtendedType || column.FixedType ? (short)0 : (short)column.MaxLength, column.CodePage, column.AllowNull, column.Packed, column.ReadOnly, column.Encrypted, column.Name, column.IsSync);
			}

			protected override bool OnActivateSchema()
			{
				Database parentStorage = (Database)ParentStorage;
				if (EncryptionKeyMd5.Empty)
				{
					if (parentStorage.EncryptionKey.Key != null && parentStorage.EncryptionKey.Key.Length > 0)
						throw new VistaDBException(116, parentStorage.Name);
					parentStorage.ResetEncryption();
				}
				else if (!EncryptionKeyMd5.Equals(parentStorage.EncryptionKey.Md5Signature))
					throw new VistaDBException(116, parentStorage.Name);
				if (DatabaseFileSchemaVersion == 0)
					throw new VistaDBException(105, "Database file header reports invalid file version.  Database is corrupt.");
				bool isOutofDate = DatabaseFileSchemaVersion < 10;
				OnActivateSchemaVersionCheck(ref isOutofDate);
				if (isOutofDate)
				{
					if (!parentStorage.packOperationMode)
						throw new VistaDBException(50);
					parentStorage.upgradeFileVersionMode = true;
					parentStorage.upgradeFTSStructure = true;
				}
				else if (DatabaseFileSchemaVersion > 10)
					throw new VistaDBException(54, "Engine version does not support this file version");
				int num1 = EndianMatches ? 1 : 0;
				int length = Buffer.Length;
				if (PageSize > length)
				{
					ParentStorage.Handle.ResetPageSize(PageSize);
					return true;
				}
				int num2 = 0;
				for (int columnCount = ColumnCount; num2 < columnCount; ++num2)
					AppendColumnInfo(0, 0, 0, true, true, true, true, null, false);
				if (GetMemoryApartment(null) > length)
					return true;
				UnformatRowBuffer();
				return false;
			}

			protected override void OnRegisterSchema(IVistaDBTableSchema schema)
			{
				SchemaVersionGuid = Guid.NewGuid();
			}

			private bool FileSchemaVersionOutOfDate()
			{
				return DatabaseFileSchemaVersion == 10;
			}

			private static Database.DatabaseHeader CreateVdb3Instance(Database.DatabaseHeader header)
			{
				header = new Database.DatabaseHeader(header.ParentStorage, (VistaDB.Engine.Core.Header.HeaderId)header.RowId, (VistaDB.Engine.Core.Indexing.Index.Type)header.Signature, header.PageSize, header.Culture);
				header.IsVdb3Header = true;
				Row.Column column1 = header[header.encryptionKeyMd5HeaderPosition];
				Row.Column column2 = header[header.encryptionKeyMd5HeaderPosition + 1];
				Row.Column column3 = header[header.databaseFileSchemaVersionHeaderPosition];
				Row.Column column4 = header[header.NumberColumnsHeaderPosition];
				Row.Column column5 = header[header.freeSpaceMapLocationHeaderPosition];
				Row.Column column6 = header[header.freeSpaceMapLocationHeaderPosition + 1];
				Row.Column column7 = header[header.schemaVersionGuidHeaderPosition];
				Row.Column column8 = header[header.schemaVersionGuidHeaderPosition + 1];
				Row.Column column9 = header[header.transactionIdHeaderPosition];
				header.RemoveRange(header._initHeaderCount, header.Count - header._initHeaderCount);
				header.endianPosition = -1;
				header.databaseFileSchemaVersionHeaderPosition = header.AppendColumn(column3);
				header.encryptionKeyMd5HeaderPosition = header.AppendColumn(column1);
				header.AppendColumn(column2);
				header.NumberColumnsHeaderPosition = header.AppendColumn(column4);
				header.freeSpaceMapLocationHeaderPosition = header.AppendColumn(column5);
				header.AppendColumn(column6);
				header.schemaVersionGuidHeaderPosition = header.AppendColumn(column7);
				header.AppendColumn(column8);
				header.AppendColumn(new BigIntColumn(0L));
				header.transactionIdHeaderPosition = header.AppendColumn(column9);
				header.numHeaderColumns = header.Count;
				return header;
			}

			private void OnDatabaseHeaderInit()
			{
				_initHeaderCount = Count;
			}

			internal bool IsVdb3Header { get; private set; }

			private void OnActivateError(ulong position, Exception ex, ref bool handled)
			{
				VistaDBException vistaDbException = ex as VistaDBException;
				handled = false;
				if (vistaDbException == null || vistaDbException.ErrorId != 105 || !(ex.InnerException is ArgumentOutOfRangeException))
					return;
				if (IsVdb3Header)
					throw new VistaDBException(51);
				ParentStorage.SwapHeader(Database.DatabaseHeader.CreateVdb3Instance(this));
				ParentStorage.Header.Activate(position);
				handled = true;
			}

			private void OnActivateSchemaVersionCheck(ref bool isOutofDate)
			{
				isOutofDate |= IsVdb3Header;
			}
		}

		private class TemporaryTables : Stack, IDisposable
		{
			private bool isDisposed;

			internal void Push(Table temporaryTable)
			{
				Push((object)temporaryTable);
			}

			internal void Pop()
			{
				((Table)base.Pop()).Dispose();
			}

			public void Dispose()
			{
				if (isDisposed)
					return;
				isDisposed = true;
				GC.SuppressFinalize(this);
				foreach (Table table in this)
					table.Dispose();
				Clear();
			}
		}

		private delegate bool TraverseJobDelegate(Row currentRow, params object[] hints);
	}
}
