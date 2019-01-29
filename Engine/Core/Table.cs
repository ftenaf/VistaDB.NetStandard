





using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Indexing;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
	internal class Table : Dictionary<string, VistaDB.Engine.Core.Indexing.Index>, ITable, IVistaDBTable, IDisposable
	{
		internal static readonly string TriggeredInsert = "INSERTED";
		internal static readonly string TriggeredDelete = "DELETED";
		public static readonly int MaxColumns = 1024;
		private static readonly string exportConstraint = "___exporting___";
		private ulong currentModificationCounter;
		private ulong tableId;
		private Database parentDatabase;
		private ClusteredRowset rowset;
		private VistaDB.Engine.Core.Indexing.Index activeOrder;
		private VistaDB.Engine.Core.Indexing.Index pkOrder;
		private Table.Status rowStatus;
		private Table.ContextStack context;
		private int clones;
		private bool isDisposed;

		internal static Table.TableType ParseType(string type)
		{
			if (type == null)
				return Table.TableType.Default;
			Table.TableType tableType = Table.TableType.Default;
			while (tableType < Table.TableType.Unknown && string.Compare(tableType.ToString(), type, StringComparison.OrdinalIgnoreCase) != 0)
				++tableType;
			return tableType;
		}

		internal static Table CreateInstance(string name, Database parentDatabase, ClusteredRowset clonedRowset, Table.TableType type)
		{
			return new Table(ClusteredRowset.CreateInstance(name, parentDatabase, clonedRowset, type), parentDatabase);
		}

		protected Table(ClusteredRowset mainRowset, Database parentDatabase)
		  : base(StringComparer.OrdinalIgnoreCase)
		{
			tableId = ++Database.UniqueId;
			activeOrder = rowset = mainRowset;
			this.parentDatabase = parentDatabase;
			context = new Table.ContextStack(this);
			AddOrder("_natural_", rowset);
		}

		internal ulong ModificationVersion
		{
			get
			{
				return currentModificationCounter;
			}
		}

		internal ClusteredRowset Rowset
		{
			get
			{
				return rowset;
			}
		}

		internal VistaDB.Engine.Core.Indexing.Index ActiveOrder
		{
			get
			{
				return activeOrder;
			}
		}

		internal DirectConnection ParentConnection
		{
			get
			{
				if (rowset != null)
					return rowset.ParentConnection;
				return null;
			}
		}

		private Table.Status RowStatus
		{
			set
			{
				if (value == Table.Status.SynchPosition || rowStatus == Table.Status.SynchPosition && value == Table.Status.NoAction)
				{
					rowStatus = value;
				}
				else
				{
					if (value == Table.Status.InsertBlank)
						Rowset.PrepareEditStatus();
					if (value == Table.Status.Update)
					{
						switch (rowStatus)
						{
							case Table.Status.Insert:
								return;
							case Table.Status.InsertBlank:
								rowStatus = Table.Status.Insert;
								return;
							default:
								if (rowStatus != Table.Status.Update)
								{
									Rowset.PrepareEditStatus();
									break;
								}
								break;
						}
					}
					rowStatus = value;
				}
			}
		}

		internal string PKIndex
		{
			get
			{
				if (pkOrder != null)
					return pkOrder.Alias;
				return null;
			}
		}

		internal bool FTSIndexExists
		{
			get
			{
				foreach (VistaDB.Engine.Core.Indexing.Index index in Values)
				{
					if (index != null && index.IsFts)
						return true;
				}
				return false;
			}
		}

		private Row CurrentKey
		{
			get
			{
				SyncPositionStatus();
				return ActiveOrder.CurrentRow;
			}
			set
			{
				GoNearestKey(value);
			}
		}

		public Row CurrentRow
		{
			get
			{
				SyncPositionStatus();
				return Rowset.CurrentRow;
			}
			set
			{
				try
				{
					SyncPositionStatus();
					RowStatus = Table.Status.Update;
					Rowset.SatelliteRow.ClearEditStatus();
					foreach (IColumn column in value)
					{
						if (column.Modified)
							Rowset.PutColumnValue(column.RowIndex, column.Type, Rowset.SatelliteRow, column);
					}
				}
				catch (Exception ex)
				{
					RowStatus = Table.Status.NoAction;
					throw ex;
				}
			}
		}

		internal string Name
		{
			get
			{
				if (rowset != null)
					return rowset.Name;
				return null;
			}
		}

		internal void AddOrder(string name, VistaDB.Engine.Core.Indexing.Index order)
		{
			if (order.IsPrimary)
				pkOrder = order;
			Add(name, order);
		}

		private void RemoveOrder(VistaDB.Engine.Core.Indexing.Index order)
		{
			Remove(order.Alias);
			order.Dispose();
			if (pkOrder != order)
				return;
			pkOrder = null;
		}

		private VistaDB.Engine.Core.Indexing.Index GetOrder(string name)
		{
			return GetOrder(name, true);
		}

		private VistaDB.Engine.Core.Indexing.Index GetOrder(string name, bool raiseError)
		{
			if (name == null)
				return Rowset;
			if (ContainsKey(name))
				return this[name];
			if (raiseError)
				throw new VistaDBException(sbyte.MaxValue, name);
			return null;
		}

		private void ActivateDependencies(ClusteredRowset rowset, DataStorage exceptStorage)
		{
			rowset.MaskRelationships(rowset, exceptStorage, false);
			try
			{
				rowset.ActivateLinks();
			}
			finally
			{
				rowset.MaskRelationships(rowset, exceptStorage, true);
			}
		}

		private bool LookForKey(Row key, VistaDB.Engine.Core.Indexing.Index index, bool partialMatching, bool softPosition, bool releaseIndexLock)
		{
			ClusteredRowset rowset = Rowset;
			rowset.LockStorage();
			try
			{
				index.LockStorage();
				try
				{
					if (key == null)
						return false;
					try
					{
						if (index.SeekRow(key, partialMatching))
							return true;
						if (!softPosition)
							index.Bottom();
						index.ActivateLinks();
						return softPosition && !index.EndOfSet;
					}
					finally
					{
						if (releaseIndexLock)
							ActivateDependencies(rowset, index);
					}
				}
				catch (Exception ex)
				{
					throw new VistaDBException(ex, 270);
				}
				finally
				{
					index.UnlockStorage(releaseIndexLock);
				}
			}
			finally
			{
				rowset.UnlockStorage(releaseIndexLock);
			}
		}

		private ulong ExpectedIndexingLoops(int totalKeyLen)
		{
			ulong num = SortSpool.EstimateMemory() / ((ulong)(totalKeyLen + 100) * 10UL);
			if (num < 2000UL)
				num = 2000UL;
			return num;
		}

		internal void BuildIndexes(bool forceRegistering)
		{
			bool commit = false;
			DataStorage rowset = Rowset;
			Database wrapperDatabase = Rowset.WrapperDatabase;
			lock (wrapperDatabase.Handle.SyncObject)
			{
				wrapperDatabase.LockStorage();
				bool noReadAheadCache = wrapperDatabase.Handle.NoReadAheadCache;
				wrapperDatabase.Handle.NoReadAheadCache = true;
				bool writeBehindCache = wrapperDatabase.Handle.NoWriteBehindCache;
				wrapperDatabase.Handle.NoWriteBehindCache = true;
				Rowset.MinimizeMemoryCache(true);
				try
				{
					Rowset.LockStorage();
					try
					{
						uint rowCount = Rowset.RowCount;
						bool flag1 = false;
						int totalKeyLen = 0;
						int num1 = 0;
						try
						{
							foreach (DataStorage dataStorage in Values)
							{
								if (dataStorage != null && dataStorage != Rowset)
								{
									RowsetIndex rowsetIndex = (RowsetIndex)dataStorage;
									int expectedKeyLen = 0;
									++num1;
									try
									{
										if (rowsetIndex.StartBuild(rowCount, ref expectedKeyLen))
											totalKeyLen += expectedKeyLen;
									}
									catch (Exception ex)
									{
										RemoveOrder(rowsetIndex);
										throw ex;
									}
									if (forceRegistering)
										rowsetIndex.RegisterInDatabase();
								}
							}
							flag1 = Count > 1;
						}
						finally
						{
							wrapperDatabase.FinalizeChanges(!flag1, forceRegistering);
						}
						if (!flag1)
							return;
						bool flag2 = true;
						Rowset.DeactivateFilters();
						rowset.FreezeRelationships();
						rowset.MaskRelationships(rowset, Rowset, true);
						rowset.Top();
						rowset.Synch();
						try
						{
							uint num2 = rowCount == 0U ? 1U : rowCount;
							uint num3 = 0;
							uint num4 = num2 / 5U;
							uint num5 = num2 + num4;
							uint num6 = num4 / (uint)Count;
							int num7 = 0;
							string message = string.Format("({0} index{1})", num1, num1 == 1 ? string.Empty : "es");
							if (Count > 0)
								Rowset.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.IndexOperation, Rowset.Name, message);
							ulong num8 = ExpectedIndexingLoops(totalKeyLen);
							while (!rowset.EndOfSet)
							{
								foreach (DataStorage dataStorage in Values)
								{
									if (dataStorage != null && dataStorage != Rowset)
										((RowsetIndex)dataStorage).EvaluateSpoolKey(num8 <= 0UL);
								}
								rowset.NextRow();
								if (num8 <= 0UL)
								{
									++num7;
									num8 = ExpectedIndexingLoops(totalKeyLen);
									Rowset.CallOperationStatusDelegate(num3++ * 100U / num5, VistaDBOperationStatusTypes.IndexOperation, Rowset.Name, string.Format("row # {0:N0} ...", num3));
								}
								else
									Rowset.CallOperationStatusDelegate(num3++ * 100U / num5, VistaDBOperationStatusTypes.IndexOperation);
								--num8;
							}
							rowset.MinimizeMemoryCache(true);
							if (num7 > 0)
							{
								int num9;
								Rowset.CallOperationStatusDelegate(num3 * 100U / num5, VistaDBOperationStatusTypes.IndexOperation, Rowset.Name, string.Format("merging {0} bands", num9 = num7 + 1));
							}
							foreach (DataStorage dataStorage in Values)
							{
								if (dataStorage != null && dataStorage != Rowset)
								{
									RowsetIndex rowsetIndex = (RowsetIndex)dataStorage;
									try
									{
										rowsetIndex.FinishBuild();
									}
									catch (Exception ex)
									{
										RemoveOrder(rowsetIndex);
										throw ex;
									}
									num3 += num6;
									Rowset.CallOperationStatusDelegate(num3 * 100U / num5, VistaDBOperationStatusTypes.IndexOperation);
									rowsetIndex.MinimizeMemoryCache(false);
								}
							}
							flag2 = false;
						}
						finally
						{
							rowset.DefreezeRelationships();
							Rowset.ActivateFilters();
							Rowset.CallOperationStatusDelegate(uint.MaxValue, VistaDBOperationStatusTypes.IndexOperation);
							foreach (DataStorage dataStorage in Values)
							{
								if (dataStorage != null && dataStorage != Rowset)
									((RowsetIndex)dataStorage).RegisterInDatabase(flag2 || forceRegistering);
							}
						}
						commit = Count > 1;
					}
					finally
					{
						Rowset.UnlockStorage(false);
						Rowset.FinalizeChanges(!commit, commit);
					}
				}
				finally
				{
					wrapperDatabase.UnlockStorage(false);
					wrapperDatabase.FinalizeChanges(!commit, commit);
					wrapperDatabase.Handle.NoReadAheadCache = noReadAheadCache;
					wrapperDatabase.Handle.NoWriteBehindCache = writeBehindCache;
					foreach (DataStorage dataStorage in Values)
						dataStorage?.MinimizeMemoryCache(true);
				}
			}
		}

		private void SyncPositionStatus()
		{
			if (rowStatus != Table.Status.SynchPosition)
				return;
			bool flag = !Rowset.PostponedSynchronization;
			if (flag)
				Rowset.LockStorage();
			try
			{
				ActiveOrder.Synch();
			}
			finally
			{
				if (flag)
					Rowset.UnlockStorage(true);
			}
			RowStatus = Table.Status.NoAction;
		}

		private string ErrorTip(uint rowId)
		{
			return "\nTable: '" + Rowset.Name + "', RowId = " + rowId.ToString() + "\n";
		}

		private void Create(bool commit, bool empty)
		{
			try
			{
				if (!Rowset.CreateRow(commit, empty) && !Rowset.SuppressErrors)
					throw new VistaDBException(250, ErrorTip(Rowset.SatelliteRow.RowId));
			}
			catch (Exception ex)
			{
				if (!Rowset.SuppressErrors)
					throw new VistaDBException(ex, 250, ErrorTip(Rowset.SatelliteRow.RowId));
			}
		}

		private void Update(bool commit)
		{
			uint rowId = Rowset.CurrentRow.RowId;
			try
			{
				if (!Rowset.UpdateRow(commit) && !Rowset.SuppressErrors)
					throw new VistaDBException(251, ErrorTip(rowId));
			}
			catch (Exception ex)
			{
				if (!Rowset.SuppressErrors)
					throw new VistaDBException(ex, 251, ErrorTip(rowId));
			}
		}

		private void Delete(bool commit)
		{
			SyncPositionStatus();
			ClusteredRowset rowset = Rowset;
			uint rowId = rowset.CurrentRow.RowId;
			try
			{
				if (!rowset.DeleteRow(commit))
					throw new VistaDBException(252, ErrorTip(rowId));
			}
			catch (Exception ex)
			{
				if (!rowset.SuppressErrors)
					throw new VistaDBException(ex, 252, ErrorTip(rowId));
			}
			DataStorage activeOrder = ActiveOrder;
			if (activeOrder == rowset)
				return;
			if (!activeOrder.ActivateLinks() && !activeOrder.EndOfSet)
				activeOrder.NextRow();
			ActivateDependencies(rowset, activeOrder);
		}

		public void PrepareTriggers(TriggerAction eventType)
		{
			Rowset.PrepareTriggers(eventType);
		}

		public void ExecuteTriggers(TriggerAction eventType, bool justReset)
		{
			Rowset.ExecuteCLRTriggers(eventType, justReset);
		}

		private void SyncUpToDateStatus(bool commit, bool leaveRowLock)
		{
			lock (Rowset.WrapperDatabase)
			{
				if (rowStatus == Table.Status.NoAction)
					return;
				if (rowStatus == Table.Status.SynchPosition)
				{
					SyncPositionStatus();
				}
				else
				{
					if (!Rowset.AllowSyncEdit && Rowset.IsSystemTable)
						throw new VistaDBException(208, Rowset.Name);
					bool postponedUserUnlock = Rowset.PostponedUserUnlock;
					Rowset.PostponedUserUnlock = leaveRowLock;
					try
					{
						switch (rowStatus)
						{
							case Table.Status.Update:
								Update(commit);
								break;
							case Table.Status.Delete:
								Delete(commit);
								break;
							case Table.Status.Insert:
								Create(commit, false);
								break;
							case Table.Status.InsertBlank:
								Create(commit, true);
								break;
						}
					}
					finally
					{
						Rowset.PostponedUserUnlock = postponedUserUnlock;
						RowStatus = Table.Status.NoAction;
						if (commit)
							Rowset.WrapperDatabase.CloseTriggeredTables();
					}
					if (!commit)
						return;
					currentModificationCounter = ++ClusteredRowset.ModificationCounter;
				}
			}
		}

		private void GoNearestKey(Row key)
		{
			VistaDB.Engine.Core.Indexing.Index activeOrder = ActiveOrder;
			if (Rowset != activeOrder)
				Rowset.LockStorage();
			try
			{
				activeOrder.LockStorage();
				try
				{
					activeOrder.SeekRow(key, true);
				}
				catch (Exception ex)
				{
					throw new VistaDBException(ex, 271);
				}
				finally
				{
					activeOrder.UnlockStorage(true);
				}
			}
			finally
			{
				if (Rowset != activeOrder)
					Rowset.UnlockStorage(true);
			}
		}

		private void Put(IValue columnValue, string columnName)
		{
			Row.Column column = Rowset.LookForColumn(columnName);
			if (column == null)
				return;
			Put(columnValue, column.RowIndex);
		}

		private void Put(object obj, int columnIndex)
		{
			IValue columnValue1 = Rowset.SatelliteRow[columnIndex];
			if (columnValue1 == null)
				return;
			if (obj == null)
			{
				columnValue1.Value = null;
				Put(columnValue1, columnIndex);
			}
			else
			{
				VistaDBType type = Row.Column.VistaDBTypeBySystemType(obj.GetType());
				IValue columnValue2 = type == columnValue1.Type ? columnValue1 : rowset.WrapperDatabase.CreateEmptyColumnInstance(type);
				columnValue2.Value = obj;
				Put(columnValue2, columnIndex);
			}
		}

		internal void DeclareIndex(string name, string keyExpression, string forExpression, bool primary, bool unique, bool v_index, bool sparse, bool fk_constraint, bool fts, bool temporary, int lcid)
		{
			name = Row.Column.FixName(name);
			if (ContainsKey(name))
				throw new VistaDBException(146, Rowset.Name + "." + name);
			if (primary && PKIndex != null)
				throw new VistaDBException(143);
			if (fts && FTSIndexExists)
				throw new VistaDBException(156);
			if (keyExpression == null || keyExpression.Length == 0)
				throw new VistaDBException(280);
			keyExpression = VistaDB.Engine.Core.Indexing.Index.FixKeyExpression(keyExpression);
			if (!temporary)
			{
				string[] strArray = keyExpression.Split(";"[0]);
				if (strArray.Length > parentDatabase.MaxIndexColumns)
					throw new VistaDBException(154, parentDatabase.MaxIndexColumns.ToString());
				List<string> stringList = new List<string>();
				foreach (string str in strArray)
				{
					if (stringList.Contains(parentDatabase.Culture.TextInfo.ToUpper(str)))
						throw new VistaDBException(147, str + "cannot contain occur more than once in index " + name);
					stringList.Add(parentDatabase.Culture.TextInfo.ToUpper(str));
				}
			}
			CultureInfo culture = lcid == 0 ? Rowset.Culture : new CultureInfo(lcid);
			RowsetIndex instance = RowsetIndex.CreateInstance("", name, keyExpression, fts, Rowset, ParentConnection, Rowset.WrapperDatabase, culture, Rowset.Encryption);
			IVistaDBIndexInformation indexInformation = new Table.TableSchema.IndexCollection.IndexInformation("", name, keyExpression, unique || primary, primary, false, sparse, fk_constraint, fts, temporary, 0UL, null);
			instance.DeclareNewStorage(indexInformation);
			AddOrder(instance.Alias, instance);
		}

		internal void CreateIndex(string name, string keyExpression, string forExpression, bool primary, bool unique, bool v_index, bool sparse, bool fk_constraint, bool fts, bool temporary)
		{
			DeclareIndex(name, keyExpression, forExpression, primary, unique, v_index, sparse, fk_constraint, fts, temporary, 0);
			BuildIndexes(false);
		}

		internal void CreateIndex(string name, string keyExpression, string forExpression, bool unique)
		{
			DeclareIndex(name, keyExpression, forExpression, false, unique, true, false, false, false, false, 0);
			BuildIndexes(false);
		}

		internal void DropIndex(string name, bool commit)
		{
			DataStorage order = GetOrder(name);
			if (Rowset == order)
				throw new VistaDBException(sbyte.MaxValue, name);
			Rowset.DropIndex((RowsetIndex)order, commit);
			RemoveOrder((VistaDB.Engine.Core.Indexing.Index)order);
		}

		private void DropFTSIndex()
		{
			foreach (VistaDB.Engine.Core.Indexing.Index order in Values)
			{
				if (order.IsFts)
				{
					Rowset.DropIndex((RowsetIndex)order, true);
					RemoveOrder(order);
					break;
				}
			}
		}

		private void RenameIndex(string oldName, string newName)
		{
			oldName = Row.Column.FixName(oldName);
			newName = Row.Column.FixName(newName);
			if (Database.DatabaseObject.EqualNames(oldName, newName))
				return;
			VistaDB.Engine.Core.Indexing.Index order = GetOrder(oldName);
			if (Rowset == order)
				return;
			Rowset.RenameIndex((RowsetIndex)order, oldName, newName, true);
			Remove(oldName);
			Add(newName, order);
		}

		internal void DropForeignKey(string constraintName, bool commit)
		{
			RowsetIndex order = (RowsetIndex)GetOrder(constraintName);
			Rowset.DropForeignKey(this, order, constraintName, commit);
			RemoveOrder(order);
		}

		internal VistaDB.Engine.Core.Indexing.Index GetIndexOrder(string name, bool raiseError)
		{
			return GetOrder(name, raiseError);
		}

		private string SetModifyProcessing(string constraint)
		{
			if (constraint == null)
				return null;
			Rowset.ActivateConstraint(Table.exportConstraint, constraint, 1, Rowset, false);
			return Table.exportConstraint;
		}

		private void ResetModifyProcessing(string constraintName)
		{
			if (constraintName == null)
				return;
			Rowset.DeactivateConstraint(constraintName);
		}

		internal void ExportToTable(Table destinationTable, string filter, bool suppressAutoValues, bool usePkIndex)
		{
			string constraintName = null;
			Rowset.CreateTranslationsList(destinationTable.Rowset);
			Rowset.FreezeRelationships();
			ClusteredRowset rowset = destinationTable.Rowset;
			bool suppressAutoValues1 = rowset.SuppressAutoValues;
			uint num = 0;
			List<ulong> ulongList = new List<ulong>();
			List<string> stringList = new List<string>();
			try
			{
				rowset.SuppressAutoValues = suppressAutoValues;
				rowset.DeactivateFilters();
				constraintName = destinationTable.SetModifyProcessing(filter);
				if (usePkIndex)
				{
					using (IVistaDBTableSchema tableSchema = parentDatabase.GetTableSchema(Name, false))
					{
						foreach (IVistaDBIndexInformation index in tableSchema.Indexes)
						{
							if (index.Primary)
							{
								parentDatabase.ActivateIndex(this, index);
								((IVistaDBTable)this).ActiveIndex = index.Name;
								break;
							}
						}
					}
					rowset.Top();
					rowset.Synch();
					ActiveOrder.Top();
					ActiveOrder.Synch();
					First();
					uint rowCount = ActiveOrder.RowCount;
					string message = string.Format("({0:N0} row{1})", rowCount, rowCount == 1U ? string.Empty : "s");
					rowset.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.DataExportOperation, Name, message);
					while (rowCount > 0U && !EndOfTable)
					{
						ulongList.Add(CurrentRow.RowId);
						stringList.Add(ActiveOrder.CurrentRow[0].ToString());
						Rowset.ExportRow(CurrentRow, rowset.SatelliteRow);
						rowset.CreateRow(true, false);
						Next();
						rowset.CallOperationStatusDelegate(++num * 100U / rowCount, VistaDBOperationStatusTypes.DataExportOperation);
					}
				}
				else
					Rowset.Export(rowset, !rowset.SuppressErrors);
			}
			finally
			{
				Rowset.DefreezeRelationships();
				Rowset.FreeTranslationsList();
				destinationTable.ResetModifyProcessing(constraintName);
				rowset.ActivateFilters();
				rowset.SuppressAutoValues = suppressAutoValues1;
			}
		}

		internal void CascadeUpdateForeignTable(VistaDB.Engine.Core.Indexing.Index primaryIndex, string order, VistaDBReferentialIntegrity integrity)
		{
			RowsetIndex order1 = (RowsetIndex)GetOrder(order);
			List<Row.Column> columnList = order1.KeyPCode.EnumColumns();
			Rowset.LockStorage();
			try
			{
				Row row = primaryIndex.CurrentPrimaryKey.CopyInstance();
				row.RowId = 0U;
				row.RowVersion = 0U;
				PushContext();
				try
				{
					switch (integrity)
					{
						case VistaDBReferentialIntegrity.Cascade:
							while (LookForKey(row, order1, false, false, false))
							{
								RowStatus = Table.Status.Update;
								for (int index = 0; index < columnList.Count; ++index)
									Rowset.SatelliteRow[columnList[index].RowIndex].Value = primaryIndex.CurrentRow[index].Value;
								SyncUpToDateStatus(false, true);
							}
							break;
						case VistaDBReferentialIntegrity.SetNull:
							while (LookForKey(row, order1, false, false, false))
							{
								RowStatus = Table.Status.Update;
								for (int index = 0; index < columnList.Count; ++index)
									Rowset.SatelliteRow[columnList[index].RowIndex].Value = null;
								SyncUpToDateStatus(false, true);
							}
							break;
						default:
							bool flag = false;
							for (int index = 0; index < columnList.Count && !flag; ++index)
								flag = Rowset.WrapperDatabase.IsDefaultValueRegistered(Rowset, columnList[index].Name);
							while (LookForKey(row, order1, false, false, false))
							{
								RowStatus = Table.Status.Update;
								for (int index = 0; index < columnList.Count; ++index)
								{
									int rowIndex = columnList[index].RowIndex;
									Row.Column column = Rowset.SatelliteRow[rowIndex];
									column.Value = Rowset.DefaultRow[rowIndex].Value;
									column.Edited = !flag;
								}
								SyncUpToDateStatus(false, true);
								if (order1.CurrentRow.EqualColumns(row, Rowset.IsClustered))
									break;
							}
							break;
					}
				}
				finally
				{
					PopContext();
				}
			}
			finally
			{
				Rowset.UnlockStorage(false);
			}
		}

		internal void CascadeDeleteForeignTable(VistaDB.Engine.Core.Indexing.Index primaryIndex, string order)
		{
			RowsetIndex order1 = (RowsetIndex)GetOrder(order);
			Rowset.LockStorage();
			try
			{
				Row key = primaryIndex.CurrentPrimaryKey.CopyInstance();
				key.RowId = 0U;
				key.RowVersion = 0U;
				PushContext();
				try
				{
					while (LookForKey(key, order1, false, false, false))
						Delete(false);
				}
				finally
				{
					PopContext();
				}
			}
			finally
			{
				Rowset.UnlockStorage(false);
			}
		}

		internal void PushContext()
		{
			context.Push();
		}

		internal void PopContext()
		{
			context.Pop();
		}

		internal bool IsClone
		{
			get
			{
				return clones > 0;
			}
		}

		internal void AddCloneReference()
		{
			++clones;
			PushContext();
		}

		internal bool RemoveCloneReference()
		{
			if (clones <= 0)
				return true;
			--clones;
			PopContext();
			return false;
		}

		protected virtual void Put(IValue columnValue, int columnIndex)
		{
			Row.Column column = Rowset.CurrentRow[columnIndex];
			if (column == null)
				return;
			SyncPositionStatus();
			try
			{
				RowStatus = Table.Status.Update;
				Rowset.PutColumnValue(column.RowIndex, column.Type, Rowset.SatelliteRow, columnValue);
			}
			catch (Exception ex)
			{
				RowStatus = Table.Status.NoAction;
				throw ex;
			}
		}

		protected virtual IVistaDBValue Get(int columnIndex)
		{
			if (IsClosed)
				throw new VistaDBException(180);
			Row.Column column1 = Rowset.CurrentRow[columnIndex];
			if (column1 == null)
				throw new VistaDBException(182, columnIndex.ToString());
			SyncPositionStatus();
			DataStorage dataStorage = Rowset;
			DataStorage activeOrder = ActiveOrder;
			if (dataStorage != activeOrder)
			{
				Row.Column column2 = ActiveOrder.LookForColumn(column1.Name);
				if (column2 != null)
				{
					dataStorage = activeOrder;
					column1 = column2;
				}
			}
			return dataStorage.GetColumnValue(column1.RowIndex, column1.Type, dataStorage.CurrentRow);
		}

		internal virtual void DoExportXml(string xmlFileName, VistaDBXmlWriteMode mode)
		{
		}

		internal virtual void DoImportXml(string xmlFileName, VistaDBXmlReadMode mode, bool interruptOnError)
		{
		}

		protected virtual bool IsAllowPooling()
		{
			if (parentDatabase != null)
				return !parentDatabase.Handle.Mode.Temporary;
			return false;
		}

		string IVistaDBTable.Name
		{
			get
			{
				return Name;
			}
		}

		void IVistaDBTable.Close()
		{
			Dispose();
		}

		public bool IsClosed
		{
			get
			{
				return rowset == null;
			}
		}

		IVistaDBRow IVistaDBTable.CurrentKey
		{
			get
			{
				return CurrentKey.CopyInstance();
			}
			set
			{
				CurrentKey = (Row)value;
			}
		}

		IVistaDBRow IVistaDBTable.CurrentRow
		{
			get
			{
				return CurrentRow.CopyInstance();
			}
			set
			{
				CurrentRow = (Row)value;
			}
		}

		string IVistaDBTable.ActiveIndex
		{
			get
			{
				VistaDB.Engine.Core.Indexing.Index activeOrder = ActiveOrder;
				if (activeOrder != null && activeOrder.IsFts)
					activeOrder.DoSetFtsInactive();
				if (activeOrder != Rowset)
					return activeOrder.Alias;
				return null;
			}
			set
			{
				VistaDB.Engine.Core.Indexing.Index order = GetOrder(value);
				if (activeOrder == order)
					return;
				if (activeOrder != null)
					activeOrder.DoSetFtsInactive();
				activeOrder = order;
			}
		}

		public bool StartOfTable
		{
			get
			{
				SyncPositionStatus();
				return ActiveOrder.BgnOfSet;
			}
		}

		public bool EndOfTable
		{
			get
			{
				SyncPositionStatus();
				return ActiveOrder.EndOfSet;
			}
		}

		public long RowCount
		{
			get
			{
				SyncPositionStatus();
				return ActiveOrder.RowCount;
			}
		}

		public void First()
		{
			ActiveOrder.Top();
			RowStatus = Table.Status.SynchPosition;
		}

		public void Last()
		{
			ActiveOrder.Bottom();
			RowStatus = Table.Status.SynchPosition;
		}

		public void MoveBy(int rowNumber)
		{
			ActiveOrder.AddAsynch(rowNumber);
			RowStatus = Table.Status.SynchPosition;
		}

		public void Next()
		{
			MoveBy(1);
		}

		public void Prev()
		{
			MoveBy(-1);
		}

		private void SetTableScope(Row lowValue, Row highValue, DataStorage.ScopeType scopes, bool exactMatching)
		{
			ClusteredRowset rowset = Rowset;
			VistaDB.Engine.Core.Indexing.Index activeOrder = ActiveOrder;
			rowset.LockStorage();
			try
			{
				activeOrder.LockStorage();
				try
				{
					activeOrder.SetScope(lowValue, highValue, scopes, exactMatching);
				}
				finally
				{
					activeOrder.UnlockStorage(true);
				}
			}
			finally
			{
				rowset.UnlockStorage(true);
			}
		}

		void IVistaDBTable.SetScope(string lowKeyExpression, string highKeyExpression)
		{
			if ((lowKeyExpression == null || lowKeyExpression.Length == 0) && (highKeyExpression == null || highKeyExpression.Length == 0))
			{
				((IVistaDBTable)this).ResetScope();
			}
			else
			{
				DataStorage activeOrder = ActiveOrder;
				if (activeOrder == Rowset)
					throw new VistaDBException(272, Rowset.Name);
				Row lowValue = activeOrder.CompileRow(lowKeyExpression, true);
				lowValue.RowId = 0U;
				Row highValue = highKeyExpression == null || highKeyExpression.Length == 0 ? lowValue.CopyInstance() : activeOrder.CompileRow(highKeyExpression, false);
				highValue.RowId = Row.MaxRowId;
				SetTableScope(lowValue, highValue, DataStorage.ScopeType.UserScope, false);
			}
		}

		void IVistaDBTable.ResetScope()
		{
			ActiveOrder.ClearScope(DataStorage.ScopeType.UserScope);
		}

		[DebuggerBrowsable(DebuggerBrowsableState.Never)]
		long IVistaDBTable.ScopeKeyCount
		{
			get
			{
				if (ActiveOrder == Rowset)
					return Rowset.RowCount;
				return ActiveOrder.GetScopeKeyCount();
			}
		}

		void IVistaDBTable.CreateIndex(string name, string keyExpression, bool primary, bool unique)
		{
			CreateIndex(name, keyExpression, null, primary, unique, false, false, false, false, false);
		}

		void IVistaDBTable.CreateTemporaryIndex(string name, string keyExpression, bool unique)
		{
			CreateIndex(name, keyExpression, null, false, unique, false, false, false, false, true);
		}

		void IVistaDBTable.CreateFTSIndex(string name, string columns)
		{
			CreateIndex(name, columns, null, false, false, false, false, false, true, false);
		}

		void IVistaDBTable.DropIndex(string indexName)
		{
			DropIndex(indexName, true);
		}

		void IVistaDBTable.DropFTSIndex()
		{
			DropFTSIndex();
		}

		void IVistaDBTable.RenameIndex(string oldName, string newName)
		{
			RenameIndex(oldName, newName);
		}

		void IVistaDBTable.Insert()
		{
			Rowset.SatelliteRow.InitTop();
			RowStatus = Table.Status.InsertBlank;
		}

		void IVistaDBTable.Post()
		{
			((IVistaDBTable)this).Post(false);
		}

		void IVistaDBTable.Post(bool leaveRowLock)
		{
			SyncUpToDateStatus(true, leaveRowLock);
		}

		void IVistaDBTable.Delete()
		{
			SyncPositionStatus();
			RowStatus = Table.Status.Delete;
			SyncUpToDateStatus(true, false);
		}

		void IVistaDBTable.Lock(long rowId)
		{
			if (rowId <= 0L)
			{
				Rowset.LockStorage();
				rowId = 0L;
			}
			Rowset.LockRow((uint)rowId, true);
		}

		void IVistaDBTable.Unlock(long rowId)
		{
			if (rowId <= 0L)
			{
				Rowset.UnlockStorage(true);
				rowId = 0L;
			}
			Rowset.UnlockRow((uint)rowId, true, true);
		}

		void IVistaDBTable.Put(int columnIndex, object columnValue)
		{
			Put(columnValue, columnIndex);
		}

		void IVistaDBTable.Put(string columnName, IVistaDBValue columnValue)
		{
			Put((IValue)columnValue, columnName);
		}

		void IVistaDBTable.Put(int columnIndex, IVistaDBValue columnValue)
		{
			Put((IValue)columnValue, columnIndex);
		}

		void IVistaDBTable.PutFromFile(string columnName, string fileName)
		{
			Row.Column column = Rowset.LookForColumn(columnName);
			if (column == null)
				return;
			((IVistaDBTable)this).PutFromFile(column.RowIndex, fileName);
		}

		void IVistaDBTable.PutFromFile(int columnIndex, string fileName)
		{
			Row.Column column1 = Rowset.CurrentRow[columnIndex];
			Row.Column column2;
			using (FileStream fileStream = new FileStream(fileName, FileMode.Open))
			{
				int length = (int)fileStream.Length;
				byte[] numArray = new byte[length];
				fileStream.Read(numArray, 0, length);
				if (column1.InternalType == VistaDBType.NChar)
				{
					column2 = Rowset.CreateSQLUnicodeColumnInstance();
					Encoding encoding = Encoding.GetEncoding(column1.CodePage);
					column2.Value = encoding.GetString(numArray, 0, length);
				}
				else
				{
					if (column1.InternalType != VistaDBType.VarBinary)
						throw new VistaDBException(179, column1.Type.ToString());
					column2 = Rowset.CreateEmptyColumnInstance(VistaDBType.Image);
					column2.Value = numArray;
				}
			}
			Put(column2, columnIndex);
		}

		void IVistaDBTable.PutString(string columnName, string value)
		{
			Row.Column unicodeColumnInstance = Rowset.CreateSQLUnicodeColumnInstance();
			unicodeColumnInstance.Value = value;
			Put(unicodeColumnInstance, columnName);
		}

		void IVistaDBTable.PutString(int index, string value)
		{
			Row.Column unicodeColumnInstance = Rowset.CreateSQLUnicodeColumnInstance();
			unicodeColumnInstance.Value = value;
			Put(unicodeColumnInstance, index);
		}

		void IVistaDBTable.PutByte(string columnName, byte value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.TinyInt);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutByte(int index, byte value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.TinyInt);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutInt16(string columnName, short value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.SmallInt);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutInt16(int index, short value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.SmallInt);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutInt32(string columnName, int value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Int);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutInt32(int index, int value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Int);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutInt64(string columnName, long value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.BigInt);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutInt64(int index, long value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.BigInt);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutSingle(string columnName, float value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Real);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutSingle(int index, float value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Real);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutDouble(string columnName, double value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Float);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutDouble(int index, double value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Float);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutDecimal(string columnName, Decimal value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Decimal);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutDecimal(int index, Decimal value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Decimal);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutBoolean(string columnName, bool value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Bit);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutBoolean(int index, bool value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Bit);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutDateTime(string columnName, DateTime value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.DateTime);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutDateTime(int index, DateTime value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.DateTime);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutBinary(string columnName, byte[] value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Image);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutBinary(int index, byte[] value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Image);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutGuid(string columnName, Guid value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.UniqueIdentifier);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutGuid(int index, Guid value)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.UniqueIdentifier);
			emptyColumnInstance.Value = value;
			Put(emptyColumnInstance, index);
		}

		void IVistaDBTable.PutNull(string columnName)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Int);
			emptyColumnInstance.Value = null;
			Put(emptyColumnInstance, columnName);
		}

		void IVistaDBTable.PutNull(int index)
		{
			Row.Column emptyColumnInstance = Rowset.CreateEmptyColumnInstance(VistaDBType.Int);
			emptyColumnInstance.Value = null;
			Put(emptyColumnInstance, index);
		}

		IVistaDBValue IVistaDBTable.Get(string columnName)
		{
			if (IsClosed)
				throw new VistaDBException(180);
			Row.Column column = Rowset.LookForColumn(columnName);
			if (column == null)
				throw new VistaDBException(181, columnName);
			return Get(column.RowIndex);
		}

		IVistaDBValue IVistaDBTable.Get(int columnIndex)
		{
			return Get(columnIndex);
		}

		void IVistaDBTable.GetToFile(string columnName, string fileName)
		{
			Row.Column column = Rowset.LookForColumn(columnName);
			if (column == null)
				return;
			((IVistaDBTable)this).GetToFile(column.RowIndex, fileName);
		}

		void IVistaDBTable.GetToFile(int columnIndex, string fileName)
		{
			IVistaDBValue vistaDbValue = Get(columnIndex);
			if (vistaDbValue.IsNull)
				return;
			Row.Column column = Rowset.CurrentRow[columnIndex];
			using (FileStream fileStream = new FileStream(fileName, FileMode.OpenOrCreate))
			{
				fileStream.SetLength(0L);
				byte[] bytes;
				if (column.InternalType == VistaDBType.NChar)
				{
					bytes = Encoding.GetEncoding(column.CodePage).GetBytes((string)vistaDbValue.Value);
				}
				else
				{
					if (column.InternalType != VistaDBType.VarBinary)
						throw new VistaDBException(179, column.Type.ToString());
					bytes = (byte[])vistaDbValue.Value;
				}
				fileStream.Write(bytes, 0, bytes.Length);
			}
		}

		IVistaDBRow IVistaDBTable.LastSessionIdentity
		{
			get
			{
				return Rowset.LastSessionIdentity;
			}
		}

		IVistaDBRow IVistaDBTable.LastTableIdentity
		{
			get
			{
				return Rowset.LastTableIdentity;
			}
		}

		IVistaDBRow IVistaDBTable.Evaluate(string expression)
		{
			SyncPositionStatus();
			return Rowset.CompileRow(expression, true);
		}

		void IVistaDBTable.SetFilter(string expression, bool optimize)
		{
			if (expression == null || expression.Length == 0)
			{
				Rowset.DetachFiltersByType(Filter.FilterType.Ordinary);
				Rowset.DetachFiltersByType(Filter.FilterType.Optimized);
			}
			else
				Rowset.AttachFilter(new OrdinaryFilter(Rowset.Parser.Compile(expression, Rowset, true)));
		}

		void IVistaDBTable.ResetFilter()
		{
			((IVistaDBTable)this).SetFilter(null, true);
		}

		string IVistaDBTable.GetFilter(bool optimizable)
		{
			return Rowset.GetFilter(Filter.FilterType.Ordinary, 0).Expression;
		}

		bool IVistaDBTable.Find(string keyEvaluationExpression, string indexName, bool partialMatching, bool softPosition)
		{
			VistaDB.Engine.Core.Indexing.Index index = indexName == null ? ActiveOrder : GetOrder(indexName);
			if (index == Rowset)
				throw new VistaDBException(272, Rowset.Name);
			return LookForKey(index.CompileRow(keyEvaluationExpression, true), index, partialMatching, softPosition, true);
		}

		void IVistaDBTable.CreateIdentity(string columnName, string seedExpression, string stepExpression)
		{
			Rowset.CreateIdentity(columnName, seedExpression, stepExpression);
		}

		void IVistaDBTable.DropIdentity(string columnName)
		{
			Rowset.DropIdentity(columnName);
		}

		void IVistaDBTable.CreateDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description)
		{
			Rowset.CreateDefaultValue(columnName, scriptExpression, useInUpdate, description);
		}

		void IVistaDBTable.DropDefaultValue(string columnName)
		{
			Rowset.DropDefaultValue(columnName);
		}

		void IVistaDBTable.CreateConstraint(string name, string scriptExpression, string description, bool insertion, bool update, bool delete)
		{
			Rowset.CreateConstraint(name, scriptExpression, description, insertion, update, delete);
		}

		void IVistaDBTable.DropConstraint(string name)
		{
			Rowset.DropConstraint(name);
		}

		void IVistaDBTable.CreateForeignKey(string constraintName, string foreignKey, string primaryTable, VistaDBReferentialIntegrity updateIntegrity, VistaDBReferentialIntegrity deleteIntegrity, string description)
		{
			Rowset.CreateForeignKey(this, constraintName, foreignKey, primaryTable, updateIntegrity, deleteIntegrity, description);
		}

		void IVistaDBTable.DropForeignKey(string constraintName)
		{
			DropForeignKey(constraintName, true);
		}

		void IVistaDBTable.ExportData(IVistaDBTable table, string constraint)
		{
			ExportToTable((Table)table, constraint, true, false);
		}

		void IVistaDBTable.SetOperationCallbackDelegate(OperationCallbackDelegate operationCallbackDelegate)
		{
			Rowset.operationDelegate = operationCallbackDelegate;
		}

		void IVistaDBTable.SetDDAEventDelegate(IVistaDBDDAEventDelegate eventDelegate)
		{
			Rowset.SetDelegate(eventDelegate);
		}

		void IVistaDBTable.ResetEventDelegate(DDAEventDelegateType eventType)
		{
			Rowset.ResetDelegate(eventType);
		}

		bool IVistaDBTable.EnforceConstraints
		{
			get
			{
				return Rowset.EnforcedConstraints;
			}
			set
			{
				Rowset.EnforcedConstraints = value;
			}
		}

		bool IVistaDBTable.EnforceIdentities
		{
			get
			{
				return Rowset.EnforcedIdentities;
			}
			set
			{
				Rowset.EnforcedIdentities = value;
			}
		}

		void IVistaDBTable.GetScope(out IVistaDBRow lowRow, out IVistaDBRow highRow)
		{
			ActiveOrder.GetScope(out lowRow, out highRow);
		}

		void IVistaDBTable.SetScope(IVistaDBRow lowRow, IVistaDBRow highRow)
		{
			if (lowRow == null || highRow == null)
				((IVistaDBTable)this).ResetScope();
			else
				SetTableScope((Row)lowRow, (Row)highRow, DataStorage.ScopeType.UserScope, false);
		}

		bool IVistaDBTable.Find(IVistaDBRow key, string indexName, bool partialMatching, bool softPosition)
		{
			VistaDB.Engine.Core.Indexing.Index index = string.IsNullOrEmpty(indexName) ? ActiveOrder : GetOrder(indexName);
			if (index == Rowset)
				throw new VistaDBException(272, Rowset.Name);
			return LookForKey((Row)key, index, partialMatching, softPosition, true);
		}

		IVistaDBIndexCollection IVistaDBTable.TemporaryIndexes
		{
			get
			{
				Table.TableSchema.IndexCollection indexCollection = new Table.TableSchema.IndexCollection();
				foreach (VistaDB.Engine.Core.Indexing.Index index in Values)
				{
					if (index != Rowset && index.IsTemporary)
						indexCollection.Add(index.Alias, ((RowsetIndex)index).CollectIndexInformation());
				}
				return indexCollection;
			}
		}

		IVistaDBIndexCollection IVistaDBTable.RegularIndexes
		{
			get
			{
				Table.TableSchema.IndexCollection indexCollection = new Table.TableSchema.IndexCollection();
				foreach (VistaDB.Engine.Core.Indexing.Index index in Values)
				{
					if (index != Rowset && !index.IsTemporary)
						indexCollection.Add(index.Alias, ((RowsetIndex)index).CollectIndexInformation());
				}
				return indexCollection;
			}
		}

		string ITable.Alias
		{
			get
			{
				return rowset.Alias;
			}
		}

		public ulong Id
		{
			get
			{
				return tableId;
			}
		}

		IRow ITable.CurrentKey
		{
			get
			{
				return CurrentKey;
			}
			set
			{
				CurrentKey = (Row)value;
			}
		}

		IRow ITable.CurrentRow
		{
			get
			{
				return CurrentRow;
			}
			set
			{
				CurrentRow = (Row)value;
			}
		}

		bool ITable.SuppressErrors
		{
			get
			{
				return Rowset.SuppressErrors;
			}
			set
			{
				Rowset.SuppressErrors = value;
			}
		}

		string ITable.PKIndex
		{
			get
			{
				if (pkOrder != null)
					return pkOrder.Alias;
				return null;
			}
		}

		public bool IsReadOnly
		{
			get
			{
				return Rowset.IsReadOnly;
			}
		}

		bool ITable.IsExclusive
		{
			get
			{
				if (!Rowset.IsShared)
					return !Rowset.IsShareReadOnly;
				return false;
			}
		}

		bool ITable.AllowPooling
		{
			get
			{
				return IsAllowPooling();
			}
		}

		IRow ITable.KeyStructure(string indexName)
		{
			DataStorage dataStorage = indexName == null ? ActiveOrder : GetOrder(indexName);
			if (dataStorage == Rowset)
				throw new VistaDBException(sbyte.MaxValue, indexName);
			return dataStorage.TopRow.CopyInstance();
		}

		internal bool FindReference(IRow key, string indexName)
		{
			VistaDB.Engine.Core.Indexing.Index index = string.IsNullOrEmpty(indexName) ? ActiveOrder : GetOrder(indexName);
			if (index == Rowset)
				throw new VistaDBException(272, Rowset.Name);
			return LookForKey((Row)key, index, false, false, false);
		}

		void ITable.CreateSparseIndex(string name, string keyExpression)
		{
			CreateIndex(name, keyExpression, null, false, true, false, true, false, false, false);
		}

		void ITable.FreezeSelfRelationships()
		{
			Rowset.FreezeSelfRelationships(this);
		}

		void ITable.DefreezeSelfRelationships()
		{
			Rowset.DefreezeSelfRelationships(this);
		}

		IOptimizedFilter ITable.BuildFilterMap(string indexName, IRow lowScopeValue, IRow highScopeValue, bool excludeNulls)
		{
			((IVistaDBTable)this).ActiveIndex = indexName;
			VistaDB.Engine.Core.Indexing.Index activeOrder = ActiveOrder;
			rowset.LockStorage();
			try
			{
				activeOrder.LockStorage();
				try
				{
					return activeOrder.BuildFiltermap((Row)lowScopeValue, (Row)highScopeValue, excludeNulls);
				}
				finally
				{
					activeOrder.UnlockStorage(true);
				}
			}
			finally
			{
				rowset.UnlockStorage(true);
			}
		}

		void ITable.BeginOptimizedFiltering(IOptimizedFilter filter, string pivotIndex)
		{
			Rowset.BeginOptimizedFiltering(filter);
			((IVistaDBTable)this).ActiveIndex = pivotIndex;
			((IVistaDBTable)this).ResetScope();
		}

		internal virtual void DoResetOptimizedFiltering()
		{
		}

		void ITable.ResetOptimizedFiltering()
		{
			Rowset.ResetOptimizedFiltering();
			foreach (DataStorage dataStorage in Values)
				dataStorage.ClearScope(DataStorage.ScopeType.UserScope);
		}

		void ITable.ClearCachedBitmaps()
		{
			foreach (VistaDB.Engine.Core.Indexing.Index index in Values)
				index.ClearCachedBitmaps();
		}

		void ITable.PrepareFtsOptimization()
		{
			ActiveOrder.DoSetFtsActive();
		}

		void ITable.Post()
		{
			Rowset.AllowSyncEdit = true;
			try
			{
				((IVistaDBTable)this).Post();
			}
			finally
			{
				Rowset.AllowSyncEdit = false;
			}
		}

		void ITable.Delete()
		{
			Rowset.AllowSyncEdit = true;
			try
			{
				((IVistaDBTable)this).Delete();
			}
			finally
			{
				Rowset.AllowSyncEdit = false;
			}
		}

		public void Dispose()
		{
			if (clones > 0 || isDisposed)
				return;
			isDisposed = true;
			GC.SuppressFinalize(this);
			string name = Name;
			try
			{
				Dispose(true);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 128, name);
			}
		}

		private void Dispose(bool disposing)
		{
			if (rowset == null)
				return;
			try
			{
				foreach (DataStorage dataStorage in Values)
				{
					if (dataStorage != Rowset && dataStorage != null)
						dataStorage.Dispose();
				}
				if (rowset != null)
					rowset.Dispose();
				rowset = null;
				Clear();
			}
			finally
			{
				Database parentDatabase = this.parentDatabase;
				this.parentDatabase = null;
				parentDatabase?.UnlinkDescriptor(this);
			}
		}

		~Table()
		{
			if (isDisposed)
				return;
			isDisposed = true;
			Dispose(false);
		}

		internal bool NotifyChangedEnvironment(Connection.Settings variable, object newValue)
		{
			foreach (DataStorage dataStorage in Values)
			{
				if (dataStorage != null && !dataStorage.NotifyChangedEnvironment(variable, newValue))
					return false;
			}
			return true;
		}

		internal enum TableType
		{
			Default,
			Tombstone,
			Anchor,
			Unknown,
		}

		internal class TableSchema : List<Row.Column>, IVistaDBTableSchema, IVistaDBDatabaseObject, IEnumerable<IVistaDBColumnAttributes>, IEnumerable, IDisposable
		{
			private Dictionary<string, Row.Column> map = new Dictionary<string, Row.Column>(25, StringComparer.OrdinalIgnoreCase);
			private Table.TableSchema.IndexCollection indexes = new Table.TableSchema.IndexCollection();
			private Table.TableSchema.IdentityCollection identities = new Table.TableSchema.IdentityCollection();
			private Table.TableSchema.DefaultValueCollection defaults = new Table.TableSchema.DefaultValueCollection();
			private Table.TableSchema.ConstraintCollection constraints = new Table.TableSchema.ConstraintCollection();
			private Database.RelationshipCollection relationships = new Database.RelationshipCollection();
			private Database.ClrTriggerCollection triggers = new Database.ClrTriggerCollection();
			private Dictionary<int, string> droppedColumns = new Dictionary<int, string>(25);
			private Dictionary<int, string> renamedColumns = new Dictionary<int, string>(25);
			private int timestampIndex = -1;
			private string description;
			private string name;
			private DataStorage linkedStorage;
			private Table.TableType type;
			private ulong tableId;
			private int newUniqueID;
			private int initIdCount;
			private bool temporary;
			private bool isDisposed;

			internal TableSchema(string name, Table.TableType type, string description, ulong tableId, DataStorage linkedStorage)
			{
				if (name == null)
					throw new VistaDBException(152);
				name = name.TrimEnd();
				if (string.IsNullOrEmpty(name))
					throw new VistaDBException(152);
				this.name = name;
				this.tableId = tableId;
				this.linkedStorage = linkedStorage;
				this.type = type;
				Description = description;
			}

			private bool IsUserTimestamp(Row.Column column)
			{
				if (column.Type == VistaDBType.Timestamp)
					return !column.IsSystem;
				return false;
			}

			private void TestDuplicateNames(string newName)
			{
				if (((IVistaDBTableSchema)this)[newName] != null)
					throw new VistaDBException(147, newName);
			}

			private void TestSystemAttribute(Row.Column column)
			{
				if (column.IsSystem)
					throw new VistaDBException(207, column.Name);
			}

			private void TestDuplicateTimestamp(Row.Column column)
			{
				if (!column.IsSystem && column.Type == VistaDBType.Timestamp && timestampIndex >= 0)
					throw new VistaDBException(157);
			}

			internal IVistaDBColumnAttributes AddNewColumn(Row.Column column)
			{
				TestDuplicateTimestamp(column);
				map.Add(column.Name, column);
				Add(column);
				column.RowIndex = Count - 1;
				column.UniqueID = newUniqueID++;
				if (IsUserTimestamp(column))
					timestampIndex = column.RowIndex;
				return column;
			}

			internal IVistaDBColumnAttributes AddSyncColumn(Row.Column column)
			{
				if (!column.IsSync)
					return null;
				TestDuplicateNames(column.Name);
				map.Add(column.Name, column);
				Add(column);
				column.RowIndex = Count - 1;
				column.UniqueID = newUniqueID++;
				return column;
			}

			internal void AddSyncColumn(string columnName, VistaDBType type)
			{
				Row.Column emptyColumnInstance = linkedStorage.CreateEmptyColumnInstance(type, 0, 0, false, true);
				emptyColumnInstance.AssignAttributes(columnName, false, true, false, false);
				AddSyncColumn(emptyColumnInstance);
			}

			internal void DropSyncColumn(string columnName)
			{
				if (!TryGetValue(columnName, out Row.Column column) || column == null || !column.IsSync)
					return;
				RemoveColumn(column);
			}

			internal void FixInitCounter()
			{
				initIdCount = newUniqueID;
			}

			private void RemoveColumn(Row.Column column)
			{
				map.Remove(column.Name);
				RemoveAt(column.RowIndex);
				if (IsUserTimestamp(column))
					timestampIndex = -1;
				if (column.UniqueID < initIdCount)
				{
					string str = renamedColumns.ContainsKey(column.UniqueID) ? renamedColumns[column.UniqueID] : column.Name;
					renamedColumns.Remove(column.UniqueID);
					droppedColumns.Add(column.UniqueID, str);
				}
				for (int index = 0; index < Count; ++index)
					this[index].RowIndex = index;
				identities.Remove(column.Name);
				defaults.Remove(column.Name);
			}

			private void ReplaceColumn(Row.Column oldColumn, Row.Column newColumn)
			{
				TestSystemAttribute(oldColumn);
				if (timestampIndex != oldColumn.RowIndex)
					TestDuplicateTimestamp(newColumn);
				if (IsUserTimestamp(newColumn))
					timestampIndex = oldColumn.RowIndex;
				newColumn.UniqueID = oldColumn.UniqueID;
				//this[oldColumn.RowIndex] = newColumn;
				RemoveAt(oldColumn.RowIndex);
				Insert(oldColumn.RowIndex, newColumn);
				newColumn.RowIndex = oldColumn.RowIndex;
				map.Remove(oldColumn.Name);
				map.Add(newColumn.Name, newColumn);
				if (!identities.ContainsKey(oldColumn.Name) || newColumn.Type == VistaDBType.SmallInt && newColumn.Type == VistaDBType.Int && newColumn.Type == VistaDBType.BigInt)
					return;
				identities.Remove(oldColumn.Name);
			}

			internal int MinRowDataSize
			{
				get
				{
					return 0;
				}
			}

			internal int MinRowPageSize
			{
				get
				{
					return MinRowDataSize / StorageHandle.DEFAULT_SIZE_OF_PAGE + 1;
				}
			}

			internal ulong HeaderPosition
			{
				get
				{
					return tableId;
				}
				set
				{
					tableId = value;
				}
			}

			internal Table.TableSchema.IndexCollection Indexes
			{
				get
				{
					return indexes;
				}
				set
				{
					indexes = value;
				}
			}

			internal Table.TableSchema.IdentityCollection Identities
			{
				get
				{
					return identities;
				}
				set
				{
					identities = value;
				}
			}

			internal Table.TableSchema.DefaultValueCollection Defaults
			{
				get
				{
					return defaults;
				}
				set
				{
					defaults = value;
				}
			}

			internal Table.TableSchema.ConstraintCollection Constraints
			{
				get
				{
					return constraints;
				}
				set
				{
					constraints = value;
				}
			}

			internal Dictionary<int, string> DroppedColumns
			{
				get
				{
					return droppedColumns;
				}
			}

			internal Dictionary<int, string> RenamedColumns
			{
				get
				{
					return renamedColumns;
				}
			}

			internal bool ContainsSyncPart
			{
				get
				{
					foreach (Row.Column column in this)
					{
						if (column.IsSync)
							return true;
					}
					return false;
				}
			}

			internal bool TemporarySchema
			{
				get
				{
					return temporary;
				}
				set
				{
					temporary = value;
				}
			}

			internal Table.TableType Type
			{
				get
				{
					return type;
				}
			}

			internal Row.Column this[string columnName]
			{
				get
				{
					if (!TryGetValue(columnName, out Row.Column column))
						return null;
					return column;
				}
			}

			internal new Row.Column this[int columnIndex]
			{
				get
				{
					if (columnIndex < 0 || columnIndex > Count)
						return null;
					return base[columnIndex];
				}
			}

			internal Database.RelationshipCollection ForeignKeys
			{
				get
				{
					return relationships;
				}
			}

			internal Database.ClrTriggerCollection Triggers
			{
				get
				{
					return triggers;
				}
			}

			internal bool TryGetValue(string columnName, out Row.Column column)
			{
				return map.TryGetValue(columnName, out column);
			}

			public string Name
			{
				get
				{
					return name;
				}
				set
				{
					name = Row.Column.FixName(value);
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

			int IVistaDBTableSchema.ColumnCount
			{
				get
				{
					return Count;
				}
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.this[string columnName]
			{
				get
				{
					if (TryGetValue(columnName, out Row.Column column))
						return column;
					if (!string.IsNullOrEmpty(columnName) && columnName.Length > 2 && (columnName[0] == '[' && columnName[columnName.Length - 1] == ']') && TryGetValue(columnName.Substring(1, columnName.Length - 2), out column))
						return column;
					return null;
				}
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.this[int columnIndex]
			{
				get
				{
					if (columnIndex < 0 || columnIndex >= Count)
						return null;
					return base[columnIndex];
				}
			}

			ICollection<string> IVistaDBTableSchema.DroppedColumns
			{
				get
				{
					return droppedColumns.Values;
				}
			}

			ICollection<string> IVistaDBTableSchema.RenamedColumns
			{
				get
				{
					return renamedColumns.Values;
				}
			}

			IVistaDBIndexCollection IVistaDBTableSchema.Indexes
			{
				get
				{
					return indexes;
				}
			}

			IVistaDBIdentityCollection IVistaDBTableSchema.Identities
			{
				get
				{
					return identities;
				}
			}

			IVistaDBDefaultValueCollection IVistaDBTableSchema.DefaultValues
			{
				get
				{
					return defaults;
				}
			}

			IVistaDBConstraintCollection IVistaDBTableSchema.Constraints
			{
				get
				{
					return constraints;
				}
			}

			IVistaDBRelationshipCollection IVistaDBTableSchema.ForeignKeys
			{
				get
				{
					return relationships;
				}
			}

			IVistaDBClrTriggerCollection IVistaDBTableSchema.Triggers
			{
				get
				{
					return triggers;
				}
			}

			bool IVistaDBTableSchema.IsSynchronized
			{
				get
				{
					return ContainsSyncPart;
				}
			}

			bool IVistaDBTableSchema.IsSystemTable
			{
				get
				{
					if (type != Table.TableType.Anchor)
						return type == Table.TableType.Tombstone;
					return true;
				}
			}

			public bool IsTombstoneTable
			{
				get
				{
					return type == Table.TableType.Tombstone;
				}
			}

			void IVistaDBTableSchema.DropColumn(string columnName)
			{
				if (!TryGetValue(columnName, out Row.Column column))
					throw new VistaDBException(181, columnName);
				TestSystemAttribute(column);
				RemoveColumn(column);
			}

			private IColumn CreateEmptyColumn(VistaDBType type)
			{
				return linkedStorage.CreateEmptyColumnInstance(type);
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AddColumn(string columnName, VistaDBType type)
			{
				columnName = Row.Column.FixName(columnName);
				TestDuplicateNames(columnName);
				Row.Column emptyColumnInstance = linkedStorage.CreateEmptyColumnInstance(type);
				emptyColumnInstance.AssignAttributes(columnName, true, false, linkedStorage.Encryption != null, false);
				return AddNewColumn(emptyColumnInstance);
			}

			internal IVistaDBColumnAttributes AddColumn(string columnName, VistaDBType type, int maxLen, int codePage, bool syncColumn)
			{
				columnName = Row.Column.FixName(columnName);
				TestDuplicateNames(columnName);
				Row.Column emptyColumnInstance = linkedStorage.CreateEmptyColumnInstance(type, (short)maxLen, codePage, !linkedStorage.CaseSensitive, syncColumn);
				emptyColumnInstance.AssignAttributes(columnName, true, false, linkedStorage.Encryption != null, false);
				return AddNewColumn(emptyColumnInstance);
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AddColumn(string columnName, VistaDBType type, int maxLen, int codePage)
			{
				return AddColumn(columnName, type, maxLen, codePage, false);
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AddColumn(string columnName, VistaDBType type, int maxLen)
			{
				return ((IVistaDBTableSchema)this).AddColumn(columnName, type, maxLen, linkedStorage.Culture.TextInfo.ANSICodePage);
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AlterColumnName(string oldName, string newName)
			{
				oldName = Row.Column.FixName(oldName);
				newName = Row.Column.FixName(newName);
				Row.Column column = (Row.Column)((IVistaDBTableSchema)this)[oldName];
				if (column == null)
					throw new VistaDBException(181, oldName);
				TestSystemAttribute(column);
				if (Database.DatabaseObject.EqualNames(oldName, newName, true))
				{
					if (Database.DatabaseObject.EqualNames(oldName, newName, false))
						return column;
				}
				else
					TestDuplicateNames(newName);
				column.AssignAttributes(newName, column.AllowNull, column.ReadOnly, column.Encrypted, column.Packed);
				map.Remove(oldName);
				map.Add(newName, column);
				if (!renamedColumns.ContainsKey(column.UniqueID) && column.UniqueID < initIdCount)
					renamedColumns.Add(column.UniqueID, oldName);
				if (identities.TryGetValue(oldName, out IVistaDBIdentityInformation identityInformation))
				{
					((Database.DatabaseObject)identityInformation).Name = newName;
					identities.Remove(oldName);
					identities.Add(newName, identityInformation);
				}
				if (defaults.TryGetValue(oldName, out IVistaDBDefaultValueInformation valueInformation))
				{
					((Database.DatabaseObject)valueInformation).Name = newName;
					defaults.Remove(oldName);
					defaults.Add(newName, valueInformation);
				}
				return column;
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AlterColumnType(string columnName, VistaDBType newType)
			{
				columnName = Row.Column.FixName(columnName);
				Row.Column oldColumn = (Row.Column)((IVistaDBTableSchema)this)[columnName];
				if (oldColumn == null)
					throw new VistaDBException(181, columnName);
				if (oldColumn.Type == newType)
					return oldColumn;
				Row.Column emptyColumnInstance = linkedStorage.CreateEmptyColumnInstance(newType, (short)oldColumn.MaxLength, oldColumn.CodePage, !linkedStorage.CaseSensitive, false);
				emptyColumnInstance.AssignAttributes(columnName, oldColumn.AllowNull, oldColumn.ReadOnly, oldColumn.Encrypted, oldColumn.Packed);
				ReplaceColumn(oldColumn, emptyColumnInstance);
				return emptyColumnInstance;
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AlterColumnType(string columnName, VistaDBType newType, int newMaxLength, int newCodePage)
			{
				columnName = Row.Column.FixName(columnName);
				Row.Column oldColumn = (Row.Column)((IVistaDBTableSchema)this)[columnName];
				if (oldColumn == null)
					throw new VistaDBException(181, columnName);
				bool flag = oldColumn.ExtendedType || oldColumn.FixedType || oldColumn.MaxLength == newMaxLength;
				if (oldColumn.Type == newType && flag && oldColumn.CodePage == newCodePage)
					return oldColumn;
				Row.Column emptyColumnInstance = linkedStorage.CreateEmptyColumnInstance(newType, (short)newMaxLength, newCodePage == 0 ? oldColumn.CodePage : newCodePage, !linkedStorage.CaseSensitive, false);
				emptyColumnInstance.AssignAttributes(columnName, oldColumn.AllowNull, oldColumn.ReadOnly, oldColumn.Encrypted, oldColumn.Packed);
				ReplaceColumn(oldColumn, emptyColumnInstance);
				return emptyColumnInstance;
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.AlterColumnOrder(string columnName, int order)
			{
				if (order < 0 || order >= Count)
					throw new VistaDBException(182, order.ToString());
				if (!TryGetValue(columnName, out Row.Column column1))
					throw new VistaDBException(181, columnName);
				TestSystemAttribute(column1);
				int rowIndex = column1.RowIndex;
				if (rowIndex == order)
					return column1;
				Row.Column column2 = column1;
				if (rowIndex < order)
				{
					int index1 = rowIndex;
					for (int index2 = rowIndex + 1; index2 <= order; ++index2)
					{
						Row.Column column3 = base[index2];
						RemoveAt(index1);
						Add(column3);
						//this[index1] = column3;
						column3.RowIndex = index1;
						++index1;
					}
				}
				else
				{
					int index1 = rowIndex;
					for (int index2 = rowIndex - 1; index2 >= order; --index2)
					{
						Row.Column column3 = base[index2];
						RemoveAt(index1);
						Insert(index1, column3);
						//this[index1] = column3;
						column3.RowIndex = index1;
						--index1;
					}
				}
				//this[order] = column2;
				RemoveAt(order);
				Insert(order, column2);
				column2.RowIndex = order;
				return column1;
			}

			IVistaDBColumnAttributes IVistaDBTableSchema.DefineColumnAttributes(string columnName, bool allowNull, bool readOnly, bool encrypted, bool packed, string caption, string description)
			{
				columnName = Row.Column.FixName(columnName);
				IVistaDBColumnAttributes columnAttributes = ((IVistaDBTableSchema)this)[columnName];
				if (columnAttributes == null)
					throw new VistaDBException(181, columnName);
				if (columnAttributes.IsSystem)
					return columnAttributes;
				((Row.Column)columnAttributes).AssignAttributes(columnName, allowNull, readOnly, encrypted, packed);
				columnAttributes.Caption = caption;
				columnAttributes.Description = description;
				return columnAttributes;
			}

			IVistaDBIndexInformation IVistaDBTableSchema.DefineIndex(string name, string keyExpression, bool primary, bool unique)
			{
				keyExpression = VistaDB.Engine.Core.Indexing.Index.FixKeyExpression(keyExpression);
				if (indexes.TryGetValue(name, out IVistaDBIndexInformation indexInformation1))
				{
					((Table.TableSchema.IndexCollection.IndexInformation)indexInformation1).KeyExpression = keyExpression;
					((Table.TableSchema.IndexCollection.IndexInformation)indexInformation1).Unique = unique;
					((Table.TableSchema.IndexCollection.IndexInformation)indexInformation1).Primary = primary;
					((Table.TableSchema.IndexCollection.IndexInformation)indexInformation1).Sparse = false;
					return indexInformation1;
				}
				if (primary)
				{
					foreach (Table.TableSchema.IndexCollection.IndexInformation indexInformation2 in indexes.Values)
					{
						if (indexInformation2.Primary)
							throw new VistaDBException(143, indexInformation2.Name);
					}
				}
				IVistaDBIndexInformation indexInformation3 = new Table.TableSchema.IndexCollection.IndexInformation("", name, keyExpression, unique, primary, false, false, false, false, false, 0UL, null);
				indexes.Add(name, indexInformation3);
				return indexInformation3;
			}

			void IVistaDBTableSchema.DropIndex(string name)
			{
				IVistaDBIndexInformation index = indexes[name];
				if (!indexes.TryGetValue(name, out index))
					throw new VistaDBException(sbyte.MaxValue, name);
				if (index.FKConstraint)
					throw new VistaDBException(132, name);
				if (!indexes.Remove(name))
					throw new VistaDBException(132, name);
			}

			void IVistaDBTableSchema.DefineIdentity(string columnName, string seedValue, string stepExpression)
			{
				columnName = Row.Column.FixName(columnName);
				if (identities.TryGetValue(columnName, out IVistaDBIdentityInformation identityInformation1))
				{
					((Table.TableSchema.IdentityCollection.IdentityInformation)identityInformation1).seedValue = seedValue;
					((Table.TableSchema.IdentityCollection.IdentityInformation)identityInformation1).stepExpression = stepExpression;
				}
				else
				{
					IVistaDBIdentityInformation identityInformation2 = new Table.TableSchema.IdentityCollection.IdentityInformation(null, null, columnName, seedValue, stepExpression);
					identities.Add(columnName, identityInformation2);
				}
			}

			void IVistaDBTableSchema.DropIdentity(string columnName)
			{
				columnName = Row.Column.FixName(columnName);
				if (!identities.Remove(columnName))
					throw new VistaDBException(187, columnName);
			}

			void IVistaDBTableSchema.DefineDefaultValue(string columnName, string scriptExpression, bool useInUpdate, string description)
			{
				columnName = Row.Column.FixName(columnName);
				if (defaults.TryGetValue(columnName, out IVistaDBDefaultValueInformation valueInformation1))
				{
					((Table.TableSchema.DefaultValueCollection.DefaultValueInformation)valueInformation1).Expression = scriptExpression;
					((Table.TableSchema.DefaultValueCollection.DefaultValueInformation)valueInformation1).UseInUpdate = useInUpdate;
					((Database.DatabaseObject)valueInformation1).Description = description;
				}
				else
				{
					IVistaDBDefaultValueInformation valueInformation2 = new Table.TableSchema.DefaultValueCollection.DefaultValueInformation(columnName, scriptExpression, useInUpdate, description);
					defaults.Add(columnName, valueInformation2);
				}
			}

			void IVistaDBTableSchema.DropDefaultValue(string columnName)
			{
				columnName = Row.Column.FixName(columnName);
				if (!defaults.Remove(columnName))
					throw new VistaDBException(194, columnName);
			}

			void IVistaDBTableSchema.DefineConstraint(string name, string scriptExpression, string description, bool insert, bool update, bool delete)
			{
				int option = 0;
				if (insert)
					option |= 1;
				if (update)
					option |= 2;
				if (delete)
					option |= 4;
				if (constraints.TryGetValue(name, out IVistaDBConstraintInformation constraintInformation))
				{
					((Table.TableSchema.ConstraintCollection.ConstraintInformation)constraintInformation).Expression = scriptExpression;
					((Database.DatabaseObject)constraintInformation).Description = description;
					((Table.TableSchema.ConstraintCollection.ConstraintInformation)constraintInformation).Option = option;
				}
				else
				{
					constraintInformation = new Table.TableSchema.ConstraintCollection.ConstraintInformation(name, scriptExpression, description, option);
					constraints.Add(name, constraintInformation);
				}
			}

			void IVistaDBTableSchema.DropConstraint(string name)
			{
				if (!constraints.Remove(name))
					throw new VistaDBException(197, name);
			}

			//public int Add(object value)
			//{
			//  return -1;
			//}

			private void Dispose(bool disposing)
			{
				if (disposing)
				{
					identities.Clear();
					defaults.Clear();
					Clear();
				}
				linkedStorage = null;
				map = null;
				indexes = null;
				identities = null;
				defaults = null;
				constraints = null;
				relationships = null;
				triggers = null;
			}

			public void Dispose()
			{
				if (isDisposed)
					return;
				isDisposed = true;
				GC.SuppressFinalize(this);
				Dispose(true);
			}

			~TableSchema()
			{
				if (isDisposed)
					return;
				isDisposed = true;
				Dispose(false);
			}

			IEnumerator<IVistaDBColumnAttributes> IEnumerable<IVistaDBColumnAttributes>.GetEnumerator()
			{
				return new CastableEnumerator<Row.Column, IVistaDBColumnAttributes>(GetEnumerator());
			}

			internal class IndexCollection : VistaDBKeyedCollection<string, IVistaDBIndexInformation>, IVistaDBIndexCollection, IVistaDBKeyedCollection<string, IVistaDBIndexInformation>, ICollection<IVistaDBIndexInformation>, IEnumerable<IVistaDBIndexInformation>, IEnumerable
			{
				internal IndexCollection()
				  : base(StringComparer.OrdinalIgnoreCase)
				{
				}

				internal class KeyColumnInformation : IVistaDBKeyColumn
				{
					private int rowIndex;
					private bool descend;

					internal KeyColumnInformation(byte highByte, byte lowByte)
					{
						rowIndex = (lowByte & 14) << 7 | highByte;
						descend = (lowByte & 1) != 0;
					}

					public int RowIndex
					{
						get
						{
							return rowIndex;
						}
					}

					public bool Descending
					{
						get
						{
							return descend;
						}
					}
				}

				internal class IndexInformation : Database.DatabaseObject, IVistaDBIndexInformation, IVistaDBDatabaseObject
				{
					private string indexFileName;
					private string keyExpression;
					private bool unique;
					private bool primary;
					private bool descend;
					private bool sparse;
					private bool fk_constraint;
					private bool temporary;
					private bool fts;
					private Table.TableSchema.IndexCollection.KeyColumnInformation[] keyStructure;
					private ulong position;

					public IndexInformation(string fileName, string name, string keyExpression, bool unique, bool primary, bool descend, bool sparse, bool fk_constraint, bool fts, bool temporary, ulong position, byte[] keyStruct)
					  : base(Database.VdbObjects.Index, name, null)
					{
						indexFileName = fileName;
						this.keyExpression = keyExpression;
						this.unique = unique;
						this.primary = primary;
						this.descend = descend;
						this.sparse = sparse;
						this.fk_constraint = fk_constraint;
						this.temporary = temporary;
						this.fts = fts;
						this.position = position;
						if (keyStruct == null)
							return;
						keyStructure = new Table.TableSchema.IndexCollection.KeyColumnInformation[keyStruct.Length / 2];
						for (int index1 = 0; index1 < keyStructure.Length; ++index1)
						{
							int index2 = 2 * index1;
							keyStructure[index1] = new Table.TableSchema.IndexCollection.KeyColumnInformation(keyStruct[index2], keyStruct[index2 + 1]);
						}
					}

					internal ulong HeaderPosition
					{
						get
						{
							return position;
						}
					}

					internal string KeyExpression
					{
						set
						{
							keyExpression = value;
							keyStructure = null;
						}
					}

					internal bool Unique
					{
						set
						{
							unique = value;
						}
					}

					internal bool Primary
					{
						set
						{
							primary = value;
						}
						get
						{
							return primary;
						}
					}

					internal bool Sparse
					{
						get
						{
							return sparse;
						}
						set
						{
							sparse = value;
						}
					}

					string IVistaDBIndexInformation.KeyExpression
					{
						get
						{
							return keyExpression;
						}
					}

					bool IVistaDBIndexInformation.Primary
					{
						get
						{
							return primary;
						}
					}

					bool IVistaDBIndexInformation.Unique
					{
						get
						{
							return unique;
						}
					}

					bool IVistaDBIndexInformation.FKConstraint
					{
						get
						{
							return fk_constraint;
						}
					}

					bool IVistaDBIndexInformation.FullTextSearch
					{
						get
						{
							return fts;
						}
					}

					bool IVistaDBIndexInformation.Temporary
					{
						get
						{
							return temporary;
						}
					}

					IVistaDBKeyColumn[] IVistaDBIndexInformation.KeyStructure
					{
						get
						{
							return keyStructure;
						}
					}

					public override bool Equals(object obj)
					{
						IVistaDBIndexInformation indexInformation = (IVistaDBIndexInformation)obj;
						if (base.Equals(obj) && string.Compare(keyExpression, indexInformation.KeyExpression) == 0 && !(primary ^ indexInformation.Primary))
							return !(unique ^ indexInformation.Unique);
						return false;
					}

					public override int GetHashCode()
					{
						return base.GetHashCode();
					}

					internal static string MakeUpKey(IVistaDBKeyColumn[] keyStruct, IVistaDBTableSchema schema)
					{
						string keyExpression = string.Empty;
						foreach (IVistaDBKeyColumn vistaDbKeyColumn in keyStruct)
						{
							string str = schema[vistaDbKeyColumn.RowIndex].Name;
							if (vistaDbKeyColumn.Descending)
								str = "DESC" + '(' + str + ')';
							keyExpression = keyExpression + str + ";";
						}
						return VistaDB.Engine.Core.Indexing.Index.FixKeyExpression(keyExpression);
					}

					internal void MakeUpKeyExpression(IVistaDBTableSchema schema)
					{
						keyExpression = Table.TableSchema.IndexCollection.IndexInformation.MakeUpKey(keyStructure, schema);
					}
				}
			}

			internal class DefaultValueCollection : VistaDBKeyedCollection<string, IVistaDBDefaultValueInformation>, IVistaDBDefaultValueCollection, IVistaDBKeyedCollection<string, IVistaDBDefaultValueInformation>, ICollection<IVistaDBDefaultValueInformation>, IEnumerable<IVistaDBDefaultValueInformation>, IEnumerable
			{
				internal DefaultValueCollection()
				  : base(StringComparer.OrdinalIgnoreCase)
				{
				}

				internal class DefaultValueInformation : Database.DatabaseObject, IVistaDBDefaultValueInformation, IVistaDBDatabaseObject
				{
					private string expression;
					private bool useInUpdate;

					internal DefaultValueInformation(string columnName, string expression, bool useInUpdate, string description)
					  : base(Database.VdbObjects.DefaultValue, columnName, description)
					{
						this.expression = expression;
						this.useInUpdate = useInUpdate;
					}

					internal string Expression
					{
						set
						{
							expression = value;
						}
					}

					internal bool UseInUpdate
					{
						set
						{
							useInUpdate = value;
						}
					}

					string IVistaDBDefaultValueInformation.ColumnName
					{
						get
						{
							return Name;
						}
					}

					string IVistaDBDefaultValueInformation.Expression
					{
						get
						{
							return expression;
						}
					}

					bool IVistaDBDefaultValueInformation.UseInUpdate
					{
						get
						{
							return useInUpdate;
						}
					}

					public override bool Equals(object obj)
					{
						if (base.Equals(obj) && string.Compare(expression, ((Table.TableSchema.DefaultValueCollection.DefaultValueInformation)obj).expression) == 0)
							return useInUpdate == ((Table.TableSchema.DefaultValueCollection.DefaultValueInformation)obj).useInUpdate;
						return false;
					}

					public override int GetHashCode()
					{
						return base.GetHashCode();
					}
				}
			}

			internal class IdentityCollection : VistaDBKeyedCollection<string, IVistaDBIdentityInformation>, IVistaDBIdentityCollection, IVistaDBKeyedCollection<string, IVistaDBIdentityInformation>, ICollection<IVistaDBIdentityInformation>, IEnumerable<IVistaDBIdentityInformation>, IEnumerable
			{
				internal IdentityCollection()
				  : base(StringComparer.OrdinalIgnoreCase)
				{
				}

				internal class IdentityInformation : Database.DatabaseObject, IVistaDBIdentityInformation, IVistaDBDatabaseObject
				{
					internal string stepExpression;
					internal string seedValue;
					internal string tableName;
					private Table.TableSchema.IdentityCollection.IdentityInformation.GetSeedValue getSeedValue;

					internal IdentityInformation(Table.TableSchema.IdentityCollection.IdentityInformation.GetSeedValue getSeedValue, string tableName, string columnName, string seedValue, string stepExpression)
					  : base(Database.VdbObjects.Identity, columnName, null)
					{
						this.getSeedValue = getSeedValue;
						this.tableName = tableName;
						this.stepExpression = stepExpression;
						this.seedValue = seedValue;
					}

					string IVistaDBIdentityInformation.ColumnName
					{
						get
						{
							return Name;
						}
					}

					string IVistaDBIdentityInformation.StepExpression
					{
						get
						{
							return stepExpression;
						}
					}

					string IVistaDBIdentityInformation.SeedValue
					{
						get
						{
							if (getSeedValue != null && string.Compare(seedValue, string.Empty) == 0)
								seedValue = getSeedValue(tableName, Name);
							return seedValue;
						}
					}

					internal bool CopySeedValue(Table.TableSchema.IdentityCollection.IdentityInformation originalIdentity)
					{
						if (originalIdentity == null || string.Compare(seedValue, string.Empty) != 0)
							return false;
						seedValue = ((IVistaDBIdentityInformation)originalIdentity).SeedValue;
						return true;
					}

					public override bool Equals(object obj)
					{
						Table.TableSchema.IdentityCollection.IdentityInformation identityInformation = (Table.TableSchema.IdentityCollection.IdentityInformation)obj;
						if (base.Equals(obj) && string.Compare(seedValue, identityInformation.seedValue) == 0 && string.Compare(stepExpression, identityInformation.stepExpression) == 0)
							return Database.DatabaseObject.EqualNames(tableName, identityInformation.tableName);
						return false;
					}

					public override int GetHashCode()
					{
						return base.GetHashCode();
					}

					internal delegate string GetSeedValue(string tableName, string columnName);
				}
			}

			internal class ConstraintCollection : VistaDBKeyedCollection<string, IVistaDBConstraintInformation>, IVistaDBConstraintCollection, IVistaDBKeyedCollection<string, IVistaDBConstraintInformation>, ICollection<IVistaDBConstraintInformation>, IEnumerable<IVistaDBConstraintInformation>, IEnumerable
			{
				internal ConstraintCollection()
				  : base(StringComparer.OrdinalIgnoreCase)
				{
				}

				internal class ConstraintInformation : Database.DatabaseObject, IVistaDBConstraintInformation, IVistaDBDatabaseObject
				{
					private string expression;
					private int option;

					internal ConstraintInformation(string name, string expression, string description, int option)
					  : base(Database.VdbObjects.Constraint, name, description)
					{
						this.expression = expression;
						this.option = option;
					}

					internal int Option
					{
						get
						{
							return option;
						}
						set
						{
							option = value;
						}
					}

					internal string Expression
					{
						set
						{
							expression = value;
						}
					}

					string IVistaDBConstraintInformation.Expression
					{
						get
						{
							return expression;
						}
					}

					bool IVistaDBConstraintInformation.AffectsInsertion
					{
						get
						{
							return Constraint.InsertionActivity(option);
						}
					}

					bool IVistaDBConstraintInformation.AffectsUpdate
					{
						get
						{
							return Constraint.UpdateActivity(option);
						}
					}

					bool IVistaDBConstraintInformation.AffectsDelete
					{
						get
						{
							return Constraint.DeleteActivity(option);
						}
					}

					public override bool Equals(object obj)
					{
						Table.TableSchema.ConstraintCollection.ConstraintInformation constraintInformation = (Table.TableSchema.ConstraintCollection.ConstraintInformation)obj;
						if (base.Equals(obj) && option == constraintInformation.option)
							return string.Compare(expression, constraintInformation.expression) == 0;
						return false;
					}

					public override int GetHashCode()
					{
						return base.GetHashCode();
					}
				}
			}
		}

		internal enum Status
		{
			NoAction,
			Update,
			Delete,
			Insert,
			InsertBlank,
			SynchPosition,
		}

		internal class ContextStack : List<Table.ContextStack.TableContext>
		{
			private Table parentTable;

			internal ContextStack(Table parentTable)
			{
				this.parentTable = parentTable;
			}

			internal void Push()
			{
				Add(new Table.ContextStack.TableContext(parentTable.activeOrder, parentTable.rowStatus));
			}

			internal void Pop()
			{
				if (Count == 0)
					return;
				parentTable.activeOrder = this[Count - 1].Restore(ref parentTable.rowStatus);
				RemoveAt(Count - 1);
			}

			internal class TableContext
			{
				private Table.Status status;
				private VistaDB.Engine.Core.Indexing.Index activeOrder;
				private DataStorage.StorageContext activeContext;

				internal TableContext(VistaDB.Engine.Core.Indexing.Index activeOrder, Table.Status status)
				{
					Init(activeOrder, status);
				}

				internal VistaDB.Engine.Core.Indexing.Index Restore(ref Table.Status status)
				{
					activeOrder.Context = activeContext;
					status = this.status;
					return activeOrder;
				}

				internal void Init(VistaDB.Engine.Core.Indexing.Index activeOrder, Table.Status status)
				{
					this.status = status;
					this.activeOrder = activeOrder;
					activeContext = activeOrder.Context;
				}
			}
		}
	}
}
