





using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Security;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
	internal abstract class DataStorage : IDisposable
	{
		private DataStorage.Operation currentOperation = DataStorage.Operation.None;
		private bool activeLinks = true;
		private string name;
		private string alias;
		private StorageHeader header;
		private Row currentRow;
		private Row satelliteRow;
		private Row topRow;
		private Row bottomRow;
		private Row defaultRow;
		private bool bgnOfData;
		private bool endOfData;
		private bool updateBufferStatus;
		private StorageHandle storageHandle;
		private StorageHandle lockStorageHandle;
		private Database wrapperDatabase;
		private bool shared;
		private bool shareReadOnly;
		private bool readOnly;
		private int asynchCounter;
		private int minizeMemoryCount;
		private FiltersList ordinaryFilters;
		private FiltersList optimizedFilters;
		private FiltersList beforeCreateFilters;
		private FiltersList afterCreateFilters;
		private FiltersList beforeUpdateFilters;
		private FiltersList afterUpdateFilters;
		private FiltersList afterDeleteFilters;
		private TranslationList translationList;
		private bool silence;
		private DirectConnection connection;
		private int lockTimeOut;
		private LockManager lockManager;
		private bool forcedBeforeAppend;
		private bool forcedAfterAppend;
		private bool forcedBeforeUpdate;
		private bool forcedAfterUpdate;
		private bool forcedAfterDelete;
		private Relationships relationships;
		private Encryption encryption;
		private DataStorage.StorageContext context;
		private DataStorage clonedStorage;
		private bool createdInMemoryYet;
		private bool finalization;
		internal uint lastprogress;
		private bool isDisposed;

		protected DataStorage(string fileName, string aliasName, DirectConnection connection, Database wrapperDatabase, Encryption encryption, DataStorage clonedStorage)
		{
			name = fileName;
			alias = aliasName;
			this.connection = connection;
			this.wrapperDatabase = wrapperDatabase;
			this.encryption = encryption;
			ordinaryFilters = new FiltersList();
			optimizedFilters = new FiltersList();
			beforeCreateFilters = new FiltersList();
			beforeUpdateFilters = new FiltersList();
			afterCreateFilters = new FiltersList();
			afterUpdateFilters = new FiltersList();
			afterDeleteFilters = new FiltersList();
			relationships = new Relationships();
			this.clonedStorage = clonedStorage;
		}

		internal uint Version
		{
			get
			{
				return Header.Version;
			}
			set
			{
				if (!IsMultiWriteSynchronization)
					return;
				Header.Version = value;
			}
		}

		internal int PageSize
		{
			get
			{
				return Header.PageSize;
			}
		}

		private bool IsNewVersion
		{
			get
			{
				return Header.NewVersion;
			}
		}

		internal bool IsDeleteOperation
		{
			get
			{
				return currentOperation == DataStorage.Operation.Delete;
			}
		}

		internal bool IsInsertOperation
		{
			get
			{
				return currentOperation == DataStorage.Operation.Insert;
			}
		}

		internal bool IsUpdateOperation
		{
			get
			{
				return currentOperation == DataStorage.Operation.Update;
			}
		}

		internal bool NoFileImage
		{
			get
			{
				return createdInMemoryYet;
			}
		}

		internal virtual bool NoLocks
		{
			get
			{
				if (shared || Handle.Mode.Shared)
					return IsShareReadOnly;
				return true;
			}
		}

		internal bool VirtualLocks
		{
			get
			{
				return !Handle.Mode.Shared;
			}
		}

		internal virtual ulong StorageId
		{
			get
			{
				return Header.Position;
			}
		}

		internal StorageHandle Handle
		{
			get
			{
				return storageHandle;
			}
		}

		internal StorageHeader Header
		{
			get
			{
				return header;
			}
		}

		internal DirectConnection ParentConnection
		{
			get
			{
				return connection;
			}
		}

		internal abstract CrossConversion Conversion { get; }

		internal bool BgnOfSet
		{
			get
			{
				return bgnOfData;
			}
			set
			{
				bgnOfData = value;
			}
		}

		internal bool EndOfSet
		{
			get
			{
				return endOfData;
			}
			set
			{
				endOfData = value;
			}
		}

		internal CultureInfo Culture
		{
			get
			{
				return header.Culture;
			}
		}

		internal Row CurrentRow
		{
			get
			{
				return currentRow;
			}
		}

		internal Row SatelliteRow
		{
			get
			{
				return satelliteRow;
			}
		}

		internal Row TopRow
		{
			get
			{
				return topRow;
			}
		}

		internal Row BottomRow
		{
			get
			{
				return bottomRow;
			}
		}

		internal Row DefaultRow
		{
			get
			{
				return defaultRow;
			}
		}

		internal ulong FilteredRowCount
		{
			get
			{
				return optimizedFilters.FilteredCount;
			}
		}

		internal TranslationList TranslationList
		{
			get
			{
				return translationList;
			}
		}

		internal virtual bool SuppressErrors
		{
			get
			{
				return silence;
			}
			set
			{
				silence = value;
			}
		}

		internal bool IsShared
		{
			get
			{
				if (!shared)
					return IsShareReadOnly;
				return true;
			}
		}

		internal virtual bool IsMultiWriteSynchronization
		{
			get
			{
				if (Handle.Mode.Shared)
					return !Handle.Mode.ReadOnlyShared;
				return false;
			}
		}

		internal bool IsReadOnly
		{
			get
			{
				if (!readOnly)
					return IsShareReadOnly;
				return true;
			}
		}

		internal bool IsShareReadOnly
		{
			get
			{
				if (WrapperDatabase != null)
					return WrapperDatabase.IsShareReadOnly;
				return shareReadOnly;
			}
		}

		internal bool IsTemporary
		{
			get
			{
				return Handle.Mode.Temporary;
			}
		}

		internal Database WrapperDatabase
		{
			get
			{
				return wrapperDatabase;
			}
		}

		internal virtual bool IsTransactionLogged
		{
			get
			{
				return false;
			}
		}

		internal virtual uint RowCount
		{
			get
			{
				return header.RowCount;
			}
		}

		internal virtual bool CaseSensitive
		{
			get
			{
				return false;
			}
		}

		internal virtual OperationCallbackDelegate operationDelegate
		{
			get
			{
				if (WrapperDatabase != null)
					return WrapperDatabase.operationDelegate;
				return null;
			}
			set
			{
				if (WrapperDatabase == null)
					return;
				WrapperDatabase.operationDelegate = value;
			}
		}

		internal virtual uint TotalOperationStatusLoops
		{
			get
			{
				if (WrapperDatabase != null)
					return WrapperDatabase.TotalOperationStatusLoops;
				return 1;
			}
			set
			{
				if (WrapperDatabase == null)
					return;
				WrapperDatabase.TotalOperationStatusLoops = value;
			}
		}

		internal virtual uint CurrentOperationStatusLoop
		{
			get
			{
				if (WrapperDatabase != null)
					return WrapperDatabase.CurrentOperationStatusLoop;
				return 0;
			}
			set
			{
				if (WrapperDatabase == null)
					return;
				WrapperDatabase.CurrentOperationStatusLoop = value;
			}
		}

		internal bool EnforcedConstraints
		{
			get
			{
				if (!afterCreateFilters.IsActive(Filter.FilterType.ConstraintAppend) && !afterUpdateFilters.IsActive(Filter.FilterType.ConstraintUpdate))
					return afterDeleteFilters.IsActive(Filter.FilterType.ConstraintDelete);
				return true;
			}
			set
			{
				if (value)
				{
					afterCreateFilters.ActivateByType(Filter.FilterType.ConstraintAppend);
					afterUpdateFilters.ActivateByType(Filter.FilterType.ConstraintUpdate);
					afterDeleteFilters.ActivateByType(Filter.FilterType.ConstraintDelete);
				}
				else
				{
					afterCreateFilters.DeactivateByType(Filter.FilterType.ConstraintAppend);
					afterUpdateFilters.DeactivateByType(Filter.FilterType.ConstraintUpdate);
					afterDeleteFilters.DeactivateByType(Filter.FilterType.ConstraintDelete);
				}
			}
		}

		internal bool EnforcedIdentities
		{
			get
			{
				return beforeCreateFilters.IsActive(Filter.FilterType.Identity);
			}
			set
			{
				if (value)
					beforeCreateFilters.ActivateByType(Filter.FilterType.Identity);
				else
					beforeCreateFilters.DeactivateByType(Filter.FilterType.Identity);
			}
		}

		internal virtual string Alias
		{
			get
			{
				return alias;
			}
			set
			{
				alias = value;
				VerifyAlias(alias);
			}
		}

		internal virtual string Name
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

		protected virtual EncryptionKey EncryptionKey
		{
			get
			{
				if (encryption != null)
					return encryption.EncryptionKeyString;
				return EncryptionKey.NullEncryptionKey;
			}
		}

		internal Encryption Encryption
		{
			get
			{
				return encryption;
			}
		}

		protected virtual Row LowScope
		{
			get
			{
				return topRow;
			}
		}

		protected virtual Row HighScope
		{
			get
			{
				return bottomRow;
			}
		}

		internal virtual bool PostponedSynchronization
		{
			get
			{
				if (ordinaryFilters.Count == 0)
					return optimizedFilters.Count == 0;
				return false;
			}
		}

		internal virtual DataStorage.StorageContext Context
		{
			get
			{
				DataStorage.StorageContext storageContext = context == null ? new DataStorage.StorageContext() : context;
				context = null;
				storageContext.Init(CurrentRow.CopyInstance(), SatelliteRow.CopyInstance(), EndOfSet, BgnOfSet, asynchCounter);
				return storageContext;
			}
			set
			{
				context = value;
				CurrentRow.Copy(value.Current);
				SatelliteRow.Copy(value.Satellite);
				asynchCounter = value.Asynch;
				BgnOfSet = value.Bof;
				EndOfSet = value.Eof;
			}
		}

		internal Row.Column LookForColumn(char[] buffer, int offset, bool containSpaces)
		{
			return currentRow.LookForColumn(buffer, offset, containSpaces);
		}

		internal Row.Column LookForColumn(string name)
		{
			return currentRow.LookForColumn(name);
		}

		protected void ResetEncryption()
		{
			encryption = null;
		}

		internal void OpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
		{
			bool flag = true;
			try
			{
				if (!openMode.Shared)
					openMode.VirtualLocks = true;
				OnOpenStorage(openMode, headerPosition);
				if (Handle.Mode.Temporary)
				{
					openMode.VirtualLocks = true;
					Handle.Mode.VirtualLocks = true;
				}
				ClearScope(DataStorage.ScopeType.UserScope);
				flag = false;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 101, Name);
			}
			finally
			{
				if (flag)
					CloseStorage();
			}
		}

		internal void CreateStorage(StorageHandle.StorageMode accessMode, ulong headerPosition, bool commit)
		{
			bool flag1 = true;
			try
			{
				bool flag2 = false;
				try
				{
					if (!accessMode.Shared)
						accessMode.VirtualLocks = true;
					OnCreateStorage(accessMode, headerPosition);
					if (Handle.Mode.Temporary)
					{
						accessMode.VirtualLocks = true;
						Handle.Mode.VirtualLocks = true;
					}
					FlushStorageVersion();
					flag2 = true;
				}
				finally
				{
					FinalizeChanges(!flag2, commit);
				}
				ClearScope(DataStorage.ScopeType.UserScope);
				flag1 = false;
				createdInMemoryYet = !commit;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 100, Name);
			}
			finally
			{
				if (flag1)
					CloseStorage();
			}
		}

		internal void CloseStorage()
		{
			try
			{
				if (isDisposed)
					return;
				if (lockManager != null)
					lockManager.UnlockAllItems();
				if (storageHandle == null)
					return;
				OnCloseStorage();
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 129);
			}
		}

		internal void DeclareNewStorage(object hint)
		{
			try
			{
				OnDeclareNewStorage(hint);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 112, Name == null || Name.Length == 0 ? Alias : Name);
			}
		}

		internal void SetFreeCluster(ulong clusterId, int pageCount)
		{
			OnSetFreeCluster(clusterId, pageCount);
		}

		internal ulong GetFreeCluster(int pageCount)
		{
			return OnGetFreeCluster(pageCount);
		}

		internal bool Import(DataStorage sourceStorage, bool interruptOnError)
		{
			if (!InitializeImport(sourceStorage, interruptOnError))
				return false;
			try
			{
				return OnImport(sourceStorage, interruptOnError) && FinalizeImport(sourceStorage, interruptOnError);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 332, sourceStorage.Name);
			}
		}

		internal bool Export(DataStorage destinationStorage, bool interruptOnError)
		{
			try
			{
				return OnExport(destinationStorage, interruptOnError);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 333, destinationStorage.Name);
			}
		}

		internal void ZapStorage(bool commit)
		{
			bool flag = false;
			try
			{
				FreezeRelationships();
				try
				{
					OnZapStorage(commit);
				}
				finally
				{
					DefreezeRelationships();
				}
				header.RowCount = 0U;
				FlushStorageVersion();
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 335, Name);
			}
			finally
			{
				FinalizeChanges(!flag, commit);
			}
		}

		internal void CleanUpDiskSpace(bool commit)
		{
			bool flag = false;
			try
			{
				OnCleanUpDiskSpace(commit);
				FlushStorageVersion();
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 339, Name);
			}
			finally
			{
				FinalizeChanges(!flag, commit);
			}
		}

		internal void SetRelationship(DataStorage masterStorage, DataStorage slaveStorage, Relationships.Type linkType, EvalStack linkProcessing, bool maxPriority)
		{
			relationships.AddRelation(masterStorage, slaveStorage, linkType, linkProcessing, maxPriority);
		}

		internal void ResetRelationship(DataStorage masterStorage, DataStorage slaveStorage)
		{
			if (relationships == null)
				return;
			relationships.RemoveRelation(masterStorage, slaveStorage);
		}

		internal IValue GetColumnValue(int rowIndex, VistaDBType crossType, Row sourceRow)
		{
			return OnGetColumnValue(rowIndex, crossType, sourceRow);
		}

		internal void PutColumnValue(int rowIndex, VistaDBType crossType, Row destinationRow, IValue columnValue)
		{
			OnPutColumnValue(rowIndex, crossType, destinationRow, columnValue);
		}

		internal void LockRow(uint rowId, bool userLock)
		{
			if (NoLocks)
				return;
			bool actualLock = false;
			OnLockRow(rowId, userLock, ref actualLock);
		}

		protected virtual void OnLockRow(uint rowId, bool userLock, ref bool actualLock)
		{
			lockManager.LockObject(userLock, rowId, LockManager.LockType.RowLock, ref actualLock, lockTimeOut);
		}

		internal void UnlockRow(uint rowId, bool userLock, bool instantly)
		{
			if (NoLocks)
				return;
			OnUnlockRow(rowId, userLock, instantly);
		}

		protected virtual void OnUnlockRow(uint rowId, bool userLock, bool instantly)
		{
			if (rowId == 0U)
				lockManager.UnlockAllItems();
			else
				lockManager.UnlockObject(userLock, rowId, LockManager.LockType.RowLock, !instantly);
		}

		internal void LockStorage()
		{
			if (NoLocks)
			{
				UpdateStorageVersion();
			}
			else
			{
				if (!OnLockStorage())
					return;
				UpdateStorageVersion();
			}
		}

		protected virtual bool OnLockStorage()
		{
			try
			{
				bool actualLock = false;
				lockManager.LockObject(false, StorageId, LockManager.LockType.FileLock, ref actualLock, lockTimeOut);
				return actualLock;
			}
			catch (Exception ex)
			{
				Header.ResetVersionInfo();
				throw ex;
			}
		}

		internal void UnlockStorage(bool doNotWaitForSynch)
		{
			if (NoLocks)
				return;
			OnUnlockStorage(doNotWaitForSynch);
		}

		protected virtual void OnUnlockStorage(bool doNotWaitForSynch)
		{
			lockManager.UnlockObject(false, StorageId, LockManager.LockType.FileLock, !doNotWaitForSynch);
		}

		internal void LowLevelLockRow(uint rowId)
		{
			try
			{
				OnLowLevelLockRow(rowId);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 164, rowId.ToString());
			}
		}

		internal void LowLevelUnlockRow(uint rowId)
		{
			try
			{
				OnLowLevelUnlockRow(rowId);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 165, rowId.ToString());
			}
		}

		internal void LowLevelLockStorage(ulong offset, int bytes)
		{
			OnLowLevelLockStorage(offset, bytes);
		}

		internal void LowLevelUnlockStorage(ulong offset, int bytes)
		{
			OnLowLevelUnlockStorage(offset, bytes);
		}

		internal void ReadRow(Row row)
		{
			row.Read(this, Row.RowScope.All, true);
		}

		internal void WriteRow(Row row)
		{
			row.Write(this, Row.RowScope.All);
		}

		internal void ClearCurrentRow()
		{
			CurrentRow.Clear();
		}

		internal void ClearSatteliteRow()
		{
			SatelliteRow.Clear();
		}

		internal void BlockCreateGenerators(Row newRow)
		{
			List<Filter> filterList = new List<Filter>();
			for (int index = 0; index < beforeCreateFilters.Count; ++index)
			{
				Filter beforeCreateFilter = beforeCreateFilters[index];
				if (beforeCreateFilter.TypeId == Filter.FilterType.DefaultValueInsertGenerator)
				{
					if (newRow[beforeCreateFilter.FirstColumn.RowIndex].IsNull)
						beforeCreateFilter.Activate(false);
					else
						beforeCreateFilter.Deactivate();
				}
			}
		}

		internal void BlockUpdateGenerators(Row newRow)
		{
			for (int index = 0; index < beforeUpdateFilters.Count; ++index)
			{
				Filter beforeUpdateFilter = beforeUpdateFilters[index];
				if (beforeUpdateFilter.TypeId == Filter.FilterType.DefaultValueUpdateGenerator)
				{
					if (!newRow[beforeUpdateFilter.FirstColumn.RowIndex].Edited)
						beforeUpdateFilter.Activate(false);
					else
						beforeUpdateFilter.Deactivate();
				}
			}
		}

		internal void Top()
		{
			OnTop();
		}

		internal void Bottom()
		{
			OnBottom();
		}

		internal void AddAsynch(int deltaRowCount)
		{
			asynchCounter += deltaRowCount;
		}

		internal void Synch()
		{
			int asynchCounter = this.asynchCounter;
			if (!AssumeLink(null, false))
				return;
			this.asynchCounter = asynchCounter;
			try
			{
				OnSynch(this.asynchCounter);
			}
			finally
			{
				this.asynchCounter = 0;
				ResumeLink(null, false);
			}
		}

		internal void MinimizeCache()
		{
			if (currentOperation != DataStorage.Operation.None)
				return;
			++minizeMemoryCount;
			if (minizeMemoryCount % 10 == 0)
				MinimizeMemoryCache(false);
			if (minizeMemoryCount % 100 != 0)
				return;
			Handle.ClearWholeCacheButHeader(StorageId);
		}

		internal void ResetAsynch()
		{
			asynchCounter = 0;
		}

		internal void MoveToRow(Row row)
		{
			SetCurrentRow(row);
			GoCurrentRow(true);
			uint transactionId = TransactionId;
			while (!EndOfSet && !PassTransaction(CurrentRow, transactionId))
				NextRow();
		}

		internal void GoCurrentRow(bool soft)
		{
			OnGoCurrentRow(soft || currentOperation == DataStorage.Operation.Seek);
		}

		internal void PrevRow()
		{
			bool endOfSet = EndOfSet;
			do
			{
				OnPrevRow();
				updateBufferStatus = true;
				UpdateCurrentRow();
				endOfData = endOfData || endOfSet && bgnOfData;
			}
			while (!PassOrdinaryFilters() || !ActivateLinks());
			MinimizeCache();
		}

		internal void NextRow()
		{
			bool bgnOfSet = BgnOfSet;
			do
			{
				OnNextRow();
				updateBufferStatus = true;
				UpdateCurrentRow();
				bgnOfData = bgnOfData || bgnOfSet && endOfData;
			}
			while (!PassOrdinaryFilters() || !ActivateLinks());
			MinimizeCache();
		}

		internal Row CompileRow(string keyEvaluationExpression, bool initTop)
		{
			return OnCompileRow(keyEvaluationExpression, initTop);
		}

		internal bool SeekRow(Row row, bool partialMatching)
		{
			return OnSeekRow(row, partialMatching);
		}

		internal void SetCurrentRow(Row row)
		{
			OnSetCurrentRow(row);
		}

		internal void SetSatelliteRow(Row row)
		{
			OnSetSatelliteRow(row);
		}

		internal void SaveRow()
		{
			SatelliteRow.Copy(CurrentRow);
		}

		internal void RestoreRow()
		{
			CurrentRow.Copy(SatelliteRow);
		}

		internal void PrepareEditStatus()
		{
			SatelliteRow.ClearEditStatus();
		}

		internal bool ImportRow(Row sourceRow, Row destinationRow)
		{
			return OnImportRow(sourceRow, destinationRow);
		}

		internal void RereadExtendedColumn(ExtendedColumn column, Row rowKey)
		{
			updateBufferStatus = true;
			OnRereadExtendedColumn(column, rowKey);
		}

		internal void ActivateFilters()
		{
			ordinaryFilters.Activate();
			optimizedFilters.Activate();
			beforeCreateFilters.Activate();
			beforeUpdateFilters.Activate();
			afterCreateFilters.Activate();
			afterUpdateFilters.Activate();
			afterDeleteFilters.Activate();
		}

		internal void DeactivateFilters()
		{
			ordinaryFilters.Deactivate();
			optimizedFilters.Deactivate();
			beforeCreateFilters.Deactivate();
			beforeUpdateFilters.Deactivate();
			afterCreateFilters.Deactivate();
			afterUpdateFilters.Deactivate();
			afterDeleteFilters.Deactivate();
		}

		internal void DetachFilters()
		{
			beforeCreateFilters.Clear();
			forcedBeforeAppend = true;
			afterCreateFilters.Clear();
			forcedAfterAppend = true;
			beforeUpdateFilters.Clear();
			forcedBeforeUpdate = true;
			afterUpdateFilters.Clear();
			forcedAfterUpdate = true;
			afterDeleteFilters.Clear();
			forcedAfterDelete = true;
		}

		internal void DetachFilter(Filter.FilterType type, Filter filter)
		{
			switch (type)
			{
				case Filter.FilterType.Ordinary:
					ordinaryFilters.RemoveFilter(filter);
					break;
				case Filter.FilterType.Optimized:
					optimizedFilters.RemoveFilter(filter);
					break;
			}
		}

		internal void DetachFiltersByType(Filter.FilterType type)
		{
			switch (type)
			{
				case Filter.FilterType.Optimized:
					optimizedFilters.ClearId(type);
					break;
				case Filter.FilterType.DefaultValueInsertGenerator:
					break;
				case Filter.FilterType.DefaultValueUpdateGenerator:
					break;
				case Filter.FilterType.Identity:
					break;
				case Filter.FilterType.ConstraintAppend:
					break;
				case Filter.FilterType.ConstraintUpdate:
					break;
				case Filter.FilterType.ConstraintDelete:
					break;
				default:
					ordinaryFilters.ClearId(type);
					break;
			}
		}

		internal void AttachFilter(Filter filter)
		{
			switch (filter.TypeId)
			{
				case Filter.FilterType.Optimized:
					optimizedFilters.AddFilter(filter);
					break;
				case Filter.FilterType.DefaultValueInsertGenerator:
				case Filter.FilterType.Identity:
					forcedBeforeAppend = beforeCreateFilters.AddFilter(filter) < 0;
					break;
				case Filter.FilterType.DefaultValueUpdateGenerator:
					forcedBeforeUpdate = beforeUpdateFilters.AddFilter(filter) < 0;
					break;
				case Filter.FilterType.ReadOnly:
				case Filter.FilterType.ConstraintUpdate:
					forcedAfterUpdate = afterUpdateFilters.AddFilter(filter) < 0;
					break;
				case Filter.FilterType.ConstraintAppend:
					forcedAfterAppend = afterCreateFilters.AddFilter(filter) < 0;
					break;
				case Filter.FilterType.ConstraintDelete:
					forcedAfterDelete = afterDeleteFilters.AddFilter(filter) < 0;
					break;
				default:
					ordinaryFilters.AddFilter(filter);
					break;
			}
		}

		internal void DetachIdentityFilter(Row.Column column)
		{
			DetachFilterByColumnAndType(beforeCreateFilters, column, Filter.FilterType.Identity);
		}

		internal void DetachDefaultValueFilter(Row.Column column)
		{
			DetachFilterByColumnAndType(beforeUpdateFilters, column, Filter.FilterType.DefaultValueUpdateGenerator);
			DetachFilterByColumnAndType(beforeCreateFilters, column, Filter.FilterType.DefaultValueInsertGenerator);
		}

		internal void DetachConstraintFilter(string name)
		{
			DetachConstraintByName(afterCreateFilters, name, Filter.FilterType.ConstraintAppend);
			DetachConstraintByName(afterUpdateFilters, name, Filter.FilterType.ConstraintUpdate);
			DetachConstraintByName(afterDeleteFilters, name, Filter.FilterType.ConstraintDelete);
		}

		internal void DetachReadonlyFilter(string name)
		{
			DetachConstraintByName(afterUpdateFilters, name, Filter.FilterType.ReadOnly);
		}

		internal Filter GetFilter(Filter.FilterType type, int index)
		{
			switch (type)
			{
				case Filter.FilterType.Ordinary:
					return ordinaryFilters[index];
				case Filter.FilterType.Optimized:
					return optimizedFilters[index];
				default:
					return null;
			}
		}

		internal bool IsClearScope
		{
			get
			{
				return OnIsClearScope();
			}
		}

		internal bool ActivateLinks()
		{
			if (!activeLinks)
				return true;
			bool assumed = false;
			try
			{
				if (!AssumeLinks(false))
					return false;
				try
				{
					for (int index = 0; index < relationships.Count; ++index)
					{
						if (!relationships[index].Activate(this))
							return false;
					}
					assumed = true;
					return assumed;
				}
				catch (Exception ex)
				{
					throw new VistaDBException(ex, 253, Name);
				}
			}
			finally
			{
				ResumeLinks(false, assumed);
			}
		}

		internal bool CreateLinks()
		{
			if (!activeLinks)
				return true;
			bool assumed = false;
			try
			{
				if (!AssumeLinks(true))
					return false;
				try
				{
					for (int index = 0; index < relationships.Count; ++index)
					{
						if (!relationships[index].Create(this))
							return false;
					}
					assumed = true;
					return assumed;
				}
				catch (Exception ex)
				{
					throw new VistaDBException(ex, byte.MaxValue, Name);
				}
			}
			finally
			{
				ResumeLinks(true, assumed);
			}
		}

		internal bool UpdateLinks()
		{
			if (!activeLinks)
				return true;
			bool assumed = false;
			try
			{
				if (!AssumeLinks(true))
					return false;
				try
				{
					for (int index = 0; index < relationships.Count; ++index)
					{
						if (!relationships[index].Update(this))
							return false;
					}
					assumed = true;
					return assumed;
				}
				catch (Exception ex)
				{
					throw new VistaDBException(ex, 257, Name);
				}
			}
			finally
			{
				ResumeLinks(true, assumed);
			}
		}

		internal bool DeleteLinks()
		{
			if (!activeLinks)
				return true;
			bool assumed = false;
			try
			{
				if (!AssumeLinks(true))
					return false;
				try
				{
					for (int index = relationships.Count - 1; index >= 0; --index)
					{
						if (!relationships[index].Delete(this))
							return false;
					}
					assumed = true;
					return assumed;
				}
				catch (Exception ex)
				{
					throw new VistaDBException(ex, 258, Name);
				}
			}
			finally
			{
				ResumeLinks(true, assumed);
			}
		}

		internal bool AssumeLink(Relationships.Relation link, bool toModify)
		{
			return OnAssumeLink(link, toModify);
		}

		internal bool ResumeLink(Relationships.Relation link, bool toModify)
		{
			return OnResumeLink(link, toModify);
		}

		internal void CommitLinkFrom(DataStorage externalStorage)
		{
			if (externalStorage == this)
				return;
			MaskRelationships(this, externalStorage, false);
			MaskRelationships(externalStorage, this, false);
			try
			{
				CommitStorageVersion();
			}
			finally
			{
				MaskRelationships(this, externalStorage, true);
				MaskRelationships(externalStorage, this, true);
			}
		}

		internal void RollBackLinkFrom(DataStorage externalStorage)
		{
			if (externalStorage == this)
				return;
			MaskRelationships(this, externalStorage, false);
			MaskRelationships(externalStorage, this, false);
			try
			{
				RollbackStorageVersion();
			}
			finally
			{
				MaskRelationships(this, externalStorage, true);
				MaskRelationships(externalStorage, this, true);
			}
		}

		internal bool ActivateLink(Relationships.Relation link)
		{
			if (!link.Active)
				return true;
			try
			{
				DataStorage slaveStorage = link.SlaveStorage;
				if (slaveStorage == this)
					return true;
				DataStorage masterStorage = link.MasterStorage;
				if (!slaveStorage.DoBeforeActivateLinkFrom(masterStorage, link))
					return false;
				bool activated = false;
				try
				{
					Row link1 = slaveStorage.DoEvaluateLink(masterStorage, link.Evaluation, masterStorage.CurrentRow, slaveStorage.CurrentRow);
					activated = slaveStorage.DoActivateLinkFrom(masterStorage, link, link1) || link.Type == Relationships.Type.One_To_ZeroOrOne;
				}
				finally
				{
					activated = slaveStorage.DoAfterActivateLinkFrom(masterStorage, link, activated);
				}
				return activated;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 253, Name);
			}
		}

		internal bool UpdateLink(Relationships.Relation link)
		{
			if (!link.Active)
				return true;
			try
			{
				DataStorage slaveStorage = link.SlaveStorage;
				DataStorage masterStorage = link.MasterStorage;
				if (!slaveStorage.DoBeforeUpdateLinkFrom(masterStorage))
					return false;
				bool updated = false;
				try
				{
					updated = slaveStorage.DoUpdateLinkFrom(masterStorage, link.Type, slaveStorage.DoEvaluateLink(masterStorage, link.Evaluation, masterStorage.CurrentRow, slaveStorage.CurrentRow), slaveStorage.DoEvaluateLink(masterStorage, link.Evaluation, masterStorage.SatelliteRow, slaveStorage.SatelliteRow)) || link.Type == Relationships.Type.One_To_ZeroOrOne;
				}
				finally
				{
					updated = slaveStorage.DoAfterUpdateLinkFrom(masterStorage, updated);
				}
				return updated;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 256, Name);
			}
		}

		internal bool CreateLink(Relationships.Relation link)
		{
			if (!link.Active)
				return true;
			try
			{
				DataStorage masterStorage = link.MasterStorage;
				DataStorage slaveStorage = link.SlaveStorage;
				if (!slaveStorage.DoBeforeCreateLinkFrom(masterStorage))
					return false;
				bool passed = false;
				try
				{
					passed = slaveStorage.DoCreateLinkFrom(masterStorage, link.Type, slaveStorage.DoEvaluateLink(masterStorage, link.Evaluation, masterStorage.SatelliteRow, slaveStorage.SatelliteRow)) || link.Type == Relationships.Type.One_To_ZeroOrOne;
				}
				finally
				{
					passed = slaveStorage.DoAfterCreateLinkFrom(masterStorage, passed);
				}
				return passed;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 254, Name);
			}
		}

		internal bool DeleteLink(Relationships.Relation link)
		{
			if (!link.Active)
				return true;
			try
			{
				DataStorage masterStorage = link.MasterStorage;
				DataStorage slaveStorage = link.SlaveStorage;
				if (!slaveStorage.DoBeforeDeleteLinkFrom(masterStorage))
					return false;
				bool deleted = false;
				try
				{
					deleted = slaveStorage.DoDeleteLinkFrom(masterStorage, link.Type, slaveStorage.DoEvaluateLink(masterStorage, link.Evaluation, masterStorage.CurrentRow, slaveStorage.CurrentRow));
				}
				finally
				{
					deleted = slaveStorage.DoAfterDeleteLinkFrom(masterStorage, deleted);
				}
				return deleted;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 258, Name);
			}
		}

		internal void ReleaseLinkingLock(DataStorage externalStorage)
		{
			if (externalStorage == this)
				return;
			MaskRelationships(this, externalStorage, false);
			MaskRelationships(externalStorage, this, false);
			try
			{
				ReleaseLocks();
			}
			finally
			{
				MaskRelationships(this, externalStorage, true);
				MaskRelationships(externalStorage, this, true);
			}
		}

		internal bool CreateRow(bool commit, bool blank)
		{
			DataStorage.Operation currentOperation = this.currentOperation;
			bool created = false;
			try
			{
				if (IsReadOnly)
					throw new VistaDBException(337, Name);
				this.currentOperation = DataStorage.Operation.Insert;
				if (!DoBeforeCreateRow())
					return false;
				try
				{
					try
					{
						if (!OnCreateRow(blank, SatelliteRow) || !CreateLinks() || !FlushCurrentRow())
							return false;
						DoIncreaseRowCount();
						FlushStorageVersion();
						PassAfterCreateFilters();
						created = true;
					}
					finally
					{
						created = DoAfterCreateRow(created);
					}
				}
				catch (Exception ex)
				{
					created = false;
					throw ex;
				}
				finally
				{
					FinalizeChanges(!created, commit);
				}
				return true;
			}
			catch (Exception ex)
			{
				SetCurrentRow(BottomRow);
				throw ex;
			}
			finally
			{
				SatelliteRow.ClearEditStatus();
				CurrentRow.ClearEditStatus();
				this.currentOperation = currentOperation;
			}
		}

		internal bool UpdateRow(bool commit)
		{
			DataStorage.Operation currentOperation = this.currentOperation;
			bool updated = false;
			try
			{
				if (IsReadOnly)
					throw new VistaDBException(337, Name);
				this.currentOperation = DataStorage.Operation.Update;
				uint rowId = CurrentRow.RowId;
				if (!DoBeforeUpdateRow(rowId))
					return false;
				try
				{
					try
					{
						if (!OnUpdateRow(CurrentRow, SatelliteRow) || !UpdateLinks() || !FlushCurrentRow())
							return false;
						DoUpdateRowCount();
						FlushStorageVersion();
						PassAfterUpdateFilters();
						updated = true;
					}
					finally
					{
						updated = DoAfterUpdateRow(rowId, updated);
					}
				}
				catch (Exception ex)
				{
					updated = false;
					throw;
				}
				finally
				{
					FinalizeChanges(!updated, commit);
				}
				return true;
			}
			catch (Exception ex)
			{
				SetCurrentRow(BottomRow);
				throw;
			}
			finally
			{
				SatelliteRow.ClearEditStatus();
				CurrentRow.ClearEditStatus();
				this.currentOperation = currentOperation;
			}
		}

		internal bool DeleteRow(bool commit)
		{
			DataStorage.Operation currentOperation = this.currentOperation;
			bool deleted = false;
			try
			{
				if (IsReadOnly)
					throw new VistaDBException(337, Name);
				this.currentOperation = DataStorage.Operation.Delete;
				uint rowId = CurrentRow.RowId;
				if (!DoBeforeDeleteRow(rowId))
					return false;
				try
				{
					try
					{
						if (!OnDeleteRow(CurrentRow) || !DeleteLinks() || !FlushCurrentRow())
							return false;
						DoDecreaseRowCount();
						FlushStorageVersion();
						PassAfterDeleteFilters();
						deleted = true;
					}
					finally
					{
						deleted = DoAfterDeleteRow(rowId, deleted);
					}
				}
				catch (Exception ex)
				{
					deleted = false;
					throw;
				}
				finally
				{
					FinalizeChanges(!deleted, commit);
				}
				return true;
			}
			catch (Exception ex)
			{
				SetCurrentRow(BottomRow);
				throw;
			}
			finally
			{
				SatelliteRow.ClearEditStatus();
				CurrentRow.ClearEditStatus();
				this.currentOperation = currentOperation;
			}
		}

		internal void UpdateStorageVersion()
		{
			bool newVersion = false;
			OnUpdateStorageVersion(ref newVersion);
		}

		internal void FlushStorageVersion()
		{
			try
			{
				if (IsReadOnly)
					throw new VistaDBException(337, Name);
				OnFlushStorageVersion();
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}

		internal void FlushDefaultRow()
		{
			OnFlushDefaultRow();
		}

		internal void ForceReread()
		{
			updateBufferStatus = true;
		}

		internal void CreateTranslationsList(DataStorage destinationStorage)
		{
			translationList = OnCreateTranslationsList(destinationStorage);
		}

		internal void FreeTranslationsList()
		{
			OnFreeTranslationsList();
		}

		internal void VerifyAlias(string alias)
		{
			if (!DirectConnection.IsCorrectAlias(alias))
				throw new VistaDBException(152, alias);
		}

		internal void ClearScope(DataStorage.ScopeType scope)
		{
			OnClearScope(scope);
		}

		internal bool SetScope(Row lowValue, Row highValue, DataStorage.ScopeType scopes, bool exactMatching)
		{
			if (OnSetScope(lowValue, highValue, scopes, exactMatching))
				return true;
			ClearScope(scopes);
			return false;
		}

		internal void GetScope(out IVistaDBRow lowValue, out IVistaDBRow highValue)
		{
			OnGetScope(out lowValue, out highValue);
		}

		internal bool NotifyChangedEnvironment(Connection.Settings variable, object newValue)
		{
			switch (variable)
			{
				case Connection.Settings.LOCKTIMEOUT:
					lockTimeOut = (int)newValue * 1000;
					break;
				case Connection.Settings.PERSISTENTLOCKS:
					if (storageHandle != lockStorageHandle && lockStorageHandle != null)
					{
						lockStorageHandle.Persistent = (bool)newValue;
						break;
					}
					break;
				default:
					return true;
			}
			return OnNotifyChangedEnvironment(variable, newValue);
		}

		internal void FreezeRelationships()
		{
			activeLinks = relationships.Freeze(activeLinks);
		}

		internal void DefreezeRelationships()
		{
			activeLinks = relationships.Defreeze(activeLinks);
		}

		internal void MaskRelationships(DataStorage masterStorage, DataStorage slaveStorage, bool activate)
		{
			relationships.MaskRelationship(masterStorage, slaveStorage, activate);
			activeLinks = activeLinks || masterStorage != slaveStorage && activate;
		}

		internal void CommitStorageVersion()
		{
			try
			{
				OnCommitStorageVersion();
			}
			finally
			{
				MinimizeMemoryCache(false);
			}
		}

		internal void RollbackStorageVersion()
		{
			if (Handle == null)
				return;
			try
			{
				OnRollbackStorageVersion();
			}
			finally
			{
				MinimizeMemoryCache(false);
			}
		}

		internal bool MinimizeMemoryCache(bool forceClearing)
		{
			return OnMinimizeMemoryCache(forceClearing);
		}

		internal void ReleaseLocks()
		{
			try
			{
				OnReleaseLocks();
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 166, Name);
			}
		}

		internal void FinalizeChanges(bool rollback, bool commit)
		{
			if (finalization)
				return;
			finalization = true;
			try
			{
				OnFinalizeChanges(rollback, commit);
			}
			finally
			{
				finalization = false;
			}
		}

		internal void CallOperationStatusDelegate(uint progress, VistaDBOperationStatusTypes operation, string name, string message)
		{
			if (operationDelegate == null)
				return;
			try
			{
				if (progress == uint.MaxValue)
					++CurrentOperationStatusLoop;
				else
					progress = (progress + CurrentOperationStatusLoop * 100U) / TotalOperationStatusLoops;
				operationDelegate(new DataStorage.OperationCallbackStatus((int)progress, operation, name, message));
			}
			catch (Exception ex)
			{
			}
		}

		internal void CallOperationStatusDelegate(uint progress, VistaDBOperationStatusTypes operation)
		{
			if (operationDelegate == null || (int)progress == (int)lastprogress)
				return;
			lastprogress = progress;
			try
			{
				if (progress == uint.MaxValue)
					++CurrentOperationStatusLoop;
				else
					progress = (progress + CurrentOperationStatusLoop * 100U) / TotalOperationStatusLoops;
				operationDelegate(new DataStorage.OperationCallbackStatus((int)progress, operation, Name));
			}
			catch (Exception ex)
			{
			}
		}

		protected virtual Row OnCreateEmptyRowInstance()
		{
			return null;
		}

		protected virtual Row OnCreateEmptyRowInstance(int maxColCount)
		{
			return null;
		}

		internal Row CreateEmptyRowInstance()
		{
			return OnCreateEmptyRowInstance();
		}

		internal Row CreateEmptyRowInstance(int maxColCount)
		{
			return OnCreateEmptyRowInstance(maxColCount);
		}

		internal static Row.Column CreateRowColumn(VistaDBType type, bool caseInsensitive, CultureInfo culture)
		{
			switch (type)
			{
				case VistaDBType.Char:
				case VistaDBType.NChar:
					return new NCharColumn(null, 8192, culture, caseInsensitive, NCharColumn.DefaultUnicode);
				case VistaDBType.VarChar:
				case VistaDBType.NVarChar:
					return new NVarcharColumn(null, 8192, culture, caseInsensitive, NCharColumn.DefaultUnicode);
				case VistaDBType.Text:
				case VistaDBType.NText:
					return new NTextColumn(null, culture, caseInsensitive, NCharColumn.DefaultUnicode);
				case VistaDBType.VarChar | VistaDBType.NVarChar:
					return new TinyIntColumn();
				case VistaDBType.TinyInt:
					return new TinyIntColumn();
				case VistaDBType.SmallInt:
					return new SmallIntColumn();
				case VistaDBType.Int:
					return new IntColumn();
				case VistaDBType.BigInt:
					return new BigIntColumn();
				case VistaDBType.Real:
					return new RealColumn();
				case VistaDBType.Float:
					return new FloatColumn();
				case VistaDBType.Decimal:
					return new DecimalColumn();
				case VistaDBType.Money:
					return new MoneyColumn();
				case VistaDBType.SmallMoney:
					return new SmallMoneyColumn();
				case VistaDBType.Bit:
					return new BitColumn();
				case VistaDBType.NChar | VistaDBType.SmallMoney:
					return new _DateColumn();
				case VistaDBType.DateTime:
					return new DateTimeColumn();
				case VistaDBType.Image:
					return new BlobColumn();
				case VistaDBType.VarBinary:
					return new BinaryColumn();
				case VistaDBType.UniqueIdentifier:
					return new UniqueIdentifierColumn();
				case VistaDBType.SmallDateTime:
					return new SmallDateTimeColumn();
				case VistaDBType.Timestamp:
					return new Timestamp();
				default:
					return null;
			}
		}

		internal static Row.Column CreateRowColumn(VistaDBType type, int maxLength, bool caseInsensitive, CultureInfo culture)
		{
			switch (type)
			{
				case VistaDBType.Char:
				case VistaDBType.NChar:
					return new NCharColumn(null, maxLength, culture, caseInsensitive, NCharColumn.DefaultUnicode);
				case VistaDBType.VarChar:
				case VistaDBType.NVarChar:
					return new NVarcharColumn(null, maxLength, culture, caseInsensitive, NCharColumn.DefaultUnicode);
				default:
					return DataStorage.CreateRowColumn(type, caseInsensitive, culture);
			}
		}

		internal Row.Column CreateSQLUnicodeColumnInstance()
		{
			return new UnicodeColumn(null, Culture, !CaseSensitive);
		}

		internal Row.Column CreateEmptyColumnInstance(VistaDBType type)
		{
			Row.Column rowColumn = DataStorage.CreateRowColumn(type, !CaseSensitive, Culture);
			if (rowColumn == null)
				throw new VistaDBException(153, type.ToString());
			return rowColumn;
		}

		internal Row.Column CreateEmptyColumnInstance(VistaDBType type, short maxLength, int codePage, bool caseInsensitive, bool syncColumn)
		{
			if (syncColumn)
			{
				switch (type)
				{
					case VistaDBType.UniqueIdentifier:
						return new SyncOriginator();
					case VistaDBType.Timestamp:
						return new SyncTimestamp();
					default:
						return null;
				}
			}
			else
			{
				switch (type)
				{
					case VistaDBType.Char:
						return new CharColumn(null, maxLength, codePage, Culture, caseInsensitive);
					case VistaDBType.NChar:
						return new NCharColumn(null, maxLength, Culture, caseInsensitive, codePage);
					case VistaDBType.VarChar:
						return new VarcharColumn(null, maxLength, codePage, Culture, caseInsensitive);
					case VistaDBType.NVarChar:
						return new NVarcharColumn(null, maxLength, Culture, caseInsensitive, codePage);
					case VistaDBType.Text:
						return new TextColumn(null, codePage, Culture, caseInsensitive);
					case VistaDBType.NText:
						return new NTextColumn(null, Culture, caseInsensitive, codePage);
					default:
						return CreateEmptyColumnInstance(type);
				}
			}
		}

		protected virtual void DoAfterConstruction(int pageSize, CultureInfo culture)
		{
			string name = this.name;
			string alias = this.alias;
			this.name = null;
			this.alias = null;
			Name = name;
			Alias = alias;
			header = DoCreateHeaderInstance(pageSize, culture, clonedStorage);
			lockManager = new LockManager(this);
			ActivateEnvironment();
		}

		protected virtual StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
		{
			return null;
		}

		protected virtual Row DoAllocateCurrentRow()
		{
			return CreateEmptyRowInstance();
		}

		protected virtual Row DoAllocateSatteliteRow()
		{
			return CurrentRow.CopyInstance();
		}

		protected virtual Row DoAllocateTopRow()
		{
			return CurrentRow.CopyInstance();
		}

		protected virtual Row DoAllocateBottomRow()
		{
			return CurrentRow.CopyInstance();
		}

		protected virtual Row DoAllocateDefaultRow()
		{
			return null;
		}

		protected virtual void OnActivateHeader(ulong position)
		{
			Header.Activate(position);
		}

		protected virtual void OnCreateHeader(ulong position)
		{
			Header.Version = 1U;
			Header.Build(position);
		}

		protected virtual void OnDeactivateHeader()
		{
		}

		protected virtual void OnAllocateColumns()
		{
		}

		protected virtual void OnDeallocateColumns()
		{
		}

		protected virtual void OnAllocateRows()
		{
			defaultRow = DoAllocateDefaultRow();
			currentRow = DoAllocateCurrentRow();
			satelliteRow = DoAllocateSatteliteRow();
			topRow = DoAllocateTopRow();
			bottomRow = DoAllocateBottomRow();
		}

		protected virtual void OnDeallocateRows()
		{
			defaultRow = null;
			currentRow = null;
			satelliteRow = null;
			topRow = null;
			bottomRow = null;
		}

		protected virtual void OnCreateDefaultRow()
		{
		}

		protected virtual void OnActivateDefaultRow()
		{
		}

		protected virtual void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
		{
			if (wrapperDatabase != null)
			{
				wrapperDatabase.Handle.CheckCompatibility(openMode, false);
				storageHandle = wrapperDatabase.Handle;
				lockTimeOut = wrapperDatabase.lockTimeOut;
			}
			else
			{
				storageHandle = connection.StorageManager.OpenStorage(name, openMode, PageSize, false);
				lockTimeOut = connection.LockTimeout * 1000;
				connection.AddNotification(this);
			}
			InitializeAccessFlags(openMode);
			if (WrapperDatabase == null)
				Name = Handle.Name;
			updateBufferStatus = true;
			AttachLockStorage(headerPosition);
			ActivateHeader(headerPosition);
			AllocateDataStructure();
			ActivateDefaultRow();
		}

		protected virtual void OnCreateStorage(StorageHandle.StorageMode accessMode, ulong headerPosition)
		{
			if (wrapperDatabase != null && accessMode.Temporary && !wrapperDatabase.IsTemporary)
				wrapperDatabase = null;
			if (wrapperDatabase != null)
			{
				wrapperDatabase.Handle.CheckCompatibility(accessMode, false);
				storageHandle = wrapperDatabase.Handle;
				lockTimeOut = wrapperDatabase.lockTimeOut;
			}
			else
			{
				if (accessMode.Temporary)
				{
					string str;
					try
					{
						str = Path.GetTempFileName();
						File.Delete(str);
					}
					catch (SecurityException ex)
					{
						str = Path.Combine(ParentConnection.TemporaryPath, "VistaDB." + Guid.NewGuid().ToString() + ".tmp");
					}
					storageHandle = connection.StorageManager.CreateTemporaryStorage(str, PageSize, accessMode.Transacted, accessMode.IsolatedStorage);
				}
				else
					storageHandle = connection.StorageManager.OpenStorage(name, accessMode, PageSize, false);
				lockTimeOut = connection.LockTimeout * 1000;
				connection.AddNotification(this);
			}
			InitializeAccessFlags(accessMode);
			if (WrapperDatabase == null)
				Name = Handle.Name;
			updateBufferStatus = false;
			AttachLockStorage(headerPosition);
			CreateHeader(headerPosition);
			AllocateDataStructure();
			CreateDefaultRow();
		}

		protected virtual void OnCloseStorage()
		{
			try
			{
				DetachLockStorage();
				DeactivateHeader();
				DeallocateDataStructure();
			}
			catch (Exception ex)
			{
				throw;
			}
			try
			{
				if (WrapperDatabase != null || Handle == null || (connection == null || connection.StorageManager == null))
					return;
				connection.StorageManager.CloseStorage(Handle);
			}
			finally
			{
				storageHandle = null;
			}
		}

		protected virtual void OnDeclareNewStorage(object hint)
		{
		}

		protected virtual ulong OnGetFreeCluster(int pageCount)
		{
			return storageHandle.GetFreeCluster(pageCount, PageSize);
		}

		protected virtual void OnSetFreeCluster(ulong clusterId, int pageCount)
		{
			storageHandle.PendingFreeCluster(StorageId, clusterId, pageCount);
		}

		protected virtual StorageHandle OnAttachLockStorage(ulong headerPosition)
		{
			return storageHandle;
		}

		protected virtual void OnDetachLockStorage(bool external)
		{
			if (!external || connection == null || connection.StorageManager == null)
				return;
			connection.StorageManager.CloseStorage(lockStorageHandle);
		}

		protected virtual bool OnImport(DataStorage sourceStorage, bool interruptOnError)
		{
			try
			{
				uint rowCount = sourceStorage.RowCount;
				if (rowCount == 0U)
					return true;
				uint num = 0;
				Top();
				Synch();
				sourceStorage.Top();
				sourceStorage.Synch();
				while (!sourceStorage.EndOfSet)
				{
					if ((!ImportRow(sourceStorage.CurrentRow, SatelliteRow) || !CreateRow(true, false)) && interruptOnError)
						return false;
					sourceStorage.NextRow();
					CallOperationStatusDelegate(++num * 100U / rowCount, VistaDBOperationStatusTypes.DataImportOperation);
				}
				Synch();
				return true;
			}
			finally
			{
				CallOperationStatusDelegate(uint.MaxValue, VistaDBOperationStatusTypes.DataImportOperation);
			}
		}

		protected virtual bool OnInitializeImport(DataStorage sourceStorage, bool interruptOnError)
		{
			return false;
		}

		protected virtual bool OnFinalizeImport(DataStorage sourceStorage, bool interruptOnError)
		{
			return true;
		}

		protected virtual bool OnExport(DataStorage destinationStorage, bool interruptOnError)
		{
			try
			{
				uint rowCount = RowCount;
				string message = string.Format("({0:N0} row{1})", rowCount, rowCount == 1U ? string.Empty : "s");
				destinationStorage.CallOperationStatusDelegate(0U, VistaDBOperationStatusTypes.DataExportOperation, Name, message);
				if (rowCount == 0U)
					return true;
				uint num = 0;
				destinationStorage.Top();
				destinationStorage.Synch();
				Top();
				Synch();
				while (!EndOfSet)
				{
					try
					{
						if (ExportRow(CurrentRow, destinationStorage.SatelliteRow))
						{
							if (destinationStorage.CreateRow(true, false))
								goto label_9;
						}
						if (interruptOnError)
							return false;
					}
					catch (Exception ex)
					{
						if (interruptOnError)
							throw ex;
					}
				label_9:
					NextRow();
					destinationStorage.CallOperationStatusDelegate(++num * 100U / rowCount, VistaDBOperationStatusTypes.DataExportOperation);
				}
				destinationStorage.Synch();
				return true;
			}
			finally
			{
				destinationStorage.CallOperationStatusDelegate(uint.MaxValue, VistaDBOperationStatusTypes.DataExportOperation);
			}
		}

		protected virtual bool OnImportRow(Row sourceRow, Row destinationRow)
		{
			destinationRow.Copy(sourceRow);
			return true;
		}

		protected virtual void OnRereadExtendedColumn(ExtendedColumn column, Row rowKey)
		{
		}

		protected virtual bool OnExportRow(Row sourceRow, Row destinationRow)
		{
			destinationRow.Copy(sourceRow);
			return true;
		}

		protected virtual void OnZapStorage(bool commit)
		{
			if (IsMultiWriteSynchronization || IsReadOnly)
				throw new VistaDBException(337, Name);
		}

		protected virtual void OnCleanUpDiskSpace(bool commit)
		{
		}

		protected virtual IValue OnGetColumnValue(int rowIndex, VistaDBType crossType, Row sourceRow)
		{
			Row.Column column = sourceRow[rowIndex];
			if (column.Type == crossType)
				return column.Duplicate(true);
			Row.Column emptyColumnInstance = CreateEmptyColumnInstance(crossType, 8192, NCharColumn.DefaultUnicode, !CaseSensitive, false);
			Conversion.Convert(column, emptyColumnInstance);
			return emptyColumnInstance;
		}

		protected virtual void OnPutColumnValue(int rowIndex, VistaDBType crossType, Row destinationRow, IValue columnValue)
		{
			IValue dstValue = destinationRow[rowIndex];
			Conversion.Convert(columnValue, dstValue);
		}

		protected virtual void OnLowLevelLockRow(uint rowId)
		{
			lockStorageHandle.Lock(rowId, 1, StorageId);
		}

		protected virtual void OnLowLevelUnlockRow(uint rowId)
		{
			lockStorageHandle.Unlock(rowId, 1, StorageId);
		}

		protected virtual void OnLowLevelLockStorage(ulong offset, int bytes)
		{
			if (lockStorageHandle == null)
				return;
			lockStorageHandle.Lock(offset, bytes, StorageId);
		}

		protected virtual void OnLowLevelUnlockStorage(ulong offset, int bytes)
		{
			if (lockStorageHandle == null)
				return;
			lockStorageHandle.Unlock(offset, bytes, StorageId);
		}

		protected virtual void OnUpdateCurrentRow()
		{
		}

		protected virtual bool OnFlushCurrentRow()
		{
			return false;
		}

		protected virtual void OnTop()
		{
			SetCurrentRow(TopRow);
			asynchCounter = 1;
		}

		protected virtual void OnBottom()
		{
			SetCurrentRow(BottomRow);
			asynchCounter = -1;
		}

		protected virtual void OnSynch(int asynchCounter)
		{
			GoCurrentRow(false);
			for (; !EndOfSet && asynchCounter > 0; --asynchCounter)
				NextRow();
			for (; !BgnOfSet && asynchCounter < 0; ++asynchCounter)
				PrevRow();
		}

		protected virtual void OnGoCurrentRow(bool soft)
		{
			UpdateCurrentRow();
		}

		protected virtual void OnNextRow()
		{
		}

		protected virtual void OnPrevRow()
		{
		}

		protected virtual Row OnCompileRow(string keyEvaluationExpression, bool initTop)
		{
			return null;
		}

		protected virtual bool OnSeekRow(Row row, bool partialMatching)
		{
			SetCurrentRow(row);
			DataStorage.Operation currentOperation = this.currentOperation;
			this.currentOperation = DataStorage.Operation.Seek;
			try
			{
				Synch();
			}
			finally
			{
				this.currentOperation = currentOperation;
			}
			if (!PassOptimizedFilters() || !PassOrdinaryFilters() || !ActivateLinks())
				NextRow();
			asynchCounter = 0;
			return !EndOfSet;
		}

		protected virtual void OnSetCurrentRow(Row row)
		{
			CurrentRow.RowId = row.RowId;
			CurrentRow.RowVersion = row.RowVersion;
			updateBufferStatus = true;
			asynchCounter = 0;
		}

		protected virtual void OnSetSatelliteRow(Row row)
		{
			SatelliteRow.RowId = row.RowId;
		}

		protected virtual bool OnIsClearScope()
		{
			if ((int)TopRow.RowId == (int)Row.MinRowId)
				return (int)BottomRow.RowId == (int)Row.MaxRowId;
			return false;
		}

		protected virtual bool OnAssumeLink(Relationships.Relation link, bool toModify)
		{
			return true;
		}

		protected virtual bool OnResumeLink(Relationships.Relation link, bool toModify)
		{
			return true;
		}

		protected virtual bool DoBeforeUpdateRow(uint rowId)
		{
			return true;
		}

		protected virtual bool OnUpdateRow(Row oldRow, Row newRow)
		{
			if (!forcedBeforeUpdate)
				PassBeforeUpdateFilters();
			return true;
		}

		protected virtual bool DoAfterUpdateRow(uint rowId, bool updated)
		{
			return updated;
		}

		protected virtual bool DoBeforeCreateRow()
		{
			return true;
		}

		protected virtual bool OnCreateRow(bool blank, Row newRow)
		{
			if (!forcedBeforeAppend)
				PassBeforeCreateFilters();
			return true;
		}

		internal void UpdateCommitedState(bool commit, int exraCount)
		{
			LockStorage();
			bool flag = false;
			try
			{
				header.RowCount += (uint)exraCount;
				header.Modified = true;
				FlushStorageVersion();
				flag = true;
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal void UpdateRollbackedState(bool commit)
		{
			LockStorage();
			bool flag = false;
			try
			{
				header.Modified = true;
				FlushStorageVersion();
				flag = true;
			}
			finally
			{
				UnlockStorage(false);
				FinalizeChanges(!flag, commit);
			}
		}

		internal virtual void DoIncreaseRowCount()
		{
			++header.RowCount;
		}

		internal virtual void DoDecreaseRowCount()
		{
			--header.RowCount;
		}

		internal virtual void DoUpdateRowCount()
		{
		}

		protected virtual bool DoAfterCreateRow(bool created)
		{
			return created;
		}

		protected virtual bool DoBeforeDeleteRow(uint rowId)
		{
			return true;
		}

		protected virtual bool OnDeleteRow(Row currentRow)
		{
			return true;
		}

		protected virtual bool DoAfterDeleteRow(uint rowId, bool deleted)
		{
			return deleted;
		}

		protected virtual void OnUpdateStorageVersion(ref bool newVersion)
		{
			newVersion = IsMultiWriteSynchronization && IsNewVersion;
		}

		protected virtual void OnFlushStorageVersion()
		{
			if (IsMultiWriteSynchronization)
				++Version;
			Header.Flush();
		}

		protected virtual void OnFlushDefaultRow()
		{
		}

		protected virtual TranslationList OnCreateTranslationsList(DataStorage destinationStorage)
		{
			TranslationList translationList = new TranslationList();
			if (destinationStorage == null)
				return translationList;
			foreach (Row.Column dstColumn in destinationStorage.SatelliteRow)
			{
				foreach (Row.Column srcColumn in CurrentRow)
				{
					if (Database.DatabaseObject.EqualNames(srcColumn.Name, dstColumn.Name))
					{
						translationList.AddTranslationRule(srcColumn, dstColumn);
						break;
					}
				}
			}
			return translationList;
		}

		protected virtual void OnFreeTranslationsList()
		{
			translationList = null;
		}

		protected virtual bool OnPassFilter(Filter filter, Row activeRow)
		{
			if (filter.Active)
				return filter.GetValidRowStatus(activeRow);
			return true;
		}

		protected virtual bool DoBeforeActivateLinkFrom(DataStorage storage, Relationships.Relation link)
		{
			return DoBeforeActivateRow();
		}

		protected virtual bool DoActivateLinkFrom(DataStorage externalStorage, Relationships.Relation link, Row linkingRow)
		{
			uint rowId = linkingRow.RowId;
			SetCurrentRow(linkingRow);
			if (!PassOptimizedFilters())
				return false;
			GoCurrentRow(false);
			FreezeRelationships();
			try
			{
				ActivateLinks();
			}
			finally
			{
				DefreezeRelationships();
			}
			if ((int)rowId == (int)CurrentRow.RowId)
				return PassOrdinaryFilters();
			return false;
		}

		protected virtual bool DoAfterActivateLinkFrom(DataStorage storage, Relationships.Relation link, bool activated)
		{
			return DoAfterActivateRow(activated);
		}

		protected virtual bool DoBeforeActivateRow()
		{
			return true;
		}

		protected virtual bool DoAfterActivateRow(bool activated)
		{
			return activated;
		}

		protected virtual bool DoBeforeUpdateLinkFrom(DataStorage storage)
		{
			return DoBeforeUpdateRow(Row.MaxRowId);
		}

		protected virtual bool DoUpdateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row oldRow, Row newRow)
		{
			return false;
		}

		protected virtual bool DoAfterUpdateLinkFrom(DataStorage storage, bool updated)
		{
			return DoAfterUpdateRow(Row.MaxRowId, updated);
		}

		protected virtual bool DoBeforeCreateLinkFrom(DataStorage storage)
		{
			return DoBeforeCreateRow();
		}

		protected virtual bool DoCreateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row newRow)
		{
			return false;
		}

		protected virtual bool DoAfterCreateLinkFrom(DataStorage storage, bool passed)
		{
			return DoAfterCreateRow(passed);
		}

		protected virtual bool DoBeforeDeleteLinkFrom(DataStorage storage)
		{
			return DoBeforeDeleteRow(Row.MaxRowId);
		}

		protected virtual bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
		{
			return false;
		}

		protected virtual bool DoAfterDeleteLinkFrom(DataStorage storage, bool deleted)
		{
			return DoAfterDeleteRow(Row.MaxRowId, deleted);
		}

		protected virtual Row DoEvaluateLink(DataStorage masterStorage, EvalStack linking, Row sourceRow, Row targetRow)
		{
			try
			{
				if (linking == null)
					targetRow.CopyMetaData(sourceRow);
				else
					linking.Exec(sourceRow, targetRow);
				return targetRow;
			}
			catch (Exception ex)
			{
				targetRow = CreateEmptyRowInstance();
				targetRow.InitBottom();
				throw ex;
			}
		}

		protected virtual void OnClearScope(DataStorage.ScopeType scope)
		{
			TopRow.InitTop();
			BottomRow.InitBottom();
		}

		protected virtual bool OnSetScope(Row lowValue, Row highValue, DataStorage.ScopeType scopes, bool exactMatching)
		{
			TopRow.Copy(lowValue);
			BottomRow.Copy(highValue);
			--TopRow.RowId;
			++BottomRow.RowId;
			return true;
		}

		protected virtual void OnGetScope(out IVistaDBRow lowValue, out IVistaDBRow highValue)
		{
			lowValue = TopRow.CopyInstance();
			highValue = BottomRow.CopyInstance();
		}

		protected virtual void OnActivateEnvironment()
		{
		}

		protected virtual void OnFinalizeChanges(bool rollback, bool commit)
		{
			FinalizeChanges(rollback, commit, true);
		}

		protected virtual void OnCommitStorageVersion()
		{
			Handle.FlushCache();
			createdInMemoryYet = false;
			try
			{
				for (int index = 0; index < relationships.Count; ++index)
				{
					Relationships.Relation relationship = relationships[index];
					if (relationship.Active && relationship.SlaveStorage != this)
						relationship.SlaveStorage.CommitLinkFrom(relationship.MasterStorage);
				}
			}
			finally
			{
				Handle.ClearWholeCacheButHeader(StorageId);
				Header.KeepSchemaVersion();
			}
		}

		protected virtual void OnRollbackStorageVersion()
		{
			try
			{
				for (int index = 0; index < relationships.Count; ++index)
				{
					Relationships.Relation relationship = relationships[index];
					if (relationship.Active && relationship.SlaveStorage != this)
						relationship.SlaveStorage.RollBackLinkFrom(relationship.MasterStorage);
				}
			}
			finally
			{
				updateBufferStatus = true;
				Handle.ClearWholeCache(StorageId, true);
				Handle.ResetCachedLength();
				Header.ResetVersionInfo();
			}
		}

		protected virtual bool OnMinimizeMemoryCache(bool forceClearing)
		{
			return false;
		}

		protected virtual bool OnNotifyChangedEnvironment(Connection.Settings variable, object newValue)
		{
			return true;
		}

		protected virtual void OnReleaseLocks()
		{
			for (int index = 0; index < relationships.Count; ++index)
			{
				Relationships.Relation relationship = relationships[index];
				if (relationship.Active && relationship.SlaveStorage != this)
					relationship.SlaveStorage.ReleaseLinkingLock(relationship.MasterStorage);
			}
			lockManager.SynchAll();
		}

		private void ActivateHeader(ulong position)
		{
			Header.Position = position;
			LockStorage();
			bool flag = false;
			try
			{
				OnActivateHeader(position);
				flag = true;
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 103, Name);
			}
			finally
			{
				UnlockStorage(!flag);
			}
		}

		private void CreateHeader(ulong position)
		{
			try
			{
				OnCreateHeader(position);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 102, Name);
			}
		}

		private void DeactivateHeader()
		{
			OnDeactivateHeader();
		}

		private void AllocateDataStructure()
		{
			AllocateRows();
		}

		private void DeallocateDataStructure()
		{
			DeallocateRows();
		}

		private void AllocateRows()
		{
			try
			{
				OnAllocateRows();
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 107, Name);
			}
		}

		private void DeallocateRows()
		{
			try
			{
				OnDeallocateRows();
			}
			catch
			{
			}
		}

		private void CreateDefaultRow()
		{
			try
			{
				OnCreateDefaultRow();
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 137, Name);
			}
		}

		protected void ActivateDefaultRow()
		{
			try
			{
				OnActivateDefaultRow();
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 138, Name);
			}
		}

		private void InitializeAccessFlags(StorageHandle.StorageMode mode)
		{
			shared = mode.Share != FileShare.None;
			shareReadOnly = mode.Share == FileShare.Read;
			readOnly = mode.Access == FileAccess.Read;
		}

		private void AttachLockStorage(ulong headerPosition)
		{
			try
			{
				lockStorageHandle = OnAttachLockStorage(headerPosition);
			}
			catch (Exception ex)
			{
				lockStorageHandle = null;
				throw new VistaDBException(ex, 160, Name);
			}
		}

		private void DetachLockStorage()
		{
			try
			{
				OnDetachLockStorage(lockStorageHandle != null && storageHandle != lockStorageHandle);
			}
			catch (Exception ex)
			{
				throw;
			}
			finally
			{
				lockStorageHandle = null;
			}
		}

		private bool InitializeImport(DataStorage sourceStorage, bool interruptOnError)
		{
			try
			{
				return OnInitializeImport(sourceStorage, interruptOnError);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 332, sourceStorage.Name);
			}
		}

		private bool FinalizeImport(DataStorage sourceStorage, bool interruptOnError)
		{
			try
			{
				return OnFinalizeImport(sourceStorage, interruptOnError);
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 332, sourceStorage.Name);
			}
		}

		internal bool ExportRow(Row sourceRow, Row destinationRow)
		{
			return OnExportRow(sourceRow, destinationRow);
		}

		private void UpdateCurrentRow()
		{
			if (!updateBufferStatus)
				return;
			try
			{
				OnUpdateCurrentRow();
			}
			catch (Exception ex)
			{
				ClearCurrentRow();
				BgnOfSet = true;
				EndOfSet = true;
				throw ex;
			}
			finally
			{
				updateBufferStatus = false;
			}
		}

		private bool FlushCurrentRow()
		{
			try
			{
				if (!OnFlushCurrentRow())
					return false;
				bool valid = !IsDeleteOperation;
				if (valid)
					RestoreRow();
				for (int index = 0; index < optimizedFilters.Count; ++index)
					optimizedFilters[index].SetRowStatus(CurrentRow, valid);
				return true;
			}
			finally
			{
				updateBufferStatus = false;
			}
		}

		private bool AssumeLinks(bool toModify)
		{
			for (int index = 0; index < relationships.Count; ++index)
			{
				if (!relationships[index].Assume(toModify))
					return false;
			}
			return true;
		}

		private bool ResumeLinks(bool toModify, bool assumed)
		{
			bool flag = assumed;
			for (int index = 0; index < relationships.Count; ++index)
				flag = relationships[index].Resume(toModify) && flag;
			return flag;
		}

		private bool PassFilter(Filter filter, Row activeRow)
		{
			try
			{
				if (filter == null || OnPassFilter(filter, activeRow))
					return true;
				switch (filter.TypeId)
				{
					case Filter.FilterType.Ordinary:
					case Filter.FilterType.Optimized:
						return false;
					default:
						throw new VistaDBException(170);
				}
			}
			catch (Exception ex)
			{
				switch (filter.TypeId)
				{
					case Filter.FilterType.DefaultValueInsertGenerator:
					case Filter.FilterType.DefaultValueUpdateGenerator:
						throw new VistaDBException(ex, 171, filter.Expression);
					case Filter.FilterType.Identity:
					case Filter.FilterType.ReadOnly:
						throw new VistaDBException(ex, 173, filter.FirstColumn.Name);
					case Filter.FilterType.ConstraintAppend:
					case Filter.FilterType.ConstraintUpdate:
					case Filter.FilterType.ConstraintDelete:
						throw new VistaDBException(ex, 172, filter.Expression);
					default:
						throw;
				}
			}
		}

		internal bool PassOrdinaryFilters()
		{
			if (BgnOfSet || EndOfSet)
				return true;
			if (!PassTransaction(CurrentRow, TransactionId))
				return false;
			List<Filter> filterList = new List<Filter>(ordinaryFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			foreach (Filter filter in filterList)
			{
				if (filter.Active && !PassFilter(filter, CurrentRow))
					return false;
			}
			return true;
		}

		private bool PassOptimizedFilters()
		{
			if (optimizedFilters.Count == 0 || BgnOfSet || EndOfSet)
				return true;
			List<Filter> filterList = new List<Filter>(optimizedFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			foreach (Filter filter in filterList)
			{
				if (filter.Active && !PassFilter(filter, CurrentRow))
					return false;
			}
			return true;
		}

		private void PassBeforeCreateFilters()
		{
			List<Filter> filterList = new List<Filter>(beforeCreateFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			try
			{
				foreach (Filter filter in filterList)
				{
					if (filter.Active && !PassFilter(filter, SatelliteRow))
						throw new VistaDBException(340);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 340);
			}
		}

		private void PassAfterCreateFilters()
		{
			if (forcedAfterAppend)
				return;
			List<Filter> filterList = new List<Filter>(afterCreateFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			try
			{
				foreach (Filter filter in filterList)
				{
					if (filter.Active && !PassFilter(filter, SatelliteRow))
						throw new VistaDBException(341);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 341);
			}
		}

		private void PassBeforeUpdateFilters()
		{
			List<Filter> filterList = new List<Filter>(beforeUpdateFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			try
			{
				foreach (Filter filter in filterList)
				{
					if (filter.Active && !PassFilter(filter, SatelliteRow))
						throw new VistaDBException(342);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 342);
			}
		}

		private void PassAfterUpdateFilters()
		{
			if (forcedAfterUpdate)
				return;
			List<Filter> filterList = new List<Filter>(afterUpdateFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			try
			{
				foreach (Filter filter in filterList)
				{
					if (filter.Active && !PassFilter(filter, SatelliteRow))
						throw new VistaDBException(343);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 343);
			}
		}

		private void PassAfterDeleteFilters()
		{
			if (forcedAfterDelete)
				return;
			List<Filter> filterList = new List<Filter>(afterDeleteFilters);
			filterList.Sort(new FilterComparer());
			filterList.Reverse();
			try
			{
				foreach (Filter filter in filterList)
				{
					if (filter.Active && !PassFilter(filter, SatelliteRow))
						throw new VistaDBException(345);
				}
			}
			catch (Exception ex)
			{
				throw new VistaDBException(ex, 345);
			}
		}

		private void ActivateEnvironment()
		{
			OnActivateEnvironment();
		}

		private void DetachFilterByColumnAndType(FiltersList filters, Row.Column column, Filter.FilterType type)
		{
			List<Filter> filterList = new List<Filter>();
			for (int index = 0; index < filters.Count; ++index)
			{
				Filter filter = filters[index];
				if (filter.TypeId == type && Database.DatabaseObject.EqualNames(filter.FirstColumn.Name, column.Name))
					filterList.Add(filter);
			}
			for (int index = 0; index < filterList.Count; ++index)
			{
				Filter filter = filterList[index];
				filter.Deactivate();
				filters.Remove(filter);
			}
		}

		private void DetachConstraintByName(FiltersList filters, string name, Filter.FilterType type)
		{
			List<Filter> filterList = new List<Filter>();
			for (int index = 0; index < filters.Count; ++index)
			{
				Constraint filter = (Constraint)filters[index];
				if (filter.TypeId == type && Database.DatabaseObject.EqualNames(filter.Name, name))
					filterList.Add(filter);
			}
			for (int index = 0; index < filterList.Count; ++index)
			{
				Filter filter = filterList[index];
				filter.Deactivate();
				filters.Remove(filter);
			}
		}

		private void FinalizeChanges(bool rollback, bool commit, bool releaseLocks)
		{
			if (!rollback)
			{
				if (!commit)
					return;
			}
			try
			{
				try
				{
					if (rollback || !commit)
						return;
					CommitStorageVersion();
				}
				catch (Exception ex)
				{
					rollback = true;
					throw ex;
				}
				finally
				{
					if (rollback)
						RollbackStorageVersion();
				}
			}
			finally
			{
				if (releaseLocks)
					ReleaseLocks();
			}
		}

		public void Dispose()
		{
			if (isDisposed)
				return;
			Destroy();
			isDisposed = true;
			GC.SuppressFinalize(this);
		}

		~DataStorage()
		{
			if (isDisposed)
				return;
			Destroy();
			isDisposed = true;
		}

		protected virtual void Destroy()
		{
			try
			{
				if (storageHandle != null)
					CloseStorage();
				storageHandle = null;
				if (connection != null)
					connection.RemoveNotification(this);
				connection = null;
				lockStorageHandle = null;
				if (lockManager != null)
					lockManager.Dispose();
				lockManager = null;
				name = null;
				alias = null;
				header = null;
				currentRow = null;
				satelliteRow = null;
				topRow = null;
				bottomRow = null;
				defaultRow = null;
				translationList = null;
				wrapperDatabase = null;
				connection = null;
				ordinaryFilters = null;
				optimizedFilters = null;
				beforeCreateFilters = null;
				afterCreateFilters = null;
				beforeUpdateFilters = null;
				afterUpdateFilters = null;
				afterDeleteFilters = null;
				relationships = null;
				encryption = null;
			}
			catch (Exception ex)
			{
				throw;
			}
		}

		internal virtual uint TransactionId
		{
			get
			{
				return WrapperDatabase.TransactionId;
			}
		}

		internal bool IsTransaction
		{
			get
			{
				return TransactionId != 0U;
			}
		}

		internal virtual IsolationLevel TpIsolationLevel
		{
			get
			{
				return WrapperDatabase.TpIsolationLevel;
			}
			set
			{
			}
		}

		internal bool PassTransaction(Row row, uint transactionToTest)
		{
			uint transactionId = row.TransactionId;
			bool outdatedStatus = row.OutdatedStatus;
			bool flag = (int)transactionId != (int)transactionToTest;
			IsolationLevel tpIsolationLevel = TpIsolationLevel;
			if (!outdatedStatus)
			{
				if (!flag)
					return true;
				if (DoGettingAnotherTransactionStatus(transactionId) != TpStatus.Commit)
					return false;
				if (tpIsolationLevel != IsolationLevel.ReadCommitted)
					return transactionId > transactionToTest;
				return true;
			}
			if (!flag)
				return false;
			if (DoGettingAnotherTransactionStatus(transactionId) != TpStatus.Commit)
				return true;
			if (tpIsolationLevel == TransactionLogRowset.SnapshotIsolationLevel)
				return transactionId <= transactionToTest;
			return false;
		}

		internal virtual TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
		{
			return TpStatus.Commit;
		}

		internal void SwapHeader(Database.DatabaseHeader databaseHeader)
		{
			header = databaseHeader;
		}

		internal class StorageContext
		{
			private Row current;
			private Row satellite;
			private bool eof;
			private bool bof;
			private int asynchCounter;

			internal StorageContext()
			{
			}

			internal void Init(Row current, Row satellite, bool eof, bool bof, int asynch)
			{
				this.current = current;
				this.satellite = satellite;
				this.eof = eof;
				this.bof = bof;
				asynchCounter = asynch;
			}

			internal Row Current
			{
				get
				{
					return current;
				}
			}

			internal Row Satellite
			{
				get
				{
					return satellite;
				}
			}

			internal bool Bof
			{
				get
				{
					return bof;
				}
			}

			internal bool Eof
			{
				get
				{
					return eof;
				}
			}

			internal int Asynch
			{
				get
				{
					return asynchCounter;
				}
			}
		}

		internal struct OperationCallbackStatus : IVistaDBOperationCallbackStatus
		{
			private int progress;
			private VistaDBOperationStatusTypes operation;
			private string operationMessage;
			private string objectName;

			internal OperationCallbackStatus(int progress, VistaDBOperationStatusTypes operation, string objectName, string message)
			{
				this.progress = progress;
				this.operation = operation;
				this.objectName = objectName;
				operationMessage = message;
			}

			internal OperationCallbackStatus(int progress, VistaDBOperationStatusTypes operation, string objectName)
			{
				this.progress = progress;
				this.operation = operation;
				this.objectName = objectName;
				operationMessage = null;
			}

			int IVistaDBOperationCallbackStatus.Progress
			{
				get
				{
					return progress;
				}
			}

			VistaDBOperationStatusTypes IVistaDBOperationCallbackStatus.Operation
			{
				get
				{
					return operation;
				}
			}

			string IVistaDBOperationCallbackStatus.ObjectName
			{
				get
				{
					return objectName;
				}
			}

			string IVistaDBOperationCallbackStatus.Message
			{
				get
				{
					return operationMessage;
				}
			}
		}

		internal enum FilteredStatus
		{
			NoFilters,
			FullOptimization,
			UsualFilters,
			IndexFilters,
		}

		internal enum ScopeType
		{
			None,
			QueryScope,
			UserScope,
		}

		internal enum Operation
		{
			Insert,
			Update,
			Delete,
			Synch,
			Seek,
			None,
		}
	}
}
