using VistaDB.Engine.Core.IO;

namespace VistaDB.Engine.Core
{
  internal class TemporaryRowset : ClusteredRowset
  {
    private Row parentTableSchema;

    internal static TemporaryRowset CreateInstance(string tableName, Database parentDatabase, Row parentTableSchema)
    {
      TemporaryRowset temporaryRowset = new TemporaryRowset(tableName, parentDatabase, parentTableSchema);
      temporaryRowset.DoAfterConstruction(parentDatabase.PageSize, parentDatabase.Culture);
      return temporaryRowset;
    }

    protected TemporaryRowset(string tableName, Database parentDatabase, Row parentTableSchema)
      : base(tableName, tableName, parentDatabase, parentDatabase.ParentConnection, parentDatabase.Parser, parentDatabase.Encryption, (ClusteredRowset) null, Table.TableType.Default)
    {
      this.parentTableSchema = parentTableSchema;
    }

    protected override void OnCreateStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnCreateStorage(openMode, ulong.MaxValue);
    }

    internal override TransactionLogRowset DoCreateTpLog(bool commit)
    {
      return (TransactionLogRowset) null;
    }

    protected override ulong OnGetFreeCluster(int pageCount)
    {
      return 0;
    }

    protected override void OnSetFreeCluster(ulong position, int pageCount)
    {
    }

    protected override void DoInitCreatedRow(Row newRow)
    {
    }

    protected override void DoCopyNonEdited(Row sourceRow, Row destinRow)
    {
    }

    protected override bool DoCheckNulls(Row row)
    {
      return true;
    }

    protected override bool DoAssignIdentity(Row newRow)
    {
      return true;
    }

    protected override bool DoAllocateExtensions(Row newRow, bool fresh)
    {
      return true;
    }

    protected override bool DoReallocateExtensions(Row oldRow, Row newRow)
    {
      return true;
    }

    protected override bool DoDeallocateExtensions(Row oldRow)
    {
      return true;
    }

    protected override void OnFlushStorageVersion()
    {
    }

    protected override void OnFinalizeChanges(bool rollback, bool commit)
    {
    }

    protected override bool OnMinimizeMemoryCache(bool forcedClearing)
    {
      return false;
    }

    protected override StorageHandle OnAttachLockStorage(ulong headerPosition)
    {
      return (StorageHandle) null;
    }

    protected override void OnCreateHeader(ulong position)
    {
    }

    protected override void OnCreateDefaultRow()
    {
    }

    protected override Row DoAllocateDefaultRow()
    {
      return parentTableSchema.CopyInstance();
    }

    protected override void OnCreateIndex()
    {
      base.OnCreateIndex();
    }

    protected override bool OnLockStorage()
    {
      return true;
    }

    protected override void OnUnlockStorage(bool doNotWaitForSynch)
    {
    }

    protected override void OnUnlockRow(uint rowId, bool userLock, bool instantly)
    {
    }

    protected override void OnLockRow(uint rowId, bool userLock, ref bool actualLock)
    {
      actualLock = true;
    }

    protected override void OnUpdateStorageVersion(ref bool newVersion)
    {
      newVersion = false;
    }

    protected override bool DoMirrowModifications(Row oldRow, Row newRow, TriggerAction triggerAction)
    {
      return true;
    }
  }
}
