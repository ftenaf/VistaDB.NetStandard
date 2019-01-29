using System.Data;
using System.Globalization;
using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class TransactionLogRowset : ClusteredRowset
  {
    private string rowsetName;

    internal static TransactionLogRowset CreateInstance(Database parentDatabase, string rowsetName)
    {
      TransactionLogRowset transactionLogRowset = new TransactionLogRowset("__TransactionLog", rowsetName, parentDatabase);
      transactionLogRowset.DoAfterConstruction(parentDatabase.PageSize, parentDatabase.Culture);
      return transactionLogRowset;
    }

    private TransactionLogRowset(string alias, string name, Database parentDatabase)
      : base((string) null, alias, parentDatabase, parentDatabase.ParentConnection, parentDatabase.Parser, parentDatabase.Encryption, (ClusteredRowset) null, Table.TableType.Default)
    {
      this.rowsetName = alias + name;
    }

    internal TransactionLogRowset.TransactionLogRowsetHeader Header
    {
      get
      {
        return (TransactionLogRowset.TransactionLogRowsetHeader) base.Header;
      }
    }

    internal override uint TransactionId
    {
      get
      {
        return 0;
      }
    }

    internal override bool NoLocks
    {
      get
      {
        return true;
      }
    }

    internal override bool IsMultiWriteSynchronization
    {
      get
      {
        return false;
      }
    }

    internal override bool IsTransactionLogged
    {
      get
      {
        return true;
      }
    }

    protected override void OnDeclareNewStorage(object hint)
    {
    }

    protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
    {
      return (StorageHeader) TransactionLogRowset.TransactionLogRowsetHeader.CreateInstance((DataStorage) this, pageSize, culture);
    }

    internal override TransactionLogRowset DoCreateTpLog(bool commit)
    {
      return (TransactionLogRowset) null;
    }

    internal override TransactionLogRowset DoOpenTpLog(ulong logHeaderPostion)
    {
      return (TransactionLogRowset) null;
    }

    internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
    {
      return TpStatus.Commit;
    }

    internal static IsolationLevel SnapshotIsolationLevel
    {
      get
      {
        return IsolationLevel.Snapshot;
      }
    }

    internal TpStatus GetTransactionStatus(uint transactionId)
    {
      this.FillRowData(this.CurrentRow, transactionId, TpStatus.Commit, 0);
      this.CurrentRow.RowId = Row.MinRowId;
      this.CurrentRow.RowVersion = Row.MinVersion;
      this.MoveToRow(this.CurrentRow);
      if ((int) this.CurrentRow[this.Header.TpIdIndex].Value != (int) transactionId)
        return TpStatus.Commit;
      return (TpStatus) (byte) this.CurrentRow[this.Header.StatusIndex].Value;
    }

    internal void RegisterTransaction(bool commit, uint transactionId)
    {
      if (this.GetTransactionStatus(transactionId) != TpStatus.Commit)
        return;
      this.RegisterTransactionStatus(commit, transactionId, TpStatus.Active);
    }

    private bool MoveToTpLogRow(uint transactionId)
    {
      this.FillRowData(this.SatelliteRow, transactionId, TpStatus.Commit, 0);
      this.SatelliteRow.RowId = Row.MinRowId;
      this.SatelliteRow.RowVersion = Row.MinVersion;
      this.MoveToRow(this.SatelliteRow);
      return (int) this.CurrentRow[this.Header.TpIdIndex].Value == (int) transactionId;
    }

    private void CreateEntry(bool commit, uint transactionId, TpStatus status)
    {
      this.PrepareEditStatus();
      this.FillRowData(this.SatelliteRow, transactionId, status, 0);
      this.CreateRow(commit, false);
    }

    private void UpdateEntry(bool commit, uint transactionId, TpStatus newStatus, int newCount)
    {
      if (!this.MoveToTpLogRow(transactionId))
        return;
      this.PrepareEditStatus();
      this.SaveRow();
      this.FillRowData(this.SatelliteRow, transactionId, newStatus, newCount);
      this.UpdateRow(commit);
    }

    private void DeleteEntry(bool commit, uint transactionId)
    {
      if (!this.MoveToTpLogRow(transactionId))
        return;
      this.DeleteRow(commit);
    }

    private void FillRowData(Row key, uint transactionId, TpStatus status, int exRowCount)
    {
      key[this.Header.TpIdIndex].Value = (object) (int) transactionId;
      key[this.Header.StatusIndex].Value = (object) (byte) status;
      key[this.Header.ExtraRowCounterIndex].Value = (object) exRowCount;
    }

    private void RegisterTransactionStatus(bool commit, uint transactionId, TpStatus status)
    {
      this.CreateEntry(commit, transactionId, status);
    }

    private void UpdateTransactionStatus(bool commit, uint transactionId, TpStatus status, int exRowCount)
    {
      this.UpdateEntry(commit, transactionId, status, exRowCount);
    }

    private void DropTransactionStatus(bool commit, uint transactionId)
    {
      this.DeleteEntry(commit, transactionId);
    }

    internal void IncreaseRowCount(uint transactionId)
    {
      int extraRowCount = this.GetExtraRowCount(transactionId);
      this.UpdateEntry(false, transactionId, TpStatus.Active, extraRowCount + 1);
    }

    internal void DecreaseRowCount(uint transactionId)
    {
      int extraRowCount = this.GetExtraRowCount(transactionId);
      this.UpdateEntry(false, transactionId, TpStatus.Active, extraRowCount - 1);
    }

    internal int GetExtraRowCount(uint transactionId)
    {
      if (!this.MoveToTpLogRow(transactionId))
        return 0;
      return (int) this.CurrentRow[this.Header.ExtraRowCounterIndex].Value;
    }

    internal bool Commit(uint transactionId, ref int extraRows)
    {
      switch (this.GetTransactionStatus(transactionId))
      {
        case TpStatus.Commit:
        case TpStatus.Rollback:
          extraRows = 0;
          return false;
        default:
          extraRows = this.GetExtraRowCount(transactionId);
          this.DeleteEntry(false, transactionId);
          return true;
      }
    }

    internal void Rollback(uint transactionId)
    {
      if (this.GetTransactionStatus(transactionId) != TpStatus.Active)
        return;
      int extraRowCount = this.GetExtraRowCount(transactionId);
      this.UpdateEntry(false, transactionId, TpStatus.Rollback, extraRowCount);
    }

    protected override bool DoMirrowModifications(Row oldRow, Row newRow, TriggerAction triggerAction)
    {
      return true;
    }

    internal class TransactionLogRowsetHeader : ClusteredRowset.ClusteredRowsetHeader
    {
      internal int TpIdIndex;
      internal int StatusIndex;
      internal int ExtraRowCounterIndex;

      internal static TransactionLogRowset.TransactionLogRowsetHeader CreateInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
      {
        return new TransactionLogRowset.TransactionLogRowsetHeader(parentStorage, VistaDB.Engine.Core.Header.HeaderId.TRANSACTION_LOG, VistaDB.Engine.Core.Indexing.Index.Type.Clustered, pageSize, culture);
      }

      protected TransactionLogRowsetHeader(DataStorage parentStorage, VistaDB.Engine.Core.Header.HeaderId id, VistaDB.Engine.Core.Indexing.Index.Type type, int pageSize, CultureInfo culture)
        : base(parentStorage, id, type, pageSize, culture)
      {
      }

      protected override void OnRegisterSchema(IVistaDBTableSchema schema)
      {
      }

      protected override bool OnActivateSchema()
      {
        return base.OnActivateSchema();
      }

      protected override Row OnAllocateDefaultRow(Row rowInstance)
      {
        this.TpIdIndex = rowInstance.AppendColumn((IColumn) new IntColumn());
        this.StatusIndex = rowInstance.AppendColumn((IColumn) new TinyIntColumn());
        this.ExtraRowCounterIndex = rowInstance.AppendColumn((IColumn) new IntColumn());
        rowInstance.InstantiateComparingMask();
        rowInstance.ComparingMask[0] = this.TpIdIndex + 1;
        return rowInstance;
      }
    }
  }
}
