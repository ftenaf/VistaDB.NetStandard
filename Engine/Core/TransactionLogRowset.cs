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
          : base(null, alias, parentDatabase, parentDatabase.ParentConnection, parentDatabase.Parser, parentDatabase.Encryption, null, Table.TableType.Default)
        {
            rowsetName = alias + name;
        }

        internal new TransactionLogRowsetHeader Header
        {
            get
            {
                return (TransactionLogRowsetHeader)base.Header;
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
            return TransactionLogRowsetHeader.CreateInstance(this, pageSize, culture);
        }

        internal override TransactionLogRowset DoCreateTpLog(bool commit)
        {
            return null;
        }

        internal override TransactionLogRowset DoOpenTpLog(ulong logHeaderPostion)
        {
            return null;
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
            FillRowData(CurrentRow, transactionId, TpStatus.Commit, 0);
            CurrentRow.RowId = Row.MinRowId;
            CurrentRow.RowVersion = Row.MinVersion;
            MoveToRow(CurrentRow);
            if ((int)CurrentRow[Header.TpIdIndex].Value != (int)transactionId)
                return TpStatus.Commit;
            return (TpStatus)(byte)CurrentRow[Header.StatusIndex].Value;
        }

        internal void RegisterTransaction(bool commit, uint transactionId)
        {
            if (GetTransactionStatus(transactionId) != TpStatus.Commit)
                return;
            RegisterTransactionStatus(commit, transactionId, TpStatus.Active);
        }

        private bool MoveToTpLogRow(uint transactionId)
        {
            FillRowData(SatelliteRow, transactionId, TpStatus.Commit, 0);
            SatelliteRow.RowId = Row.MinRowId;
            SatelliteRow.RowVersion = Row.MinVersion;
            MoveToRow(SatelliteRow);
            return (int)CurrentRow[Header.TpIdIndex].Value == (int)transactionId;
        }

        private void CreateEntry(bool commit, uint transactionId, TpStatus status)
        {
            PrepareEditStatus();
            FillRowData(SatelliteRow, transactionId, status, 0);
            CreateRow(commit, false);
        }

        private void UpdateEntry(bool commit, uint transactionId, TpStatus newStatus, int newCount)
        {
            if (!MoveToTpLogRow(transactionId))
                return;
            PrepareEditStatus();
            SaveRow();
            FillRowData(SatelliteRow, transactionId, newStatus, newCount);
            UpdateRow(commit);
        }

        private void DeleteEntry(bool commit, uint transactionId)
        {
            if (!MoveToTpLogRow(transactionId))
                return;
            DeleteRow(commit);
        }

        private void FillRowData(Row key, uint transactionId, TpStatus status, int exRowCount)
        {
            key[Header.TpIdIndex].Value = (int)transactionId;
            key[Header.StatusIndex].Value = (byte)status;
            key[Header.ExtraRowCounterIndex].Value = exRowCount;
        }

        private void RegisterTransactionStatus(bool commit, uint transactionId, TpStatus status)
        {
            CreateEntry(commit, transactionId, status);
        }

        internal void IncreaseRowCount(uint transactionId)
        {
            int extraRowCount = GetExtraRowCount(transactionId);
            UpdateEntry(false, transactionId, TpStatus.Active, extraRowCount + 1);
        }

        internal void DecreaseRowCount(uint transactionId)
        {
            int extraRowCount = GetExtraRowCount(transactionId);
            UpdateEntry(false, transactionId, TpStatus.Active, extraRowCount - 1);
        }

        internal int GetExtraRowCount(uint transactionId)
        {
            if (!MoveToTpLogRow(transactionId))
                return 0;
            return (int)CurrentRow[Header.ExtraRowCounterIndex].Value;
        }

        internal bool Commit(uint transactionId, ref int extraRows)
        {
            switch (GetTransactionStatus(transactionId))
            {
                case TpStatus.Commit:
                case TpStatus.Rollback:
                    extraRows = 0;
                    return false;
                default:
                    extraRows = GetExtraRowCount(transactionId);
                    DeleteEntry(false, transactionId);
                    return true;
            }
        }

        internal void Rollback(uint transactionId)
        {
            if (GetTransactionStatus(transactionId) != TpStatus.Active)
                return;
            int extraRowCount = GetExtraRowCount(transactionId);
            UpdateEntry(false, transactionId, TpStatus.Rollback, extraRowCount);
        }

        protected override bool DoMirrowModifications(Row oldRow, Row newRow, TriggerAction triggerAction)
        {
            return true;
        }

        internal class TransactionLogRowsetHeader : ClusteredRowsetHeader
        {
            internal int TpIdIndex;
            internal int StatusIndex;
            internal int ExtraRowCounterIndex;

            internal static new TransactionLogRowsetHeader CreateInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
            {
                return new TransactionLogRowsetHeader(parentStorage, HeaderId.TRANSACTION_LOG, Type.Clustered, pageSize, culture);
            }

            protected TransactionLogRowsetHeader(DataStorage parentStorage, HeaderId id, Type type, int pageSize, CultureInfo culture)
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
                TpIdIndex = rowInstance.AppendColumn(new IntColumn());
                StatusIndex = rowInstance.AppendColumn(new TinyIntColumn());
                ExtraRowCounterIndex = rowInstance.AppendColumn(new IntColumn());
                rowInstance.InstantiateComparingMask();
                rowInstance.ComparingMask[0] = TpIdIndex + 1;
                return rowInstance;
            }
        }
    }
}
