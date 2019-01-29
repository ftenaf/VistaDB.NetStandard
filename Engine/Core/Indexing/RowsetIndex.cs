using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Indexing
{
  internal class RowsetIndex : Index
  {
    private const int MaxKeyColumnCount = 255;
    private ClusteredRowset rowSet;
    private EvalStack findEvaluator;
    protected SortSpool spool;
    private bool buildingStatus;
    private EvalStack keyPcode;
    private string keyExpression;

    internal static RowsetIndex CreateInstance(string fileName, string indexName, string keyExpression, bool fts, ClusteredRowset rowSet, DirectConnection connection, Database wrapperDatabase, CultureInfo culture, Encryption encryption)
    {
      RowsetIndex rowsetIndex = fts ? (RowsetIndex) new FTSIndex(fileName, indexName, keyExpression, rowSet, connection, wrapperDatabase, encryption, (RowsetIndex) null) : new RowsetIndex(fileName, indexName, keyExpression, rowSet, connection, wrapperDatabase, encryption, (RowsetIndex) null);
      rowsetIndex.DoAfterConstruction(rowSet.PageSize, culture == null ? rowSet.Culture : culture);
      return rowsetIndex;
    }

    protected RowsetIndex(string fileName, string indexName, string keyExpression, ClusteredRowset rowSet, DirectConnection connection, Database wrapperDatabase, Encryption encryption, RowsetIndex clonedOrigin)
      : base(fileName, indexName, (Parser) null, connection, wrapperDatabase, encryption, (Index) clonedOrigin)
    {
      this.rowSet = rowSet;
      this.keyExpression = keyExpression;
    }

    internal RowsetIndex.RowsetIndexHeader Header
    {
      get
      {
        return (RowsetIndex.RowsetIndexHeader) base.Header;
      }
    }

    internal string KeyExpression
    {
      get
      {
        return this.keyExpression;
      }
    }

    internal override bool BuildingStatus
    {
      get
      {
        return this.buildingStatus;
      }
    }

    internal override ClusteredRowset ParentRowset
    {
      get
      {
        return this.rowSet;
      }
    }

    protected override Row OnCreateEmptyRowInstance()
    {
      return Row.CreateInstance(0U, !this.Header.Descend, this.Encryption, (int[]) null);
    }

    protected override Row OnCreateEmptyRowInstance(int maxColCount)
    {
      return Row.CreateInstance(0U, !this.Header.Descend, this.Encryption, (int[]) null, maxColCount);
    }

    internal override CrossConversion Conversion
    {
      get
      {
        return this.ParentRowset.WrapperDatabase.Conversion;
      }
    }

    internal EvalStack KeyPCode
    {
      get
      {
        return this.keyPcode;
      }
    }

    private void CreatePCode()
    {
      try
      {
        this.OnCreatePCode();
      }
      catch (Exception ex)
      {
        this.keyPcode = (EvalStack) null;
        throw ex;
      }
    }

    private void InitSpool(uint keyCount, ref int expectedKeyLen)
    {
      try
      {
        if (this.spool != null)
          this.spool.Dispose();
        this.spool = keyCount == 0U ? (SortSpool) new DummySpool() : new SortSpool(this.Handle.IsolatedStorage, keyCount, ref expectedKeyLen, this.BottomRow, this.ParentConnection.StorageManager, false);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 305);
      }
    }

    private bool ContainsNulls(Row key)
    {
      foreach (Row.Column column in (List<Row.Column>) key)
      {
        if (column.IsNull)
          return true;
      }
      return false;
    }

    private void RegisterRowset()
    {
      try
      {
        this.SetRelationship((DataStorage) this, (DataStorage) this.ParentRowset, Relationships.Type.One_To_One, (EvalStack) null, false);
        this.ParentRowset.SetRelationship((DataStorage) this.ParentRowset, (DataStorage) this, Relationships.Type.One_To_One, this.KeyPCode, true);
      }
      finally
      {
        this.DefreezeRelationships();
        this.ParentRowset.DefreezeRelationships();
      }
    }

    private void UnregisterRowset()
    {
      this.ParentRowset.ResetRelationship((DataStorage) this.ParentRowset, (DataStorage) this);
      this.ResetRelationship((DataStorage) this, (DataStorage) this.ParentRowset);
    }

    internal bool IsCorrectPrimaryKeyExpr(string primaryKey)
    {
      return false;
    }

    internal void ReDeclareIndex()
    {
      this.DeclareNewStorage((object) this.CollectIndexInformation());
    }

    internal byte[] RowKeyStructure
    {
      get
      {
        List<Row.Column> columnList = this.KeyPCode.EnumColumns();
        byte[] numArray = new byte[columnList.Count * 2];
        for (int index1 = 0; index1 < columnList.Count; ++index1)
        {
          int index2 = index1 * 2;
          short rowIndex = (short) columnList[index1].RowIndex;
          numArray[index2] = (byte) rowIndex;
          short num = (short) ((int) (short) ((int) rowIndex & 768) >> 7);
          numArray[index2 + 1] = this.IsDescendKeyColumn(index1) ? (byte) (1U | (uint) (byte) num) : (byte) num;
        }
        return numArray;
      }
    }

    internal IVistaDBIndexInformation CollectIndexInformation()
    {
      return (IVistaDBIndexInformation) new Table.TableSchema.IndexCollection.IndexInformation(this.Name, this.Alias, this.KeyExpression, this.IsUnique, this.IsPrimary, this.Header.Descend, this.IsSparse, this.IsForeignKey, this.IsFts, false, this.StorageId, this.RowKeyStructure);
    }

    internal void EvaluateSpoolKey(bool forceOutput)
    {
      if (!this.buildingStatus)
        return;
      this.OnEvaluateSpoolKey(forceOutput);
    }

    internal bool StartBuild(uint maxKeyCount, ref int expectedKeyLen)
    {
      if (this.buildingStatus)
        this.OnStartBuild(maxKeyCount, ref expectedKeyLen);
      return this.buildingStatus;
    }

    internal void FinishBuild()
    {
      if (!this.buildingStatus)
        return;
      bool flag = false;
      bool isTemporary = this.IsTemporary;
      try
      {
        this.OnFinishBuild();
        this.FlushStorageVersion();
        flag = true;
      }
      finally
      {
        this.FinalizeChanges(!flag, isTemporary);
        if (flag)
          this.RegisterRowset();
      }
      this.MinimizeMemoryCache(false);
    }

    internal void RegisterInDatabase()
    {
      if (!this.buildingStatus || this.WrapperDatabase == null)
        return;
      this.WrapperDatabase.RegisterIndex(this);
    }

    internal void RegisterInDatabase(bool justResetBuildStatus)
    {
      if (!this.buildingStatus)
        return;
      this.buildingStatus = false;
      if (justResetBuildStatus || this.WrapperDatabase == null || this.WrapperDatabase == null)
        return;
      this.WrapperDatabase.RegisterIndex(this);
    }

    protected virtual void OnCreatePCode()
    {
      this.SetParser(this.ParentRowset.WrapperDatabase.SqlKeyParser);
      this.keyPcode = this.Parser.Compile(this.KeyExpression, (DataStorage) this.ParentRowset, false, false, this.CaseSensitive, (EvalStack) null);
      if (this.keyPcode == null)
        throw new VistaDBException(282, this.keyExpression);
      if (this.ParentRowset != null)
        this.ParentRowset.SatelliteRow.InitTop();
      this.keyPcode.Exec(this.ParentRowset.SatelliteRow, this.CreateEmptyRowInstance());
    }

    protected virtual bool IsDescendKeyColumn(int index)
    {
      return this.CurrentRow[index].Descending;
    }

    protected virtual void OnEvaluateSpoolKey(bool forceOutput)
    {
      Row currentRow = this.ParentRowset.CurrentRow;
      try
      {
        this.KeyPCode.Exec(currentRow, this.CreateEmptyRowInstance(this.BottomRow.Count));
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 307, new string(this.KeyPCode.Expression));
      }
      this.spool.PushKey(this.KeyPCode.EvaluatedRow, forceOutput);
    }

    internal virtual void OnStartBuild(uint maxKeyCount, ref int expectedKeyLen)
    {
      this.CloseStorage();
      StorageHandle.StorageMode accessMode = this.ParentRowset.Handle.Mode;
      bool commit = this.Header.Temporary && !this.WrapperDatabase.IsTemporary;
      if (commit)
      {
        accessMode = new StorageHandle.StorageMode(FileMode.CreateNew, false, false, accessMode.Access | FileAccess.Write, false, accessMode.IsolatedStorage);
        accessMode.Temporary = true;
      }
      ulong headerPosition = commit ? 0UL : this.WrapperDatabase.GetFreeCluster(1);
      this.CreateStorage(accessMode, headerPosition, commit);
      this.InitSpool(maxKeyCount, ref expectedKeyLen);
    }

    internal virtual void OnFinishBuild()
    {
      bool flag = !this.IsSparse;
      this.CurrentRow.RowId = 0U;
      using (this.spool)
      {
        this.spool.Sort();
        Row row = this.BottomRow;
        bool isUnique = this.IsUnique;
        bool isPrimary = this.IsPrimary;
        bool isFts = this.IsFts;
        int num = 0;
        int keyCount = this.spool.KeyCount;
        while (num < keyCount)
        {
          Row key = this.spool.PopKey();
          if (isUnique && num > 0 && key.EqualColumns(row, this.IsClustered))
          {
            if (flag)
              throw new VistaDBException(309, this.Alias + ": " + row.ToString());
          }
          else
          {
            if (isPrimary && this.ContainsNulls(key))
              throw new VistaDBException(142, this.Alias + ": " + key.ToString());
            if (!isFts || num <= 0 || key - row != 0)
              this.Tree.AppendLeafKey(key);
          }
          ++num;
          row = key;
        }
        this.Tree.FinalizeAppending();
      }
    }

    protected override Row DoAllocateCurrentRow()
    {
      return this.KeyPCode.EvaluatedRow.CopyInstance();
    }

    protected override Row DoAllocateCachedPkInstance()
    {
      return this.KeyPCode.EvaluatedRow.CopyInstance();
    }

    protected override void OnDeclareNewStorage(object hint)
    {
      if (hint == null)
        throw new VistaDBException(133, this.Alias);
      IVistaDBIndexInformation indexInformation = (IVistaDBIndexInformation) hint;
      Index.Type type = Index.Type.Standard;
      bool fullTextSearch = indexInformation.FullTextSearch;
      if (fullTextSearch)
        type |= Index.Type.Fts;
      if (indexInformation.Primary)
        type |= Index.Type.PrimaryKey;
      if (indexInformation.FKConstraint)
        type |= Index.Type.ForeignKey;
      if (indexInformation.Unique)
        type |= Index.Type.Unique;
      if (((Table.TableSchema.IndexCollection.IndexInformation) indexInformation).Sparse)
        type = type | Index.Type.Sparse | Index.Type.Unique;
      if (indexInformation.Temporary)
        type |= Index.Type.Temporary;
      if (this.ParentRowset.WrapperDatabase.CaseSensitive)
        type |= Index.Type.Sensitive;
      this.Header.Signature = (uint) type;
      this.Header.Descend = false;
      this.CreatePCode();
      List<Row.Column> columnList = this.KeyPCode.EnumColumns();
      if (columnList.Count > (int) byte.MaxValue)
        throw new VistaDBException(149, this.keyExpression);
      foreach (Row.Column column in columnList)
      {
        if (fullTextSearch && column.InternalType != VistaDBType.NChar || column.InternalType == VistaDBType.VarBinary)
          throw new VistaDBException(150, column.Name);
      }
      this.buildingStatus = true;
    }

    protected override StorageHandle OnAttachLockStorage(ulong headerPosition)
    {
      return this.Handle;
    }

    protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnOpenStorage(openMode, headerPosition);
      this.RegisterRowset();
    }

    protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
    {
      if (clonedStorage != null)
        return base.DoCreateHeaderInstance(pageSize, culture, clonedStorage);
      return (StorageHeader) RowsetIndex.RowsetIndexHeader.CreateHeaderInstance((DataStorage) this, this.ParentRowset.PageSize, this.ParentRowset.Culture);
    }

    protected override void OnActivateHeader(ulong position)
    {
      base.OnActivateHeader(position);
      this.CreatePCode();
    }

    protected override void OnCloseStorage()
    {
      this.UnregisterRowset();
      base.OnCloseStorage();
    }

    protected override bool DoAfterDeleteLinkFrom(DataStorage storage, bool deleted)
    {
      if (storage != this.ParentRowset)
        return base.DoAfterDeleteLinkFrom(storage, deleted);
      return true;
    }

    protected override void OnSynch(int asynchCounter)
    {
      bool flag = asynchCounter != 0 && this.ParentRowset.PostponedSynchronization;
      if (flag)
        this.FreezeRelationships();
      try
      {
        base.OnSynch(asynchCounter);
      }
      finally
      {
        if (flag)
          this.DefreezeRelationships();
      }
    }

    protected override void OnTop()
    {
      this.ParentRowset.EndOfSet = false;
      this.ParentRowset.BgnOfSet = false;
      base.OnTop();
    }

    protected override void OnBottom()
    {
      this.ParentRowset.EndOfSet = false;
      this.ParentRowset.BgnOfSet = false;
      base.OnBottom();
    }

    protected override bool OnSetScope(Row lowValue, Row highValue, DataStorage.ScopeType scopes, bool exactMatching)
    {
      return base.OnSetScope(lowValue, highValue, scopes, exactMatching);
    }

    protected override bool OnAssumeLink(Relationships.Relation link, bool toModify)
    {
      this.LockStorage();
      return true;
    }

    protected override bool OnResumeLink(Relationships.Relation link, bool toModify)
    {
      this.UnlockStorage(!toModify);
      return true;
    }

    protected override Row DoEvaluateLink(DataStorage masterStorage, EvalStack linking, Row sourceRow, Row targetRow)
    {
      return base.DoEvaluateLink(masterStorage, linking, sourceRow, targetRow);
    }

    protected override bool DoActivateLinkFrom(DataStorage externalStorage, Relationships.Relation link, Row linkingRow)
    {
      return base.DoActivateLinkFrom(externalStorage, link, linkingRow);
    }

    protected override bool DoUpdateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row oldKey, Row newKey)
    {
      this.SavePrimaryKey(oldKey);
      if ((int) oldKey.RowVersion == (int) newKey.RowVersion && oldKey.EqualColumns(newKey, false) && !newKey.EditedExtensions)
      {
        this.NoKeyUpdate = true;
        return true;
      }
      this.NoKeyUpdate = false;
      return base.DoUpdateLinkFrom(externalStorage, type, oldKey, newKey);
    }

    protected override bool DoCreateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row newRow)
    {
      this.SavePrimaryKey(newRow);
      return base.DoCreateLinkFrom(externalStorage, type, newRow);
    }

    protected override bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
    {
      this.SavePrimaryKey(row);
      return base.DoDeleteLinkFrom(externalStorage, type, row);
    }

    protected override Row OnCompileRow(string keyEvaluationExpression, bool initTop)
    {
      try
      {
        EvalStack evalStack = this.ParentRowset.WrapperDatabase.Parser.Compile(keyEvaluationExpression, (DataStorage) this.ParentRowset, false, false, this.CaseSensitive, this.findEvaluator);
        if (this.findEvaluator == null)
          this.findEvaluator = evalStack;
        if (evalStack != this.findEvaluator)
          return (Row) null;
        Row contextRow = this.ParentRowset.CurrentRow.CopyInstance();
        List<Row.Column> columnList = this.KeyPCode.EnumColumns();
        int index = 0;
        for (int count = columnList.Count; index < count; ++index)
        {
          Row.Column column = columnList[index];
          contextRow[column.RowIndex].Descending = this.CurrentRow[index].Descending;
        }
        if (initTop)
          contextRow.InitTop();
        else
          contextRow.InitBottom();
        evalStack.Exec(contextRow);
        this.KeyPCode.Exec(contextRow, this.CreateEmptyRowInstance());
        return this.KeyPCode.EvaluatedRow;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 307, keyEvaluationExpression);
      }
    }

    protected override bool OnSeekRow(Row row, bool partialMatching)
    {
      if (base.OnSeekRow(row, partialMatching))
        return true;
      if (partialMatching)
        return this.PartialKeyFound(row);
      return false;
    }

    protected override bool OnPartialKeyFound(Row patternRow)
    {
      foreach (Row.Column column in (List<Row.Column>) patternRow)
      {
        int rowIndex = column.RowIndex;
        Row.Column b = this.CurrentRow[rowIndex];
        long num = (long) column.MinusColumn(b);
        if (rowIndex == 0)
        {
          if (num != 0L)
          {
            if (b.InternalType == VistaDBType.NChar && !b.IsNull && !column.IsNull)
              return ((string) b.Value).StartsWith((string) column.Value, !this.CaseSensitive, this.Culture);
            return false;
          }
        }
        else if (num > 0L)
          return false;
      }
      return true;
    }

    protected override void OnCleanUpDiskSpace(bool commit)
    {
      if (this.WrapperDatabase == null)
        return;
      this.WrapperDatabase.LockStorage();
      try
      {
        this.Tree.CleanUpNodeSpace(this.Tree.RootNode, true);
        this.Tree.Modified = false;
      }
      finally
      {
        this.WrapperDatabase.UnlockStorage(false);
      }
    }

    internal override bool SuppressErrors
    {
      get
      {
        return this.ParentRowset.SuppressErrors;
      }
      set
      {
        this.ParentRowset.SuppressErrors = value;
      }
    }

    internal override bool DoCheckIfRelated(EvalStack fkEvaluator)
    {
      if (!this.IsPrimary)
        return false;
      List<Row.Column> columnList1 = this.KeyPCode.EnumColumns();
      List<Row.Column> columnList2 = fkEvaluator.EnumColumns();
      if (columnList1.Count != columnList2.Count)
        return false;
      int index = 0;
      for (int count = columnList1.Count; index < count; ++index)
      {
        if (columnList2[index].InternalType != columnList1[index].InternalType)
          return false;
      }
      return true;
    }

    internal override bool DoCheckIfSame(EvalStack fkEvaluator)
    {
      if (!this.IsPrimary)
        return false;
      List<Row.Column> columnList1 = this.KeyPCode.EnumColumns();
      List<Row.Column> columnList2 = fkEvaluator.EnumColumns();
      try
      {
        if (columnList1.Count != columnList2.Count)
          return false;
        int index1 = 0;
        for (int count1 = columnList1.Count; index1 < count1; ++index1)
        {
          Row.Column column1 = columnList1[index1];
          bool flag = false;
          int index2 = 0;
          for (int count2 = columnList2.Count; index2 < count2; ++index2)
          {
            Row.Column column2 = columnList2[index2];
            if (column1.RowIndex == column2.RowIndex)
            {
              flag = true;
              break;
            }
          }
          if (!flag)
            return false;
        }
        return true;
      }
      finally
      {
        columnList1.Clear();
        columnList2.Clear();
      }
    }

    internal override bool DoCheckLinkedForeignKey(string primaryTable)
    {
      Row satelliteRow = this.SatelliteRow;
      if (this.ContainsNulls(satelliteRow) || this.ParentRowset.IsUpdateOperation && this.NoKeyUpdate)
        return true;
      Row key = satelliteRow.CopyInstance();
      key.RowId = 0U;
      return this.WrapperDatabase.LookForReferencedKey(this.ParentRowset, key, primaryTable, (string) null);
    }

    internal override bool DoCheckUnlinkedPrimaryKey(string foreignKeyTable, string foreignKeyIndex, VistaDBReferentialIntegrity integrity)
    {
      bool isUpdateOperation = this.ParentRowset.IsUpdateOperation;
      if (isUpdateOperation && this.NoKeyUpdate)
        return true;
      if (integrity == VistaDBReferentialIntegrity.None)
      {
        Row key = this.CurrentPrimaryKey.CopyInstance();
        key.RowId = 0U;
        key.RowVersion = 0U;
        return !this.WrapperDatabase.LookForReferencedKey(this.ParentRowset, key, foreignKeyTable, foreignKeyIndex);
      }
      this.WrapperDatabase.ModifyForeignTable(isUpdateOperation, (Index) this, foreignKeyTable, foreignKeyIndex, integrity);
      return true;
    }

    protected override void Destroy()
    {
      if (this.spool != null)
      {
        this.spool.Dispose();
        this.spool = (SortSpool) null;
      }
      this.findEvaluator = (EvalStack) null;
      this.keyPcode = (EvalStack) null;
      base.Destroy();
      this.rowSet = (ClusteredRowset) null;
    }

    internal override uint TransactionId
    {
      get
      {
        return this.ParentRowset.TransactionId;
      }
    }

    internal override IsolationLevel TpIsolationLevel
    {
      get
      {
        return this.ParentRowset.TpIsolationLevel;
      }
      set
      {
      }
    }

    internal override uint RowCount
    {
      get
      {
        return this.ParentRowset.RowCount;
      }
    }

    internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
    {
      return this.ParentRowset.DoGettingAnotherTransactionStatus(transactionId);
    }

    internal override void DoIncreaseRowCount()
    {
    }

    internal override void DoDecreaseRowCount()
    {
    }

    internal override void DoUpdateRowCount()
    {
    }

    internal class RowsetIndexHeader : Index.IndexHeader
    {
      internal static RowsetIndex.RowsetIndexHeader CreateHeaderInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
      {
        return new RowsetIndex.RowsetIndexHeader(parentStorage, pageSize, culture);
      }

      private RowsetIndexHeader(DataStorage parentIndex, int pageSize, CultureInfo culture)
        : base(parentIndex, VistaDB.Engine.Core.Header.HeaderId.INDEX_HEADER, Index.Type.Standard, pageSize, culture)
      {
      }
    }
  }
}
