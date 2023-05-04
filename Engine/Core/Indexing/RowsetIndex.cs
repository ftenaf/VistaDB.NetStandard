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

    internal new RowsetIndexHeader Header
    {
      get
      {
        return (RowsetIndexHeader) base.Header;
      }
    }

    internal string KeyExpression
    {
      get
      {
        return keyExpression;
      }
    }

    internal override bool BuildingStatus
    {
      get
      {
        return buildingStatus;
      }
    }

    internal override ClusteredRowset ParentRowset
    {
      get
      {
        return rowSet;
      }
    }

    protected override Row OnCreateEmptyRowInstance()
    {
      return Row.CreateInstance(0U, !Header.Descend, Encryption, (int[]) null);
    }

    protected override Row OnCreateEmptyRowInstance(int maxColCount)
    {
      return Row.CreateInstance(0U, !Header.Descend, Encryption, (int[]) null, maxColCount);
    }

    internal override CrossConversion Conversion
    {
      get
      {
        return ParentRowset.WrapperDatabase.Conversion;
      }
    }

    internal EvalStack KeyPCode
    {
      get
      {
        return keyPcode;
      }
    }

    private void CreatePCode()
    {
      try
      {
        OnCreatePCode();
      }
      catch (Exception ex)
      {
        keyPcode = (EvalStack) null;
        throw ex;
      }
    }

    private void InitSpool(uint keyCount, ref int expectedKeyLen)
    {
      try
      {
        if (spool != null)
          spool.Dispose();
        spool = keyCount == 0U ? (SortSpool) new DummySpool() : new SortSpool(Handle.IsolatedStorage, keyCount, ref expectedKeyLen, BottomRow, ParentConnection.StorageManager, false);
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
        SetRelationship((DataStorage) this, (DataStorage) ParentRowset, Relationships.Type.One_To_One, (EvalStack) null, false);
        ParentRowset.SetRelationship((DataStorage) ParentRowset, (DataStorage) this, Relationships.Type.One_To_One, KeyPCode, true);
      }
      finally
      {
        DefreezeRelationships();
        ParentRowset.DefreezeRelationships();
      }
    }

    private void UnregisterRowset()
    {
      ParentRowset.ResetRelationship((DataStorage) ParentRowset, (DataStorage) this);
      ResetRelationship((DataStorage) this, (DataStorage) ParentRowset);
    }

    internal bool IsCorrectPrimaryKeyExpr(string primaryKey)
    {
      return false;
    }

    internal void ReDeclareIndex()
    {
      DeclareNewStorage((object) CollectIndexInformation());
    }

    internal byte[] RowKeyStructure
    {
      get
      {
        List<Row.Column> columnList = KeyPCode.EnumColumns();
        byte[] numArray = new byte[columnList.Count * 2];
        for (int index1 = 0; index1 < columnList.Count; ++index1)
        {
          int index2 = index1 * 2;
          short rowIndex = (short) columnList[index1].RowIndex;
          numArray[index2] = (byte) rowIndex;
          short num = (short) ((int) (short) ((int) rowIndex & 768) >> 7);
          numArray[index2 + 1] = IsDescendKeyColumn(index1) ? (byte) (1U | (uint) (byte) num) : (byte) num;
        }
        return numArray;
      }
    }

    internal IVistaDBIndexInformation CollectIndexInformation()
    {
      return (IVistaDBIndexInformation) new Table.TableSchema.IndexCollection.IndexInformation(Name, Alias, KeyExpression, IsUnique, IsPrimary, Header.Descend, IsSparse, IsForeignKey, IsFts, false, StorageId, RowKeyStructure);
    }

    internal void EvaluateSpoolKey(bool forceOutput)
    {
      if (!buildingStatus)
        return;
      OnEvaluateSpoolKey(forceOutput);
    }

    internal bool StartBuild(uint maxKeyCount, ref int expectedKeyLen)
    {
      if (buildingStatus)
        OnStartBuild(maxKeyCount, ref expectedKeyLen);
      return buildingStatus;
    }

    internal void FinishBuild()
    {
      if (!buildingStatus)
        return;
      bool flag = false;
      bool isTemporary = IsTemporary;
      try
      {
        OnFinishBuild();
        FlushStorageVersion();
        flag = true;
      }
      finally
      {
        FinalizeChanges(!flag, isTemporary);
        if (flag)
          RegisterRowset();
      }
      MinimizeMemoryCache(false);
    }

    internal void RegisterInDatabase()
    {
      if (!buildingStatus || WrapperDatabase == null)
        return;
      WrapperDatabase.RegisterIndex(this);
    }

    internal void RegisterInDatabase(bool justResetBuildStatus)
    {
      if (!buildingStatus)
        return;
      buildingStatus = false;
      if (justResetBuildStatus || WrapperDatabase == null || WrapperDatabase == null)
        return;
      WrapperDatabase.RegisterIndex(this);
    }

    protected virtual void OnCreatePCode()
    {
      SetParser(ParentRowset.WrapperDatabase.SqlKeyParser);
      keyPcode = Parser.Compile(KeyExpression, (DataStorage) ParentRowset, false, false, CaseSensitive, (EvalStack) null);
      if (keyPcode == null)
        throw new VistaDBException(282, keyExpression);
      if (ParentRowset != null)
        ParentRowset.SatelliteRow.InitTop();
      keyPcode.Exec(ParentRowset.SatelliteRow, CreateEmptyRowInstance());
    }

    protected virtual bool IsDescendKeyColumn(int index)
    {
      return CurrentRow[index].Descending;
    }

    protected virtual void OnEvaluateSpoolKey(bool forceOutput)
    {
      Row currentRow = ParentRowset.CurrentRow;
      try
      {
        KeyPCode.Exec(currentRow, CreateEmptyRowInstance(BottomRow.Count));
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 307, new string(KeyPCode.Expression));
      }
      spool.PushKey(KeyPCode.EvaluatedRow, forceOutput);
    }

    internal virtual void OnStartBuild(uint maxKeyCount, ref int expectedKeyLen)
    {
      CloseStorage();
      StorageHandle.StorageMode accessMode = ParentRowset.Handle.Mode;
      bool commit = Header.Temporary && !WrapperDatabase.IsTemporary;
      if (commit)
      {
        accessMode = new StorageHandle.StorageMode(FileMode.CreateNew, false, false, accessMode.Access | FileAccess.Write, false, accessMode.IsolatedStorage);
        accessMode.Temporary = true;
      }
      ulong headerPosition = commit ? 0UL : WrapperDatabase.GetFreeCluster(1);
      CreateStorage(accessMode, headerPosition, commit);
      InitSpool(maxKeyCount, ref expectedKeyLen);
    }

    internal virtual void OnFinishBuild()
    {
      bool flag = !IsSparse;
      CurrentRow.RowId = 0U;
      using (spool)
      {
        spool.Sort();
        Row row = BottomRow;
        bool isUnique = IsUnique;
        bool isPrimary = IsPrimary;
        bool isFts = IsFts;
        int num = 0;
        int keyCount = spool.KeyCount;
        while (num < keyCount)
        {
          Row key = spool.PopKey();
          if (isUnique && num > 0 && key.EqualColumns(row, IsClustered))
          {
            if (flag)
              throw new VistaDBException(309, Alias + ": " + row.ToString());
          }
          else
          {
            if (isPrimary && ContainsNulls(key))
              throw new VistaDBException(142, Alias + ": " + key.ToString());
            if (!isFts || num <= 0 || key - row != 0)
              Tree.AppendLeafKey(key);
          }
          ++num;
          row = key;
        }
        Tree.FinalizeAppending();
      }
    }

    protected override Row DoAllocateCurrentRow()
    {
      return KeyPCode.EvaluatedRow.CopyInstance();
    }

    protected override Row DoAllocateCachedPkInstance()
    {
      return KeyPCode.EvaluatedRow.CopyInstance();
    }

    protected override void OnDeclareNewStorage(object hint)
    {
      if (hint == null)
        throw new VistaDBException(133, Alias);
      IVistaDBIndexInformation indexInformation = (IVistaDBIndexInformation) hint;
            Type type = Type.Standard;
      bool fullTextSearch = indexInformation.FullTextSearch;
      if (fullTextSearch)
        type |= Type.Fts;
      if (indexInformation.Primary)
        type |= Type.PrimaryKey;
      if (indexInformation.FKConstraint)
        type |= Type.ForeignKey;
      if (indexInformation.Unique)
        type |= Type.Unique;
      if (((Table.TableSchema.IndexCollection.IndexInformation) indexInformation).Sparse)
        type = type | Type.Sparse | Type.Unique;
      if (indexInformation.Temporary)
        type |= Type.Temporary;
      if (ParentRowset.WrapperDatabase.CaseSensitive)
        type |= Type.Sensitive;
      Header.Signature = (uint) type;
      Header.Descend = false;
      CreatePCode();
      List<Row.Column> columnList = KeyPCode.EnumColumns();
      if (columnList.Count > (int) byte.MaxValue)
        throw new VistaDBException(149, keyExpression);
      foreach (Row.Column column in columnList)
      {
        if (fullTextSearch && column.InternalType != VistaDBType.NChar || column.InternalType == VistaDBType.VarBinary)
          throw new VistaDBException(150, column.Name);
      }
      buildingStatus = true;
    }

    protected override StorageHandle OnAttachLockStorage(ulong headerPosition)
    {
      return Handle;
    }

    protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnOpenStorage(openMode, headerPosition);
      RegisterRowset();
    }

    protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
    {
      if (clonedStorage != null)
        return base.DoCreateHeaderInstance(pageSize, culture, clonedStorage);
      return (StorageHeader)RowsetIndexHeader.CreateHeaderInstance((DataStorage) this, ParentRowset.PageSize, ParentRowset.Culture);
    }

    protected override void OnActivateHeader(ulong position)
    {
      base.OnActivateHeader(position);
      CreatePCode();
    }

    protected override void OnCloseStorage()
    {
      UnregisterRowset();
      base.OnCloseStorage();
    }

    protected override bool DoAfterDeleteLinkFrom(DataStorage storage, bool deleted)
    {
      if (storage != ParentRowset)
        return base.DoAfterDeleteLinkFrom(storage, deleted);
      return true;
    }

    protected override void OnSynch(int asynchCounter)
    {
      bool flag = asynchCounter != 0 && ParentRowset.PostponedSynchronization;
      if (flag)
        FreezeRelationships();
      try
      {
        base.OnSynch(asynchCounter);
      }
      finally
      {
        if (flag)
          DefreezeRelationships();
      }
    }

    protected override void OnTop()
    {
      ParentRowset.EndOfSet = false;
      ParentRowset.BgnOfSet = false;
      base.OnTop();
    }

    protected override void OnBottom()
    {
      ParentRowset.EndOfSet = false;
      ParentRowset.BgnOfSet = false;
      base.OnBottom();
    }

    protected override bool OnSetScope(Row lowValue, Row highValue, ScopeType scopes, bool exactMatching)
    {
      return base.OnSetScope(lowValue, highValue, scopes, exactMatching);
    }

    protected override bool OnAssumeLink(Relationships.Relation link, bool toModify)
    {
      LockStorage();
      return true;
    }

    protected override bool OnResumeLink(Relationships.Relation link, bool toModify)
    {
      UnlockStorage(!toModify);
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
      SavePrimaryKey(oldKey);
      if ((int) oldKey.RowVersion == (int) newKey.RowVersion && oldKey.EqualColumns(newKey, false) && !newKey.EditedExtensions)
      {
        NoKeyUpdate = true;
        return true;
      }
      NoKeyUpdate = false;
      return base.DoUpdateLinkFrom(externalStorage, type, oldKey, newKey);
    }

    protected override bool DoCreateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row newRow)
    {
      SavePrimaryKey(newRow);
      return base.DoCreateLinkFrom(externalStorage, type, newRow);
    }

    protected override bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
    {
      SavePrimaryKey(row);
      return base.DoDeleteLinkFrom(externalStorage, type, row);
    }

    protected override Row OnCompileRow(string keyEvaluationExpression, bool initTop)
    {
      try
      {
        EvalStack evalStack = ParentRowset.WrapperDatabase.Parser.Compile(keyEvaluationExpression, (DataStorage) ParentRowset, false, false, CaseSensitive, findEvaluator);
        if (findEvaluator == null)
          findEvaluator = evalStack;
        if (evalStack != findEvaluator)
          return (Row) null;
        Row contextRow = ParentRowset.CurrentRow.CopyInstance();
        List<Row.Column> columnList = KeyPCode.EnumColumns();
        int index = 0;
        for (int count = columnList.Count; index < count; ++index)
        {
          Row.Column column = columnList[index];
          contextRow[column.RowIndex].Descending = CurrentRow[index].Descending;
        }
        if (initTop)
          contextRow.InitTop();
        else
          contextRow.InitBottom();
        evalStack.Exec(contextRow);
        KeyPCode.Exec(contextRow, CreateEmptyRowInstance());
        return KeyPCode.EvaluatedRow;
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
        return PartialKeyFound(row);
      return false;
    }

    protected override bool OnPartialKeyFound(Row patternRow)
    {
      foreach (Row.Column column in (List<Row.Column>) patternRow)
      {
        int rowIndex = column.RowIndex;
        Row.Column b = CurrentRow[rowIndex];
        long num = (long) column.MinusColumn(b);
        if (rowIndex == 0)
        {
          if (num != 0L)
          {
            if (b.InternalType == VistaDBType.NChar && !b.IsNull && !column.IsNull)
              return ((string) b.Value).StartsWith((string) column.Value, !CaseSensitive, Culture);
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
      if (WrapperDatabase == null)
        return;
      WrapperDatabase.LockStorage();
      try
      {
        Tree.CleanUpNodeSpace(Tree.RootNode, true);
        Tree.Modified = false;
      }
      finally
      {
        WrapperDatabase.UnlockStorage(false);
      }
    }

    internal override bool SuppressErrors
    {
      get
      {
        return ParentRowset.SuppressErrors;
      }
      set
      {
        ParentRowset.SuppressErrors = value;
      }
    }

    internal override bool DoCheckIfRelated(EvalStack fkEvaluator)
    {
      if (!IsPrimary)
        return false;
      List<Row.Column> columnList1 = KeyPCode.EnumColumns();
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
      if (!IsPrimary)
        return false;
      List<Row.Column> columnList1 = KeyPCode.EnumColumns();
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
      Row satelliteRow = SatelliteRow;
      if (ContainsNulls(satelliteRow) || ParentRowset.IsUpdateOperation && NoKeyUpdate)
        return true;
      Row key = satelliteRow.CopyInstance();
      key.RowId = 0U;
      return WrapperDatabase.LookForReferencedKey(ParentRowset, key, primaryTable, (string) null);
    }

    internal override bool DoCheckUnlinkedPrimaryKey(string foreignKeyTable, string foreignKeyIndex, VistaDBReferentialIntegrity integrity)
    {
      bool isUpdateOperation = ParentRowset.IsUpdateOperation;
      if (isUpdateOperation && NoKeyUpdate)
        return true;
      if (integrity == VistaDBReferentialIntegrity.None)
      {
        Row key = CurrentPrimaryKey.CopyInstance();
        key.RowId = 0U;
        key.RowVersion = 0U;
        return !WrapperDatabase.LookForReferencedKey(ParentRowset, key, foreignKeyTable, foreignKeyIndex);
      }
      WrapperDatabase.ModifyForeignTable(isUpdateOperation, (Index) this, foreignKeyTable, foreignKeyIndex, integrity);
      return true;
    }

    protected override void Destroy()
    {
      if (spool != null)
      {
        spool.Dispose();
        spool = (SortSpool) null;
      }
      findEvaluator = (EvalStack) null;
      keyPcode = (EvalStack) null;
      base.Destroy();
      rowSet = (ClusteredRowset) null;
    }

    internal override uint TransactionId
    {
      get
      {
        return ParentRowset.TransactionId;
      }
    }

    internal override IsolationLevel TpIsolationLevel
    {
      get
      {
        return ParentRowset.TpIsolationLevel;
      }
      set
      {
      }
    }

    internal override uint RowCount
    {
      get
      {
        return ParentRowset.RowCount;
      }
    }

    internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
    {
      return ParentRowset.DoGettingAnotherTransactionStatus(transactionId);
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

    internal class RowsetIndexHeader : IndexHeader
        {
      internal static RowsetIndexHeader CreateHeaderInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
      {
        return new RowsetIndexHeader(parentStorage, pageSize, culture);
      }

      private RowsetIndexHeader(DataStorage parentIndex, int pageSize, CultureInfo culture)
        : base(parentIndex, HeaderId.INDEX_HEADER, Type.Standard, pageSize, culture)
      {
      }
    }
  }
}
