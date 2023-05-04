using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Indexing
{
  internal class Index : DataStorage
  {
    private Tree tree;
    private Row lowUserScope;
    private Row highUserScope;
    private Row lowQueryScope;
    private Row highQueryScope;
    private ScopeType currentScope;
    private Parser parser;
    private bool activation;
    private bool forcedTreeSeek;
    private bool dynamicDescend;
    private Row currentPrimaryKey;
    private bool noRI_KeyUpdate;
    private RowIdFilterCollection bmpFilters;

    internal static string FixKeyExpression(string keyExpression)
    {
      keyExpression = keyExpression.Trim();
      keyExpression = keyExpression.TrimEnd(";"[0]);
      return keyExpression;
    }

    protected Index(string indexName, string alias, Parser parser, DirectConnection connection, Database parentDatabase, Encryption encryption, Index clonedOrigin)
      : base(indexName, alias, connection, parentDatabase, encryption, clonedOrigin)
    {
      this.parser = parser;
    }

    internal new IndexHeader Header
    {
      get
      {
        return (IndexHeader) base.Header;
      }
    }

    internal Tree Tree
    {
      get
      {
        return tree;
      }
    }

    internal Parser Parser
    {
      get
      {
        return parser;
      }
    }

    internal bool IsPrimary
    {
      get
      {
        return Header.Primary;
      }
    }

    internal bool IsUnique
    {
      get
      {
        if (!Header.Unique)
          return Header.Sparse;
        return true;
      }
    }

    internal bool IsSparse
    {
      get
      {
        return Header.Sparse;
      }
    }

    internal bool IsFts
    {
      get
      {
        return Header.Fts;
      }
    }

    internal bool IsForeignKey
    {
      get
      {
        return Header.ForeignKey;
      }
    }

    internal override bool CaseSensitive
    {
      get
      {
        if (WrapperDatabase != null)
          return WrapperDatabase.CaseSensitive;
        return Header.CaseSensitive;
      }
    }

    internal uint IndexSignature
    {
      get
      {
        uint num = 0;
        if (IsUnique)
          num |= 4U;
        if (IsSparse)
          num |= 512U;
        if (IsPrimary)
          num |= 8U;
        if (CaseSensitive)
          num |= 64U;
        if (IsForeignKey)
          num |= 16U;
        if (IsFts)
          num |= 32U;
        return num;
      }
    }

    protected bool NoKeyUpdate
    {
      get
      {
        return noRI_KeyUpdate;
      }
      set
      {
        noRI_KeyUpdate = value;
      }
    }

    internal Row CurrentPrimaryKey
    {
      get
      {
        return currentPrimaryKey;
      }
    }

    internal virtual ClusteredRowset ParentRowset
    {
      get
      {
        return null;
      }
    }

    internal virtual bool IsClustered
    {
      get
      {
        return false;
      }
    }

    internal virtual bool BuildingStatus
    {
      get
      {
        return false;
      }
    }

    internal virtual bool AllowPostponing
    {
      get
      {
        return false;
      }
    }

    internal virtual bool ForcedCollectionMode
    {
      get
      {
        if (WrapperDatabase != null)
          return WrapperDatabase.ForcedCollectionMode;
        return false;
      }
      set
      {
        if (WrapperDatabase == null)
          return;
        WrapperDatabase.ForcedCollectionMode = value;
      }
    }

    internal override string Alias
    {
      set
      {
        base.Alias = value;
      }
    }

    protected override Row LowScope
    {
      get
      {
        return lowUserScope;
      }
    }

    protected override Row HighScope
    {
      get
      {
        return highUserScope;
      }
    }

    internal override CrossConversion Conversion
    {
      get
      {
        return WrapperDatabase.Conversion;
      }
    }

    protected void SavePrimaryKey(Row key)
    {
      if (currentPrimaryKey == null)
        return;
      currentPrimaryKey.Copy(key);
    }

    protected void SetParser(Parser parser)
    {
      this.parser = parser;
    }

    protected int SplitPolicy_3_4(int oldCount)
    {
      return oldCount * 3 / 4;
    }

    protected int SplitPolicy_1_2(int oldCount)
    {
      return oldCount / 2;
    }

    protected int SplitPolicy_1_4(int oldCount)
    {
      return oldCount / 4;
    }

    private void CreateTreeInstance()
    {
      tree = OnCreateTreeInstance();
    }

    private void CreateIndex()
    {
      try
      {
        OnCreateIndex();
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 130);
      }
    }

    private void ActivateIndex()
    {
      try
      {
        OnActivateIndex();
      }
      catch (Exception ex)
      {
        if (WrapperDatabase == null)
          throw new VistaDBException(ex, 135, Name);
        throw new VistaDBException(ex, 134, Alias);
      }
    }

    private bool DeactivateIndex()
    {
      try
      {
        return OnDeactivateIndex();
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 136);
      }
    }

    internal void ReactivateIndex()
    {
      if (tree == null)
        return;
      try
      {
        OnReactivateIndex();
      }
      catch (Exception ex)
      {
        if (tree != null)
          tree.Clear();
        if (WrapperDatabase == null)
          throw new VistaDBException(ex, 135, Name);
        throw new VistaDBException(ex, 134, Alias);
      }
    }

    internal virtual int DoSplitPolicy(int oldCount)
    {
      return SplitPolicy_3_4(oldCount);
    }

    protected virtual void OnCreateIndex()
    {
      CreateTreeInstance();
      tree.CreateRoot();
    }

    protected virtual void OnActivateIndex()
    {
      LockStorage();
      try
      {
        CreateTreeInstance();
        tree.ActivateRoot(Header.RootPosition);
      }
      finally
      {
        UnlockStorage(true);
      }
    }

    protected virtual bool OnDeactivateIndex()
    {
      Handle.ClearWholeCache(StorageId, false);
      if (tree != null)
        tree.Dispose();
      tree = null;
      return true;
    }

    protected virtual void OnReactivateIndex()
    {
      if (bmpFilters != null)
        bmpFilters.Clear();
      tree.Clear();
      Handle.ClearWholeCache(StorageId, false);
      Header.Update();
      ActivateDefaultRow();
      tree.ActivateRoot(Header.RootPosition);
    }

    protected virtual Tree OnCreateTreeInstance()
    {
      Index index = null;
      if (index != null)
        return index.Tree.GetClone();
      return new Tree(this);
    }

    protected virtual bool OnPartialKeyFound(Row patternRow)
    {
      return false;
    }

    internal virtual bool DoCheckIfRelated(EvalStack fkEvaluator)
    {
      return false;
    }

    internal virtual bool DoCheckIfSame(EvalStack fkEvaluator)
    {
      return true;
    }

    internal virtual bool DoCheckLinkedForeignKey(string primaryTable)
    {
      return false;
    }

    internal virtual bool DoCheckUnlinkedPrimaryKey(string foreignKeyTable, string foreignKeyIndex, VistaDBReferentialIntegrity integrity)
    {
      return false;
    }

    internal virtual void DoSetFtsActive()
    {
    }

    internal virtual void DoSetFtsInactive()
    {
    }

    protected override void OnRollbackStorageVersion()
    {
      try
      {
        base.OnRollbackStorageVersion();
      }
      finally
      {
        if (tree != null)
          tree.Clear();
      }
    }

    protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      activation = true;
      try
      {
        base.OnOpenStorage(openMode, headerPosition);
        ActivateIndex();
      }
      finally
      {
        activation = false;
      }
    }

    protected override void OnCreateStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnCreateStorage(openMode, headerPosition);
      CreateIndex();
    }

    protected override void OnCloseStorage()
    {
      DeactivateIndex();
      base.OnCloseStorage();
    }

    protected override StorageHandle OnAttachLockStorage(ulong headerPosition)
    {
      StorageHandle storageHandle = base.OnAttachLockStorage(headerPosition);
      if (NoLocks || WrapperDatabase == null || VirtualLocks)
        return storageHandle;
      int num = 5;
      while (true)
      {
        try
        {
          return ParentConnection.StorageManager.OpenOrCreateTemporaryStorage(WrapperDatabase.Name + '$' + headerPosition.ToString() + ".vdb4lck", true, PageSize, (WrapperDatabase.Handle.IsolatedStorage ? 1 : 0) != 0, (ParentConnection.PersistentLockFiles ? 1 : 0) != 0);
        }
        catch (Exception ex)
        {
          if (!(ex is UnauthorizedAccessException) || num <= 0)
            throw ex;
          Thread.Sleep(50);
        }
        --num;
      }
    }

    protected override void OnLowLevelLockStorage(ulong offset, int bytes)
    {
      Handle.Lock(Header.Position, 1, 0UL);
    }

    protected override void OnLowLevelUnlockStorage(ulong offset, int bytes)
    {
      if (Handle == null)
        return;
      Handle.Unlock(Header.Position, 1, 0UL);
    }

    protected override bool OnLockStorage()
    {
      if (base.OnLockStorage())
        return !activation;
      return false;
    }

    protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
    {
      return clonedStorage.Header;
    }

    protected override void OnActivateHeader(ulong position)
    {
      base.OnActivateHeader(position);
      dynamicDescend = Header.Descend;
    }

    protected override void OnUpdateStorageVersion(ref bool newVersion)
    {
      if (activation)
        return;
      base.OnUpdateStorageVersion(ref newVersion);
      newVersion = newVersion || tree != null && tree.IsCacheEmpty;
      if (!newVersion)
        return;
      ReactivateIndex();
    }

    protected override void OnFlushStorageVersion()
    {
      Header.RootPosition = tree.RootNode.Id;
      tree.FlushTree();
      base.OnFlushStorageVersion();
    }

    protected override void OnAllocateRows()
    {
      base.OnAllocateRows();
      lowUserScope = TopRow.CopyInstance();
      highUserScope = BottomRow.CopyInstance();
      lowQueryScope = TopRow.CopyInstance();
      highQueryScope = BottomRow.CopyInstance();
      currentScope = ScopeType.None;
      if (!IsPrimary)
        return;
      currentPrimaryKey = DoAllocateCachedPkInstance();
    }

    protected virtual Row DoAllocateCachedPkInstance()
    {
      return CurrentRow.CopyInstance();
    }

    protected override void OnGoCurrentRow(bool soft)
    {
      if (forcedTreeSeek || (int) CurrentRow.RowId != (int) tree.CurrentKey.RowId || ((int) CurrentRow.RowVersion != (int) tree.CurrentKey.RowVersion || (int) CurrentRow.RowId == (int) Row.MinRowId) || !tree.CurrentNode.IsLeaf)
      {
        switch (tree.GoKey(CurrentRow, null).KeyRank)
        {
          case Node.KeyPosition.OnLeft:
            if (!soft)
            {
              tree.GoPrevKey();
              break;
            }
            break;
          case Node.KeyPosition.OnRight:
            tree.GoNextKey();
            break;
        }
      }
      base.OnGoCurrentRow(soft);
    }

    protected override void OnNextRow()
    {
      tree.GoNextKey();
    }

    protected override void OnPrevRow()
    {
      tree.GoPrevKey();
    }

    protected override void OnSynch(int asynchCounter)
    {
      LockStorage();
      try
      {
        base.OnSynch(asynchCounter);
      }
      finally
      {
        UnlockStorage(true);
      }
    }

    private void SynchronizeTreePosition()
    {
      if (Tree.CurrentNode.SuspectForCorruption)
        throw new VistaDBException(133, "Index appears to be corrupted and cannot be used.");
      CurrentRow.Copy(tree.CurrentKey);
      BgnOfSet = CurrentRow - TopRow <= 0;
      EndOfSet = CurrentRow - BottomRow >= 0;
    }

    protected override void OnUpdateCurrentRow()
    {
      SynchronizeTreePosition();
    }

    protected override bool OnMinimizeMemoryCache(bool forceClearing)
    {
      if (tree == null)
        return false;
      return tree.MinimizeTreeMemory(forceClearing);
    }

    protected override bool OnFlushCurrentRow()
    {
      if ((int) SatelliteRow.RowId != (int) Row.MaxRowId)
        return tree.ReplaceKey(CurrentRow, SatelliteRow, TransactionId);
      return tree.DeleteKey(CurrentRow, TransactionId);
    }

    protected override bool OnCreateRow(bool blank, Row newKey)
    {
      if (!base.OnCreateRow(blank, newKey))
        return false;
      CurrentRow.RowId = 0U;
      if (IsFts && (long) tree.TestEqualKeyData(newKey) == newKey.RowId)
        return false;
      if (!IsUnique || (long) tree.TestEqualKeyData(newKey) == Row.MaxRowId)
        return true;
      if (IsSparse || SuppressErrors)
        return false;
      throw new VistaDBException(309, Alias + ": " + newKey.ToString());
    }

    protected override bool OnUpdateRow(Row oldKey, Row newKey)
    {
      bool flag1 = false;
      if (!base.OnUpdateRow(oldKey, newKey))
        return false;
      bool isPrimary = IsPrimary;
      bool isForeignKey = IsForeignKey;
      bool flag2 = oldKey.EqualColumns(newKey, IsClustered);
      noRI_KeyUpdate = (isForeignKey || isPrimary) && flag2;
      if (!flag2 && IsUnique)
      {
        ulong num = tree.TestEqualKeyData(newKey);
        flag1 = (long) num != Row.MaxRowId && (long) num != oldKey.RowId;
        if (flag1)
        {
          if (SuppressErrors || IsSparse)
            return false;
          throw new VistaDBException(309, Alias + ": " + newKey.ToString());
        }
      }
      if (flag1)
        newKey.RowId = Row.MaxRowId;
      return true;
    }

    protected override bool OnDeleteRow(Row currentRow)
    {
      SatelliteRow.RowId = Row.MaxRowId;
      return true;
    }

    protected override bool DoAfterCreateRow(bool created)
    {
      if (created)
      {
        GoCurrentRow(false);
        SynchronizeTreePosition();
      }
      return created;
    }

    protected override bool DoAfterUpdateRow(uint rowId, bool updated)
    {
      if (updated)
      {
        GoCurrentRow(false);
        SynchronizeTreePosition();
      }
      return updated;
    }

    protected override bool DoAfterDeleteRow(uint rowId, bool deleted)
    {
      if (deleted)
      {
        GoCurrentRow(true);
        SynchronizeTreePosition();
        while (!PassOrdinaryFilters())
        {
          tree.GoNextKey();
          SynchronizeTreePosition();
        }
      }
      return deleted;
    }

    protected override void OnSetCurrentRow(Row key)
    {
      CurrentRow.Copy(key);
      base.OnSetCurrentRow(key);
    }

    protected override void OnSetSatelliteRow(Row key)
    {
      SatelliteRow.Copy(key);
    }

    protected override bool OnIsClearScope()
    {
      return currentScope != ScopeType.UserScope;
    }

    protected override void OnClearScope(ScopeType scope)
    {
      switch (scope)
      {
        case ScopeType.QueryScope:
          if (currentScope == ScopeType.UserScope)
          {
            SetScope(null, null, ScopeType.UserScope, true);
            return;
          }
          break;
        case ScopeType.UserScope:
          if (currentScope == ScopeType.QueryScope)
          {
            SetScope(null, null, ScopeType.QueryScope, true);
            return;
          }
          break;
      }
      currentScope = ScopeType.None;
      BottomRow.InitBottom();
      TopRow.InitTop();
    }

    protected override bool OnSetScope(Row lowValue, Row highValue, ScopeType scope, bool exactMatching)
    {
      switch (scope)
      {
        case ScopeType.QueryScope:
          if (lowValue == null && highValue == null)
          {
            if (currentScope != ScopeType.QueryScope)
              return true;
            lowValue = lowQueryScope;
            highValue = highQueryScope;
            break;
          }
          lowQueryScope.Copy(lowValue);
          highQueryScope.Copy(highValue);
          currentScope = ScopeType.QueryScope;
          break;
        case ScopeType.UserScope:
          if (lowValue == null && highValue == null)
          {
            if (currentScope != ScopeType.UserScope)
              return true;
            lowValue = lowUserScope;
            highValue = highUserScope;
            break;
          }
          lowUserScope.Copy(lowValue);
          highUserScope.Copy(highValue);
          currentScope = ScopeType.UserScope;
          break;
      }
      TopRow.InitTop();
      BottomRow.InitBottom();
      if (highValue != null)
      {
        switch (tree.GoKey(highValue, null).KeyRank)
        {
          case Node.KeyPosition.Less:
          case Node.KeyPosition.OnLeft:
            BottomRow.Copy(tree.CurrentKey);
            break;
          default:
            tree.GoNextKey();
            goto case Node.KeyPosition.Less;
        }
      }
      if (lowValue != null)
      {
        if (tree.GoKey(lowValue, null).KeyRank != Node.KeyPosition.OnRight)
          tree.GoPrevKey();
        TopRow.Copy(tree.CurrentKey);
      }
      SetCurrentRow(TopRow);
      return true;
    }

    protected override bool DoActivateLinkFrom(DataStorage externalStorage, Relationships.Relation link, Row linkingRow)
    {
      if (externalStorage.EndOfSet)
        return true;
      try
      {
        return base.DoActivateLinkFrom(externalStorage, link, linkingRow);
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 253, externalStorage.Name);
      }
    }

    protected override bool DoCreateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row newRow)
    {
      FreezeRelationships();
      try
      {
        return CreateRow(false, false) || IsUnique && IsSparse;
      }
      finally
      {
        DefreezeRelationships();
      }
    }

    protected override bool DoUpdateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row oldKey, Row newKey)
    {
      FreezeRelationships();
      try
      {
        return UpdateRow(false);
      }
      finally
      {
        DefreezeRelationships();
      }
    }

    protected override bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
    {
      FreezeRelationships();
      try
      {
        return DeleteRow(false);
      }
      finally
      {
        DefreezeRelationships();
      }
    }

    protected override bool OnSeekRow(Row row, bool partialMatching)
    {
      bool forcedTreeSeek = this.forcedTreeSeek;
      this.forcedTreeSeek = true;
      try
      {
        base.OnSeekRow(row, partialMatching);
      }
      finally
      {
        this.forcedTreeSeek = forcedTreeSeek;
      }
      return Tree.CurrentKey.EqualColumns(row, IsClustered);
    }

    protected override ulong OnGetFreeCluster(int pageCount)
    {
      if (WrapperDatabase == null)
        return base.OnGetFreeCluster(pageCount);
      WrapperDatabase.LockSpaceMap();
      try
      {
        return base.OnGetFreeCluster(pageCount);
      }
      finally
      {
        WrapperDatabase.UnlockSpaceMap();
      }
    }

    protected override void OnSetFreeCluster(ulong clusterId, int pageCount)
    {
      if (WrapperDatabase != null)
      {
        WrapperDatabase.LockSpaceMap();
        try
        {
          base.OnSetFreeCluster(clusterId, pageCount);
        }
        finally
        {
          WrapperDatabase.UnlockSpaceMap();
        }
      }
      else
        base.OnSetFreeCluster(clusterId, pageCount);
    }

    internal void Flip()
    {
      dynamicDescend = !dynamicDescend;
    }

    protected bool PartialKeyFound(Row patternRow)
    {
      return OnPartialKeyFound(patternRow);
    }

    internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
    {
      return TpStatus.Commit;
    }

    internal void ClearCachedBitmaps()
    {
      if (bmpFilters == null)
        return;
      bmpFilters.Clear();
    }

    private void MarkOptimizedStatus(RowIdFilter filter, Row lowScope, Row highScope, bool markNulls, bool forcedTrueStatus)
    {
      FreezeRelationships();
      bool forcedTreeSeek = this.forcedTreeSeek;
      try
      {
        TopRow.Copy(lowScope);
        BottomRow.Copy(highScope);
        this.forcedTreeSeek = true;
        MoveToRow(lowScope);
        while (!EndOfSet)
        {
          if (markNulls || !CurrentRow.HasNulls)
            filter.SetValidStatus(CurrentRow, forcedTrueStatus || !markNulls);
          NextRow();
        }
      }
      finally
      {
        this.forcedTreeSeek = forcedTreeSeek;
        DefreezeRelationships();
        TopRow.InitTop();
        BottomRow.InitBottom();
      }
    }

    internal IOptimizedFilter BuildFiltermap(Row lowConstant, Row highConstant, bool excludeNulls)
    {
      if (bmpFilters == null)
        bmpFilters = new RowIdFilterCollection();
      RowIdFilter filter = bmpFilters.GetFilter(lowConstant, highConstant, excludeNulls);
      if (filter != null)
        return filter;
      Row lowConstant1 = lowConstant.CopyInstance();
      Row highConstant1 = highConstant.CopyInstance();
      try
      {
        filter = ParentRowset.NewOptimizedFilter;
        if (excludeNulls)
        {
          MarkOptimizedStatus(filter, lowConstant, highConstant, false, false);
          if (IsFts || !excludeNulls)
            return filter;
          bool flag = false;
          foreach (Row.Column column in (List<Row.Column>) CurrentRow)
          {
            if (column.AllowNull)
            {
              flag = true;
              break;
            }
          }
          if (!flag)
            return filter;
        }
        lowConstant = TopRow;
        highConstant = BottomRow;
        lowConstant.RowId = Row.MinRowId;
        highConstant.RowId = Row.MaxRowId;
        for (int index = 0; index < 1; ++index)
        {
          lowConstant[index].Value = null;
          highConstant[index].Value = null;
        }
        MarkOptimizedStatus(filter, lowConstant, highConstant, true, !excludeNulls);
        return filter;
      }
      finally
      {
        TopRow.InitTop();
        BottomRow.InitBottom();
        bmpFilters.PutFilter(filter, lowConstant1, highConstant1, excludeNulls);
      }
    }

    internal long GetScopeKeyCount()
    {
      FreezeRelationships();
      try
      {
        int num = 0;
        MoveToRow(TopRow);
        if (BgnOfSet)
          NextRow();
        while (!EndOfSet)
        {
          ++num;
          NextRow();
        }
        return num;
      }
      finally
      {
        DefreezeRelationships();
      }
    }

    internal enum Type : uint
    {
      Standard = 0,
      Clustered = 1,
      Unique = 4,
      PrimaryKey = 8,
      ForeignKey = 16, // 0x00000010
      Fts = 32, // 0x00000020
      Sensitive = 64, // 0x00000040
      For = 128, // 0x00000080
      Temporary = 256, // 0x00000100
      Sparse = 512, // 0x00000200
    }

    internal class RowsetHeader : StorageHeader
    {
      internal static RowsetHeader CreateInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
      {
        return new RowsetHeader(parentStorage, HeaderId.ROWSET_HEADER, 0, pageSize, culture);
      }

      protected RowsetHeader(DataStorage parentStorage, HeaderId id, int signature, int pageSize, CultureInfo culture)
        : base(parentStorage, id, EmptyReference, signature, pageSize, culture)
      {
      }
    }

    internal class IndexHeader : StorageHeader
    {
      private int rootEntry;
      private int descendEntry;

      protected IndexHeader(DataStorage parentIndex, HeaderId id, Type type, int pageSize, CultureInfo culture)
        : base(parentIndex, id, EmptyReference, (int) type, pageSize, culture)
      {
        rootEntry = AppendColumn(new BigIntColumn((long)EmptyReference));
        descendEntry = AppendColumn(new BitColumn(false));
      }

      internal static bool IsUnique(uint signature)
      {
        return ((int) signature & 4) == 4;
      }

      internal static bool IsPrimary(uint signature)
      {
        return ((int) signature & 8) == 8;
      }

      internal static bool IsForeignKey(uint signature)
      {
        return ((int) signature & 16) == 16;
      }

      internal static bool IsFts(uint signature)
      {
        return ((int) signature & 32) == 32;
      }

      internal static bool IsSparse(uint signature)
      {
        return ((int) signature & 512) == 512;
      }

      internal static bool IsCaseSensitive(uint signature)
      {
        return ((int) signature & 64) == 64;
      }

      internal ulong RootPosition
      {
        get
        {
          return (ulong) (long) this[rootEntry].Value;
        }
        set
        {
          Modified = (long) RootPosition != (long) value;
          this[rootEntry].Value = (long)value;
        }
      }

      internal bool Descend
      {
        get
        {
          return (bool) this[descendEntry].Value;
        }
        set
        {
          this[descendEntry].Value = value;
        }
      }

      internal bool Unique
      {
        get
        {
          return IsUnique(Signature);
        }
      }

      internal bool Primary
      {
        get
        {
          return IsPrimary(Signature);
        }
      }

      internal bool ForeignKey
      {
        get
        {
          return IsForeignKey(Signature);
        }
      }

      internal bool Fts
      {
        get
        {
          return IsFts(Signature);
        }
      }

      internal bool Sparse
      {
        get
        {
          return IsSparse(Signature);
        }
      }

      internal bool CaseSensitive
      {
        get
        {
          return IsCaseSensitive(Signature);
        }
      }

      internal bool Temporary
      {
        get
        {
          return ((int) Signature & 256) == 256;
        }
      }

      internal void CreateSchema(IVistaDBTableSchema schema)
      {
        OnCreateSchema(schema);
      }

      internal bool ActivateSchema()
      {
        return OnActivateSchema();
      }

      internal void SetSensitivity(bool caseSensitive)
      {
        if (caseSensitive)
          Signature |= 64U;
        else
          Signature &= 4294967231U;
      }

      protected virtual void OnCreateSchema(IVistaDBTableSchema schema)
      {
        Modified = true;
      }

      protected virtual bool OnActivateSchema()
      {
        return false;
      }

      internal void RegisterSchema(IVistaDBTableSchema schema)
      {
        OnRegisterSchema(schema);
      }

      protected virtual void OnRegisterSchema(IVistaDBTableSchema schema)
      {
        ParentStorage.WrapperDatabase.RegisterRowsetSchema(ParentStorage, schema);
      }
    }
  }
}
