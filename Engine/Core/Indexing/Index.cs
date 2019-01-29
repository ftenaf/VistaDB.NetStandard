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
    private DataStorage.ScopeType currentScope;
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
      : base(indexName, alias, connection, parentDatabase, encryption, (DataStorage) clonedOrigin)
    {
      this.parser = parser;
    }

    internal Index.IndexHeader Header
    {
      get
      {
        return (Index.IndexHeader) base.Header;
      }
    }

    internal Tree Tree
    {
      get
      {
        return this.tree;
      }
    }

    internal Parser Parser
    {
      get
      {
        return this.parser;
      }
    }

    internal bool IsPrimary
    {
      get
      {
        return this.Header.Primary;
      }
    }

    internal bool IsUnique
    {
      get
      {
        if (!this.Header.Unique)
          return this.Header.Sparse;
        return true;
      }
    }

    internal bool IsSparse
    {
      get
      {
        return this.Header.Sparse;
      }
    }

    internal bool IsFts
    {
      get
      {
        return this.Header.Fts;
      }
    }

    internal bool IsForeignKey
    {
      get
      {
        return this.Header.ForeignKey;
      }
    }

    internal override bool CaseSensitive
    {
      get
      {
        if (this.WrapperDatabase != null)
          return this.WrapperDatabase.CaseSensitive;
        return this.Header.CaseSensitive;
      }
    }

    internal uint IndexSignature
    {
      get
      {
        uint num = 0;
        if (this.IsUnique)
          num |= 4U;
        if (this.IsSparse)
          num |= 512U;
        if (this.IsPrimary)
          num |= 8U;
        if (this.CaseSensitive)
          num |= 64U;
        if (this.IsForeignKey)
          num |= 16U;
        if (this.IsFts)
          num |= 32U;
        return num;
      }
    }

    protected bool NoKeyUpdate
    {
      get
      {
        return this.noRI_KeyUpdate;
      }
      set
      {
        this.noRI_KeyUpdate = value;
      }
    }

    internal Row CurrentPrimaryKey
    {
      get
      {
        return this.currentPrimaryKey;
      }
    }

    internal virtual ClusteredRowset ParentRowset
    {
      get
      {
        return (ClusteredRowset) null;
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
        if (this.WrapperDatabase != null)
          return this.WrapperDatabase.ForcedCollectionMode;
        return false;
      }
      set
      {
        if (this.WrapperDatabase == null)
          return;
        this.WrapperDatabase.ForcedCollectionMode = value;
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
        return this.lowUserScope;
      }
    }

    protected override Row HighScope
    {
      get
      {
        return this.highUserScope;
      }
    }

    internal override CrossConversion Conversion
    {
      get
      {
        return this.WrapperDatabase.Conversion;
      }
    }

    protected void SavePrimaryKey(Row key)
    {
      if (this.currentPrimaryKey == null)
        return;
      this.currentPrimaryKey.Copy(key);
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
      this.tree = this.OnCreateTreeInstance();
    }

    private void CreateIndex()
    {
      try
      {
        this.OnCreateIndex();
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
        this.OnActivateIndex();
      }
      catch (Exception ex)
      {
        if (this.WrapperDatabase == null)
          throw new VistaDBException(ex, 135, this.Name);
        throw new VistaDBException(ex, 134, this.Alias);
      }
    }

    private bool DeactivateIndex()
    {
      try
      {
        return this.OnDeactivateIndex();
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 136);
      }
    }

    internal void ReactivateIndex()
    {
      if (this.tree == null)
        return;
      try
      {
        this.OnReactivateIndex();
      }
      catch (Exception ex)
      {
        if (this.tree != null)
          this.tree.Clear();
        if (this.WrapperDatabase == null)
          throw new VistaDBException(ex, 135, this.Name);
        throw new VistaDBException(ex, 134, this.Alias);
      }
    }

    internal virtual int DoSplitPolicy(int oldCount)
    {
      return this.SplitPolicy_3_4(oldCount);
    }

    protected virtual void OnCreateIndex()
    {
      this.CreateTreeInstance();
      this.tree.CreateRoot();
    }

    protected virtual void OnActivateIndex()
    {
      this.LockStorage();
      try
      {
        this.CreateTreeInstance();
        this.tree.ActivateRoot(this.Header.RootPosition);
      }
      finally
      {
        this.UnlockStorage(true);
      }
    }

    protected virtual bool OnDeactivateIndex()
    {
      this.Handle.ClearWholeCache(this.StorageId, false);
      if (this.tree != null)
        this.tree.Dispose();
      this.tree = (Tree) null;
      return true;
    }

    protected virtual void OnReactivateIndex()
    {
      if (this.bmpFilters != null)
        this.bmpFilters.Clear();
      this.tree.Clear();
      this.Handle.ClearWholeCache(this.StorageId, false);
      this.Header.Update();
      this.ActivateDefaultRow();
      this.tree.ActivateRoot(this.Header.RootPosition);
    }

    protected virtual Tree OnCreateTreeInstance()
    {
      Index index = (Index) null;
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
        if (this.tree != null)
          this.tree.Clear();
      }
    }

    protected override void OnOpenStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      this.activation = true;
      try
      {
        base.OnOpenStorage(openMode, headerPosition);
        this.ActivateIndex();
      }
      finally
      {
        this.activation = false;
      }
    }

    protected override void OnCreateStorage(StorageHandle.StorageMode openMode, ulong headerPosition)
    {
      base.OnCreateStorage(openMode, headerPosition);
      this.CreateIndex();
    }

    protected override void OnCloseStorage()
    {
      this.DeactivateIndex();
      base.OnCloseStorage();
    }

    protected override StorageHandle OnAttachLockStorage(ulong headerPosition)
    {
      StorageHandle storageHandle = base.OnAttachLockStorage(headerPosition);
      if (this.NoLocks || this.WrapperDatabase == null || this.VirtualLocks)
        return storageHandle;
      int num = 5;
      while (true)
      {
        try
        {
          return this.ParentConnection.StorageManager.OpenOrCreateTemporaryStorage(this.WrapperDatabase.Name + (object) '$' + headerPosition.ToString() + ".vdb4lck", true, this.PageSize, (this.WrapperDatabase.Handle.IsolatedStorage ? 1 : 0) != 0, (this.ParentConnection.PersistentLockFiles ? 1 : 0) != 0);
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
      this.Handle.Lock(this.Header.Position, 1, 0UL);
    }

    protected override void OnLowLevelUnlockStorage(ulong offset, int bytes)
    {
      if (this.Handle == null)
        return;
      this.Handle.Unlock(this.Header.Position, 1, 0UL);
    }

    protected override bool OnLockStorage()
    {
      if (base.OnLockStorage())
        return !this.activation;
      return false;
    }

    protected override StorageHeader DoCreateHeaderInstance(int pageSize, CultureInfo culture, DataStorage clonedStorage)
    {
      return clonedStorage.Header;
    }

    protected override void OnActivateHeader(ulong position)
    {
      base.OnActivateHeader(position);
      this.dynamicDescend = this.Header.Descend;
    }

    protected override void OnUpdateStorageVersion(ref bool newVersion)
    {
      if (this.activation)
        return;
      base.OnUpdateStorageVersion(ref newVersion);
      newVersion = newVersion || this.tree != null && this.tree.IsCacheEmpty;
      if (!newVersion)
        return;
      this.ReactivateIndex();
    }

    protected override void OnFlushStorageVersion()
    {
      this.Header.RootPosition = this.tree.RootNode.Id;
      this.tree.FlushTree();
      base.OnFlushStorageVersion();
    }

    protected override void OnAllocateRows()
    {
      base.OnAllocateRows();
      this.lowUserScope = this.TopRow.CopyInstance();
      this.highUserScope = this.BottomRow.CopyInstance();
      this.lowQueryScope = this.TopRow.CopyInstance();
      this.highQueryScope = this.BottomRow.CopyInstance();
      this.currentScope = DataStorage.ScopeType.None;
      if (!this.IsPrimary)
        return;
      this.currentPrimaryKey = this.DoAllocateCachedPkInstance();
    }

    protected virtual Row DoAllocateCachedPkInstance()
    {
      return this.CurrentRow.CopyInstance();
    }

    protected override void OnGoCurrentRow(bool soft)
    {
      if (this.forcedTreeSeek || (int) this.CurrentRow.RowId != (int) this.tree.CurrentKey.RowId || ((int) this.CurrentRow.RowVersion != (int) this.tree.CurrentKey.RowVersion || (int) this.CurrentRow.RowId == (int) Row.MinRowId) || !this.tree.CurrentNode.IsLeaf)
      {
        switch (this.tree.GoKey(this.CurrentRow, (Node) null).KeyRank)
        {
          case Node.KeyPosition.OnLeft:
            if (!soft)
            {
              this.tree.GoPrevKey();
              break;
            }
            break;
          case Node.KeyPosition.OnRight:
            this.tree.GoNextKey();
            break;
        }
      }
      base.OnGoCurrentRow(soft);
    }

    protected override void OnNextRow()
    {
      this.tree.GoNextKey();
    }

    protected override void OnPrevRow()
    {
      this.tree.GoPrevKey();
    }

    protected override void OnSynch(int asynchCounter)
    {
      this.LockStorage();
      try
      {
        base.OnSynch(asynchCounter);
      }
      finally
      {
        this.UnlockStorage(true);
      }
    }

    private void SynchronizeTreePosition()
    {
      if (this.Tree.CurrentNode.SuspectForCorruption)
        throw new VistaDBException(133, "Index appears to be corrupted and cannot be used.");
      this.CurrentRow.Copy(this.tree.CurrentKey);
      this.BgnOfSet = this.CurrentRow - this.TopRow <= 0;
      this.EndOfSet = this.CurrentRow - this.BottomRow >= 0;
    }

    protected override void OnUpdateCurrentRow()
    {
      this.SynchronizeTreePosition();
    }

    protected override bool OnMinimizeMemoryCache(bool forceClearing)
    {
      if (this.tree == null)
        return false;
      return this.tree.MinimizeTreeMemory(forceClearing);
    }

    protected override bool OnFlushCurrentRow()
    {
      if ((int) this.SatelliteRow.RowId != (int) Row.MaxRowId)
        return this.tree.ReplaceKey(this.CurrentRow, this.SatelliteRow, this.TransactionId);
      return this.tree.DeleteKey(this.CurrentRow, this.TransactionId);
    }

    protected override bool OnCreateRow(bool blank, Row newKey)
    {
      if (!base.OnCreateRow(blank, newKey))
        return false;
      this.CurrentRow.RowId = 0U;
      if (this.IsFts && (long) this.tree.TestEqualKeyData(newKey) == (long) newKey.RowId)
        return false;
      if (!this.IsUnique || (long) this.tree.TestEqualKeyData(newKey) == (long) Row.MaxRowId)
        return true;
      if (this.IsSparse || this.SuppressErrors)
        return false;
      throw new VistaDBException(309, this.Alias + ": " + newKey.ToString());
    }

    protected override bool OnUpdateRow(Row oldKey, Row newKey)
    {
      bool flag1 = false;
      if (!base.OnUpdateRow(oldKey, newKey))
        return false;
      bool isPrimary = this.IsPrimary;
      bool isForeignKey = this.IsForeignKey;
      bool flag2 = oldKey.EqualColumns(newKey, this.IsClustered);
      this.noRI_KeyUpdate = (isForeignKey || isPrimary) && flag2;
      if (!flag2 && this.IsUnique)
      {
        ulong num = this.tree.TestEqualKeyData(newKey);
        flag1 = (long) num != (long) Row.MaxRowId && (long) num != (long) oldKey.RowId;
        if (flag1)
        {
          if (this.SuppressErrors || this.IsSparse)
            return false;
          throw new VistaDBException(309, this.Alias + ": " + newKey.ToString());
        }
      }
      if (flag1)
        newKey.RowId = Row.MaxRowId;
      return true;
    }

    protected override bool OnDeleteRow(Row currentRow)
    {
      this.SatelliteRow.RowId = Row.MaxRowId;
      return true;
    }

    protected override bool DoAfterCreateRow(bool created)
    {
      if (created)
      {
        this.GoCurrentRow(false);
        this.SynchronizeTreePosition();
      }
      return created;
    }

    protected override bool DoAfterUpdateRow(uint rowId, bool updated)
    {
      if (updated)
      {
        this.GoCurrentRow(false);
        this.SynchronizeTreePosition();
      }
      return updated;
    }

    protected override bool DoAfterDeleteRow(uint rowId, bool deleted)
    {
      if (deleted)
      {
        this.GoCurrentRow(true);
        this.SynchronizeTreePosition();
        while (!this.PassOrdinaryFilters())
        {
          this.tree.GoNextKey();
          this.SynchronizeTreePosition();
        }
      }
      return deleted;
    }

    protected override void OnSetCurrentRow(Row key)
    {
      this.CurrentRow.Copy(key);
      base.OnSetCurrentRow(key);
    }

    protected override void OnSetSatelliteRow(Row key)
    {
      this.SatelliteRow.Copy(key);
    }

    protected override bool OnIsClearScope()
    {
      return this.currentScope != DataStorage.ScopeType.UserScope;
    }

    protected override void OnClearScope(DataStorage.ScopeType scope)
    {
      switch (scope)
      {
        case DataStorage.ScopeType.QueryScope:
          if (this.currentScope == DataStorage.ScopeType.UserScope)
          {
            this.SetScope((Row) null, (Row) null, DataStorage.ScopeType.UserScope, true);
            return;
          }
          break;
        case DataStorage.ScopeType.UserScope:
          if (this.currentScope == DataStorage.ScopeType.QueryScope)
          {
            this.SetScope((Row) null, (Row) null, DataStorage.ScopeType.QueryScope, true);
            return;
          }
          break;
      }
      this.currentScope = DataStorage.ScopeType.None;
      this.BottomRow.InitBottom();
      this.TopRow.InitTop();
    }

    protected override bool OnSetScope(Row lowValue, Row highValue, DataStorage.ScopeType scope, bool exactMatching)
    {
      switch (scope)
      {
        case DataStorage.ScopeType.QueryScope:
          if (lowValue == null && highValue == null)
          {
            if (this.currentScope != DataStorage.ScopeType.QueryScope)
              return true;
            lowValue = this.lowQueryScope;
            highValue = this.highQueryScope;
            break;
          }
          this.lowQueryScope.Copy(lowValue);
          this.highQueryScope.Copy(highValue);
          this.currentScope = DataStorage.ScopeType.QueryScope;
          break;
        case DataStorage.ScopeType.UserScope:
          if (lowValue == null && highValue == null)
          {
            if (this.currentScope != DataStorage.ScopeType.UserScope)
              return true;
            lowValue = this.lowUserScope;
            highValue = this.highUserScope;
            break;
          }
          this.lowUserScope.Copy(lowValue);
          this.highUserScope.Copy(highValue);
          this.currentScope = DataStorage.ScopeType.UserScope;
          break;
      }
      this.TopRow.InitTop();
      this.BottomRow.InitBottom();
      if (highValue != null)
      {
        switch (this.tree.GoKey(highValue, (Node) null).KeyRank)
        {
          case Node.KeyPosition.Less:
          case Node.KeyPosition.OnLeft:
            this.BottomRow.Copy(this.tree.CurrentKey);
            break;
          default:
            this.tree.GoNextKey();
            goto case Node.KeyPosition.Less;
        }
      }
      if (lowValue != null)
      {
        if (this.tree.GoKey(lowValue, (Node) null).KeyRank != Node.KeyPosition.OnRight)
          this.tree.GoPrevKey();
        this.TopRow.Copy(this.tree.CurrentKey);
      }
      this.SetCurrentRow(this.TopRow);
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
      this.FreezeRelationships();
      try
      {
        return this.CreateRow(false, false) || this.IsUnique && this.IsSparse;
      }
      finally
      {
        this.DefreezeRelationships();
      }
    }

    protected override bool DoUpdateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row oldKey, Row newKey)
    {
      this.FreezeRelationships();
      try
      {
        return this.UpdateRow(false);
      }
      finally
      {
        this.DefreezeRelationships();
      }
    }

    protected override bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
    {
      this.FreezeRelationships();
      try
      {
        return this.DeleteRow(false);
      }
      finally
      {
        this.DefreezeRelationships();
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
      return this.Tree.CurrentKey.EqualColumns(row, this.IsClustered);
    }

    protected override ulong OnGetFreeCluster(int pageCount)
    {
      if (this.WrapperDatabase == null)
        return base.OnGetFreeCluster(pageCount);
      this.WrapperDatabase.LockSpaceMap();
      try
      {
        return base.OnGetFreeCluster(pageCount);
      }
      finally
      {
        this.WrapperDatabase.UnlockSpaceMap();
      }
    }

    protected override void OnSetFreeCluster(ulong clusterId, int pageCount)
    {
      if (this.WrapperDatabase != null)
      {
        this.WrapperDatabase.LockSpaceMap();
        try
        {
          base.OnSetFreeCluster(clusterId, pageCount);
        }
        finally
        {
          this.WrapperDatabase.UnlockSpaceMap();
        }
      }
      else
        base.OnSetFreeCluster(clusterId, pageCount);
    }

    internal void Flip()
    {
      this.dynamicDescend = !this.dynamicDescend;
    }

    protected bool PartialKeyFound(Row patternRow)
    {
      return this.OnPartialKeyFound(patternRow);
    }

    internal override TpStatus DoGettingAnotherTransactionStatus(uint transactionId)
    {
      return TpStatus.Commit;
    }

    internal void ClearCachedBitmaps()
    {
      if (this.bmpFilters == null)
        return;
      this.bmpFilters.Clear();
    }

    private void MarkOptimizedStatus(RowIdFilter filter, Row lowScope, Row highScope, bool markNulls, bool forcedTrueStatus)
    {
      this.FreezeRelationships();
      bool forcedTreeSeek = this.forcedTreeSeek;
      try
      {
        this.TopRow.Copy(lowScope);
        this.BottomRow.Copy(highScope);
        this.forcedTreeSeek = true;
        this.MoveToRow(lowScope);
        while (!this.EndOfSet)
        {
          if (markNulls || !this.CurrentRow.HasNulls)
            filter.SetValidStatus(this.CurrentRow, forcedTrueStatus || !markNulls);
          this.NextRow();
        }
      }
      finally
      {
        this.forcedTreeSeek = forcedTreeSeek;
        this.DefreezeRelationships();
        this.TopRow.InitTop();
        this.BottomRow.InitBottom();
      }
    }

    internal IOptimizedFilter BuildFiltermap(Row lowConstant, Row highConstant, bool excludeNulls)
    {
      if (this.bmpFilters == null)
        this.bmpFilters = new RowIdFilterCollection();
      RowIdFilter filter = this.bmpFilters.GetFilter(lowConstant, highConstant, excludeNulls);
      if (filter != null)
        return (IOptimizedFilter) filter;
      Row lowConstant1 = lowConstant.CopyInstance();
      Row highConstant1 = highConstant.CopyInstance();
      try
      {
        filter = this.ParentRowset.NewOptimizedFilter;
        if (excludeNulls)
        {
          this.MarkOptimizedStatus(filter, lowConstant, highConstant, false, false);
          if (this.IsFts || !excludeNulls)
            return (IOptimizedFilter) filter;
          bool flag = false;
          foreach (Row.Column column in (List<Row.Column>) this.CurrentRow)
          {
            if (column.AllowNull)
            {
              flag = true;
              break;
            }
          }
          if (!flag)
            return (IOptimizedFilter) filter;
        }
        lowConstant = this.TopRow;
        highConstant = this.BottomRow;
        lowConstant.RowId = Row.MinRowId;
        highConstant.RowId = Row.MaxRowId;
        for (int index = 0; index < 1; ++index)
        {
          lowConstant[index].Value = (object) null;
          highConstant[index].Value = (object) null;
        }
        this.MarkOptimizedStatus(filter, lowConstant, highConstant, true, !excludeNulls);
        return (IOptimizedFilter) filter;
      }
      finally
      {
        this.TopRow.InitTop();
        this.BottomRow.InitBottom();
        this.bmpFilters.PutFilter(filter, lowConstant1, highConstant1, excludeNulls);
      }
    }

    internal long GetScopeKeyCount()
    {
      this.FreezeRelationships();
      try
      {
        int num = 0;
        this.MoveToRow(this.TopRow);
        if (this.BgnOfSet)
          this.NextRow();
        while (!this.EndOfSet)
        {
          ++num;
          this.NextRow();
        }
        return (long) num;
      }
      finally
      {
        this.DefreezeRelationships();
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
      internal static Index.RowsetHeader CreateInstance(DataStorage parentStorage, int pageSize, CultureInfo culture)
      {
        return new Index.RowsetHeader(parentStorage, VistaDB.Engine.Core.Header.HeaderId.ROWSET_HEADER, 0, pageSize, culture);
      }

      protected RowsetHeader(DataStorage parentStorage, VistaDB.Engine.Core.Header.HeaderId id, int signature, int pageSize, CultureInfo culture)
        : base(parentStorage, id, Row.EmptyReference, signature, pageSize, culture)
      {
      }
    }

    internal class IndexHeader : StorageHeader
    {
      private int rootEntry;
      private int descendEntry;

      protected IndexHeader(DataStorage parentIndex, VistaDB.Engine.Core.Header.HeaderId id, Index.Type type, int pageSize, CultureInfo culture)
        : base(parentIndex, id, Row.EmptyReference, (int) type, pageSize, culture)
      {
        this.rootEntry = this.AppendColumn((IColumn) new BigIntColumn((long) Row.EmptyReference));
        this.descendEntry = this.AppendColumn((IColumn) new BitColumn(false));
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
          return (ulong) (long) this[this.rootEntry].Value;
        }
        set
        {
          this.Modified = (long) this.RootPosition != (long) value;
          this[this.rootEntry].Value = (object) (long) value;
        }
      }

      internal bool Descend
      {
        get
        {
          return (bool) this[this.descendEntry].Value;
        }
        set
        {
          this[this.descendEntry].Value = (object) value;
        }
      }

      internal bool Unique
      {
        get
        {
          return Index.IndexHeader.IsUnique(this.Signature);
        }
      }

      internal bool Primary
      {
        get
        {
          return Index.IndexHeader.IsPrimary(this.Signature);
        }
      }

      internal bool ForeignKey
      {
        get
        {
          return Index.IndexHeader.IsForeignKey(this.Signature);
        }
      }

      internal bool Fts
      {
        get
        {
          return Index.IndexHeader.IsFts(this.Signature);
        }
      }

      internal bool Sparse
      {
        get
        {
          return Index.IndexHeader.IsSparse(this.Signature);
        }
      }

      internal bool CaseSensitive
      {
        get
        {
          return Index.IndexHeader.IsCaseSensitive(this.Signature);
        }
      }

      internal bool Temporary
      {
        get
        {
          return ((int) this.Signature & 256) == 256;
        }
      }

      internal void CreateSchema(IVistaDBTableSchema schema)
      {
        this.OnCreateSchema(schema);
      }

      internal bool ActivateSchema()
      {
        return this.OnActivateSchema();
      }

      internal void SetSensitivity(bool caseSensitive)
      {
        if (caseSensitive)
          this.Signature |= 64U;
        else
          this.Signature &= 4294967231U;
      }

      protected virtual void OnCreateSchema(IVistaDBTableSchema schema)
      {
        this.Modified = true;
      }

      protected virtual bool OnActivateSchema()
      {
        return false;
      }

      internal void RegisterSchema(IVistaDBTableSchema schema)
      {
        this.OnRegisterSchema(schema);
      }

      protected virtual void OnRegisterSchema(IVistaDBTableSchema schema)
      {
        this.ParentStorage.WrapperDatabase.RegisterRowsetSchema(this.ParentStorage, schema);
      }
    }
  }
}
