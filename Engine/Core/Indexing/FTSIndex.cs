using System;
using System.Collections.Generic;
using System.IO;
using VistaDB.DDA;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Indexing
{
  internal class FTSIndex : RowsetIndex
  {
    internal static FTSIndex.WordAnalyzer WordBreaker = new FTSIndex.WordAnalyzer();
    private int tokenIndex = -1;
    private int columnOrderIndex = -1;
    private int occurrenceIndex = -1;
    private FTSIndex.FtsKeys deleteFtsKeys;
    private FTSIndex.FtsKeys insertFtsKeys;
    private EvalStack ftsEvaluator;
    private Row ftsEvaluationRow;
    private FTSKeysFilter ftsFilter;

    internal FTSIndex(string filename, string indexName, string keyExpression, ClusteredRowset rowSet, DirectConnection connection, Database wrapperDatabase, Encryption encryption, RowsetIndex clonedOrigin)
      : base(filename, indexName, keyExpression, rowSet, connection, wrapperDatabase, encryption, clonedOrigin)
    {
    }

    private void ClearFtsKeys()
    {
      this.insertFtsKeys.Clear();
      this.deleteFtsKeys.Clear();
    }

    private bool FlushFtsKeys()
    {
      try
      {
        foreach (Row deleteFtsKey in (List<Row>) this.deleteFtsKeys)
          this.Tree.DeleteKey(deleteFtsKey, this.TransactionId);
        this.ftsEvaluationRow.RowId = Row.MinRowId;
        foreach (Row insertFtsKey in (List<Row>) this.insertFtsKeys)
          this.Tree.ReplaceKey(this.ftsEvaluationRow, insertFtsKey, this.TransactionId);
        return true;
      }
      finally
      {
        this.ClearFtsKeys();
      }
    }

    private bool CreateDeletionFtsKeys(Row evaluatedRow)
    {
      return this.deleteFtsKeys.ParseKeys(evaluatedRow);
    }

    private bool CreateInsertionFtsKeys(Row evaluatedRow)
    {
      return this.insertFtsKeys.ParseKeys(evaluatedRow);
    }

    private bool SynchFtsLists()
    {
      if (this.deleteFtsKeys.Count <= 0)
        return this.insertFtsKeys.Count > 0;
      return true;
    }

    private short[] GetTableColumnsOrder()
    {
      List<Row.Column> columnList = this.KeyPCode.EnumColumns();
      short[] numArray = new short[columnList.Count];
      int index = 0;
      for (int count = columnList.Count; index < count; ++index)
        numArray[index] = (short) columnList[index].RowIndex;
      return numArray;
    }

    protected override bool IsDescendKeyColumn(int index)
    {
      return false;
    }

    protected override void OnCreatePCode()
    {
      base.OnCreatePCode();
    }

    protected override Row DoAllocateCurrentRow()
    {
      Row emptyRowInstance = this.CreateEmptyRowInstance();
      this.columnOrderIndex = emptyRowInstance.AppendColumn((IColumn) new SmallIntColumn());
      this.tokenIndex = emptyRowInstance.AppendColumn((IColumn) new NVarcharColumn((string) null, 8192, this.Culture, !this.CaseSensitive, NCharColumn.DefaultUnicode));
      this.occurrenceIndex = emptyRowInstance.AppendColumn((IColumn) new IntColumn());
      if (this.Encryption != null)
        emptyRowInstance[this.tokenIndex].AssignAttributes((string) null, true, false, true, false);
      return emptyRowInstance;
    }

    protected override void OnEvaluateSpoolKey(bool forceOutput)
    {
      Row currentRow = this.ParentRowset.CurrentRow;
      try
      {
        this.KeyPCode.Exec(currentRow, this.ftsEvaluationRow);
        if (!this.CreateInsertionFtsKeys(this.ftsEvaluationRow))
          return;
        foreach (Row insertFtsKey in (List<Row>) this.insertFtsKeys)
          this.spool.PushKey(insertFtsKey, forceOutput);
      }
      finally
      {
        this.insertFtsKeys.Clear();
      }
    }

    protected override void OnAllocateRows()
    {
      base.OnAllocateRows();
      short[] tableColumnsOrder = this.GetTableColumnsOrder();
      this.deleteFtsKeys = new FTSIndex.FtsKeys(this.CurrentRow.CopyInstance(), tableColumnsOrder);
      this.insertFtsKeys = new FTSIndex.FtsKeys(this.CurrentRow.CopyInstance(), tableColumnsOrder);
      this.ftsEvaluationRow = this.CreateEmptyRowInstance();
    }

    protected override bool OnCreateRow(bool blank, Row newRow)
    {
      return true;
    }

    protected override bool OnUpdateRow(Row oldRow, Row newRow)
    {
      return true;
    }

    protected override bool OnDeleteRow(Row currentRow)
    {
      return true;
    }

    protected override bool OnFlushCurrentRow()
    {
      return this.FlushFtsKeys();
    }

    protected override Row DoEvaluateLink(DataStorage masterStorage, EvalStack linking, Row sourceRow, Row targetRow)
    {
      this.ftsEvaluator = linking;
      Row link = base.DoEvaluateLink(masterStorage, (EvalStack) null, sourceRow, targetRow);
      link?.InitTop();
      return link;
    }

    protected override bool DoActivateLinkFrom(DataStorage externalStorage, Relationships.Relation link, Row linkingRow)
    {
      return true;
    }

    protected override bool DoUpdateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row oldKey, Row newKey)
    {
      try
      {
        this.KeyPCode.Exec(this.ParentRowset.CurrentRow, this.ftsEvaluationRow);
        this.CreateDeletionFtsKeys(this.ftsEvaluationRow);
        this.KeyPCode.Exec(this.ParentRowset.SatelliteRow, this.ftsEvaluationRow);
        this.CreateInsertionFtsKeys(this.ftsEvaluationRow);
        if (!this.SynchFtsLists())
          return true;
        this.FreezeRelationships();
        try
        {
          return this.UpdateRow(true);
        }
        finally
        {
          this.DefreezeRelationships();
        }
      }
      finally
      {
        this.ClearFtsKeys();
      }
    }

    protected override bool DoCreateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row newRow)
    {
      try
      {
        this.KeyPCode.Exec(this.ParentRowset.SatelliteRow, this.ftsEvaluationRow);
        return !this.CreateInsertionFtsKeys(this.ftsEvaluationRow) || base.DoCreateLinkFrom(externalStorage, type, newRow);
      }
      finally
      {
        this.ClearFtsKeys();
      }
    }

    protected override bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
    {
      try
      {
        this.KeyPCode.Exec(this.ParentRowset.CurrentRow, this.ftsEvaluationRow);
        return !this.CreateDeletionFtsKeys(this.ftsEvaluationRow) || base.DoDeleteLinkFrom(externalStorage, type, row);
      }
      finally
      {
        this.ClearFtsKeys();
      }
    }

    protected override bool OnSetScope(Row lowValue, Row highValue, DataStorage.ScopeType scopes, bool exactMatching)
    {
      if (this.ftsFilter == null)
        this.ftsFilter = new FTSKeysFilter(this.ParentRowset.MaxRowId);
      else
        this.ftsFilter.Reset(this.ParentRowset.MaxRowId);
      return base.OnSetScope(lowValue, highValue, scopes, exactMatching);
    }

    protected override void OnGetScope(out IVistaDBRow lowValue, out IVistaDBRow highValue)
    {
      short[] tableColumnsOrder = this.GetTableColumnsOrder();
      Row emptyRowInstance1 = this.CreateEmptyRowInstance();
      Row emptyRowInstance2 = this.CreateEmptyRowInstance();
      int index = 0;
      for (int length = tableColumnsOrder.Length; index < length; ++index)
      {
        emptyRowInstance1.AppendColumn((IColumn) this.ParentRowset.TopRow[(int) tableColumnsOrder[index]].Duplicate(false));
        emptyRowInstance2.AppendColumn((IColumn) this.ParentRowset.BottomRow[(int) tableColumnsOrder[index]].Duplicate(false));
      }
      lowValue = (IVistaDBRow) emptyRowInstance1;
      highValue = (IVistaDBRow) emptyRowInstance2;
    }

    internal override void DoSetFtsActive()
    {
      this.DoSetFtsInactive();
      if (this.ftsFilter == null)
        this.ftsFilter = new FTSKeysFilter(this.ParentRowset.MaxRowId);
      else
        this.ftsFilter.Reset(this.ParentRowset.MaxRowId);
      this.ParentRowset.AttachFilter((Filter) this.ftsFilter);
    }

    internal override void DoSetFtsInactive()
    {
      this.ParentRowset.DetachFilter(Filter.FilterType.Optimized, (Filter) this.ftsFilter);
    }

    protected override void Destroy()
    {
      this.ftsEvaluator = (EvalStack) null;
      if (this.deleteFtsKeys != null)
        this.deleteFtsKeys.Clear();
      this.deleteFtsKeys = (FTSIndex.FtsKeys) null;
      if (this.insertFtsKeys != null)
        this.insertFtsKeys.Clear();
      this.insertFtsKeys = (FTSIndex.FtsKeys) null;
      this.ftsEvaluationRow = (Row) null;
      base.Destroy();
    }

    internal class WordAnalyzer : IWordBreaker
    {
      private static FTSIndex.WordAnalyzer.StopWords stopWords = new FTSIndex.WordAnalyzer.StopWords();

      public bool IsWordBreaker(string s, int position)
      {
        if (position < 0 || position >= s.Length || char.IsPunctuation(s, position))
          return true;
        return char.IsWhiteSpace(s, position);
      }

      public bool IsStopWord(string word)
      {
        return FTSIndex.WordAnalyzer.stopWords.ContainsKey(word);
      }

      internal class StopWords : Dictionary<string, int>
      {
        internal StopWords()
          : base((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase)
        {
          try
          {
            using (StringReader stringReader = new StringReader(SQLResource.StopWords_EN))
            {
              for (string key = stringReader.ReadLine(); key != null; key = stringReader.ReadLine())
                this.Add(key, 0);
            }
          }
          catch (Exception ex)
          {
          }
        }
      }
    }

    private class FtsKeys : List<Row>
    {
      private static int columnOrderIndex = 0;
      private static int dataIndex = 1;
      private static int occurenceIndex = 2;
      private Row patternRowResult;
      private short[] tableColumnsOrders;
      private int maxLen;

      internal FtsKeys(Row patternRowResult, short[] columnOrders)
        : base(100)
      {
        this.patternRowResult = patternRowResult;
        this.patternRowResult.InitTop();
        this.tableColumnsOrders = columnOrders;
        this.maxLen = this.patternRowResult[FTSIndex.FtsKeys.dataIndex].MaxLength;
      }

      private void CreateEntry(Row evaluatedRow, string token, int columnOrder, int occurence)
      {
        Row row = this.patternRowResult.CopyInstance();
        row.CopyMetaData(evaluatedRow);
        row[FTSIndex.FtsKeys.columnOrderIndex].Value = (object) this.tableColumnsOrders[columnOrder];
        row[FTSIndex.FtsKeys.dataIndex].Value = (object) token;
        row[FTSIndex.FtsKeys.occurenceIndex].Value = (object) occurence;
        this.Add(row);
      }

      private string GetToken(string columnValue, ref int position)
      {
        ++position;
        if (position >= columnValue.Length)
          return (string) null;
        while (position < columnValue.Length && FTSIndex.WordBreaker.IsWordBreaker(columnValue, position))
          ++position;
        int startIndex = position;
        while (position < columnValue.Length && !FTSIndex.WordBreaker.IsWordBreaker(columnValue, position))
          ++position;
        if (position == startIndex)
          return (string) null;
        string str = columnValue.Substring(startIndex, position - startIndex).Trim(' ', char.MinValue);
        if (str.Length == 0)
          return (string) null;
        if (str.Length <= this.maxLen)
          return str;
        return str.Substring(0, 1);
      }

      internal bool ParseKeys(Row evaluatedRow)
      {
        foreach (Row.Column column in (List<Row.Column>) evaluatedRow)
        {
          string columnValue = (string) column.Value;
          if (columnValue != null)
          {
            int occurence = -1;
            int position = -1;
            for (string token = this.GetToken(columnValue, ref position); token != null; token = this.GetToken(columnValue, ref position))
            {
              ++occurence;
              if (!FTSIndex.WordBreaker.IsStopWord(token))
                this.CreateEntry(evaluatedRow, token, column.RowIndex, occurence);
            }
          }
        }
        return this.Count > 0;
      }
    }
  }
}
