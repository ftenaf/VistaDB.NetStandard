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
    internal static WordAnalyzer WordBreaker = new WordAnalyzer();
    private int tokenIndex = -1;
    private int columnOrderIndex = -1;
    private int occurrenceIndex = -1;
    private FtsKeys deleteFtsKeys;
    private FtsKeys insertFtsKeys;
    private EvalStack ftsEvaluator;
    private Row ftsEvaluationRow;
    private FTSKeysFilter ftsFilter;

    internal FTSIndex(string filename, string indexName, string keyExpression, ClusteredRowset rowSet, DirectConnection connection, Database wrapperDatabase, Encryption encryption, RowsetIndex clonedOrigin)
      : base(filename, indexName, keyExpression, rowSet, connection, wrapperDatabase, encryption, clonedOrigin)
    {
    }

    private void ClearFtsKeys()
    {
      insertFtsKeys.Clear();
      deleteFtsKeys.Clear();
    }

    private bool FlushFtsKeys()
    {
      try
      {
        foreach (Row deleteFtsKey in (List<Row>) deleteFtsKeys)
          Tree.DeleteKey(deleteFtsKey, TransactionId);
        ftsEvaluationRow.RowId = Row.MinRowId;
        foreach (Row insertFtsKey in (List<Row>) insertFtsKeys)
          Tree.ReplaceKey(ftsEvaluationRow, insertFtsKey, TransactionId);
        return true;
      }
      finally
      {
        ClearFtsKeys();
      }
    }

    private bool CreateDeletionFtsKeys(Row evaluatedRow)
    {
      return deleteFtsKeys.ParseKeys(evaluatedRow);
    }

    private bool CreateInsertionFtsKeys(Row evaluatedRow)
    {
      return insertFtsKeys.ParseKeys(evaluatedRow);
    }

    private bool SynchFtsLists()
    {
      if (deleteFtsKeys.Count <= 0)
        return insertFtsKeys.Count > 0;
      return true;
    }

    private short[] GetTableColumnsOrder()
    {
      List<Row.Column> columnList = KeyPCode.EnumColumns();
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
      Row emptyRowInstance = CreateEmptyRowInstance();
      columnOrderIndex = emptyRowInstance.AppendColumn((IColumn) new SmallIntColumn());
      tokenIndex = emptyRowInstance.AppendColumn((IColumn) new NVarcharColumn((string) null, 8192, Culture, !CaseSensitive, NCharColumn.DefaultUnicode));
      occurrenceIndex = emptyRowInstance.AppendColumn((IColumn) new IntColumn());
      if (Encryption != null)
        emptyRowInstance[tokenIndex].AssignAttributes((string) null, true, false, true, false);
      return emptyRowInstance;
    }

    protected override void OnEvaluateSpoolKey(bool forceOutput)
    {
      Row currentRow = ParentRowset.CurrentRow;
      try
      {
        KeyPCode.Exec(currentRow, ftsEvaluationRow);
        if (!CreateInsertionFtsKeys(ftsEvaluationRow))
          return;
        foreach (Row insertFtsKey in (List<Row>) insertFtsKeys)
          spool.PushKey(insertFtsKey, forceOutput);
      }
      finally
      {
        insertFtsKeys.Clear();
      }
    }

    protected override void OnAllocateRows()
    {
      base.OnAllocateRows();
      short[] tableColumnsOrder = GetTableColumnsOrder();
      deleteFtsKeys = new FtsKeys(CurrentRow.CopyInstance(), tableColumnsOrder);
      insertFtsKeys = new FtsKeys(CurrentRow.CopyInstance(), tableColumnsOrder);
      ftsEvaluationRow = CreateEmptyRowInstance();
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
      return FlushFtsKeys();
    }

    protected override Row DoEvaluateLink(DataStorage masterStorage, EvalStack linking, Row sourceRow, Row targetRow)
    {
      ftsEvaluator = linking;
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
        KeyPCode.Exec(ParentRowset.CurrentRow, ftsEvaluationRow);
        CreateDeletionFtsKeys(ftsEvaluationRow);
        KeyPCode.Exec(ParentRowset.SatelliteRow, ftsEvaluationRow);
        CreateInsertionFtsKeys(ftsEvaluationRow);
        if (!SynchFtsLists())
          return true;
        FreezeRelationships();
        try
        {
          return UpdateRow(true);
        }
        finally
        {
          DefreezeRelationships();
        }
      }
      finally
      {
        ClearFtsKeys();
      }
    }

    protected override bool DoCreateLinkFrom(DataStorage externalStorage, Relationships.Type type, Row newRow)
    {
      try
      {
        KeyPCode.Exec(ParentRowset.SatelliteRow, ftsEvaluationRow);
        return !CreateInsertionFtsKeys(ftsEvaluationRow) || base.DoCreateLinkFrom(externalStorage, type, newRow);
      }
      finally
      {
        ClearFtsKeys();
      }
    }

    protected override bool DoDeleteLinkFrom(DataStorage externalStorage, Relationships.Type type, Row row)
    {
      try
      {
        KeyPCode.Exec(ParentRowset.CurrentRow, ftsEvaluationRow);
        return !CreateDeletionFtsKeys(ftsEvaluationRow) || base.DoDeleteLinkFrom(externalStorage, type, row);
      }
      finally
      {
        ClearFtsKeys();
      }
    }

    protected override bool OnSetScope(Row lowValue, Row highValue, ScopeType scopes, bool exactMatching)
    {
      if (ftsFilter == null)
        ftsFilter = new FTSKeysFilter(ParentRowset.MaxRowId);
      else
        ftsFilter.Reset(ParentRowset.MaxRowId);
      return base.OnSetScope(lowValue, highValue, scopes, exactMatching);
    }

    protected override void OnGetScope(out IVistaDBRow lowValue, out IVistaDBRow highValue)
    {
      short[] tableColumnsOrder = GetTableColumnsOrder();
      Row emptyRowInstance1 = CreateEmptyRowInstance();
      Row emptyRowInstance2 = CreateEmptyRowInstance();
      int index = 0;
      for (int length = tableColumnsOrder.Length; index < length; ++index)
      {
        emptyRowInstance1.AppendColumn((IColumn) ParentRowset.TopRow[(int) tableColumnsOrder[index]].Duplicate(false));
        emptyRowInstance2.AppendColumn((IColumn) ParentRowset.BottomRow[(int) tableColumnsOrder[index]].Duplicate(false));
      }
      lowValue = (IVistaDBRow) emptyRowInstance1;
      highValue = (IVistaDBRow) emptyRowInstance2;
    }

    internal override void DoSetFtsActive()
    {
      DoSetFtsInactive();
      if (ftsFilter == null)
        ftsFilter = new FTSKeysFilter(ParentRowset.MaxRowId);
      else
        ftsFilter.Reset(ParentRowset.MaxRowId);
      ParentRowset.AttachFilter((Filter) ftsFilter);
    }

    internal override void DoSetFtsInactive()
    {
      ParentRowset.DetachFilter(Filter.FilterType.Optimized, (Filter) ftsFilter);
    }

    protected override void Destroy()
    {
      ftsEvaluator = (EvalStack) null;
      if (deleteFtsKeys != null)
        deleteFtsKeys.Clear();
      deleteFtsKeys = (FtsKeys) null;
      if (insertFtsKeys != null)
        insertFtsKeys.Clear();
      insertFtsKeys = (FtsKeys) null;
      ftsEvaluationRow = (Row) null;
      base.Destroy();
    }

    internal class WordAnalyzer : IWordBreaker
    {
      private static StopWords stopWords = new StopWords();

      public bool IsWordBreaker(string s, int position)
      {
        if (position < 0 || position >= s.Length || char.IsPunctuation(s, position))
          return true;
        return char.IsWhiteSpace(s, position);
      }

      public bool IsStopWord(string word)
      {
        return stopWords.ContainsKey(word);
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
                Add(key, 0);
            }
          }
          catch (Exception)
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
        tableColumnsOrders = columnOrders;
        maxLen = this.patternRowResult[dataIndex].MaxLength;
      }

      private void CreateEntry(Row evaluatedRow, string token, int columnOrder, int occurence)
      {
        Row row = patternRowResult.CopyInstance();
        row.CopyMetaData(evaluatedRow);
        row[columnOrderIndex].Value = (object) tableColumnsOrders[columnOrder];
        row[dataIndex].Value = (object) token;
        row[occurenceIndex].Value = (object) occurence;
        Add(row);
      }

      private string GetToken(string columnValue, ref int position)
      {
        ++position;
        if (position >= columnValue.Length)
          return (string) null;
        while (position < columnValue.Length && WordBreaker.IsWordBreaker(columnValue, position))
          ++position;
        int startIndex = position;
        while (position < columnValue.Length && !WordBreaker.IsWordBreaker(columnValue, position))
          ++position;
        if (position == startIndex)
          return (string) null;
        string str = columnValue.Substring(startIndex, position - startIndex).Trim(' ', char.MinValue);
        if (str.Length == 0)
          return (string) null;
        if (str.Length <= maxLen)
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
            for (string token = GetToken(columnValue, ref position); token != null; token = GetToken(columnValue, ref position))
            {
              ++occurence;
              if (!WordBreaker.IsStopWord(token))
                CreateEntry(evaluatedRow, token, column.RowIndex, occurence);
            }
          }
        }
        return Count > 0;
      }
    }
  }
}
