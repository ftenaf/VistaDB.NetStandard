using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class HashGroupTable : TempTable, IEqualityComparer
  {
    private SelectStatement.AddRowMethod addRowMethod;
    private SelectStatement.ResultColumnList resultColumns;
    private SelectStatement.GroupColumnList groupColumns;
    private SelectStatement.AggregateExpressionList aggColumns;
    private SelectStatement.HavingClause havingClause;
    private Hashtable hashRows;

    public HashGroupTable(IDatabase database, SelectStatement.AddRowMethod addRowMethod, SelectStatement.ResultColumnList resultColumns, SelectStatement.GroupColumnList groupColumns, SelectStatement.AggregateExpressionList aggColumns, SelectStatement.HavingClause havingClause)
      : base(database)
    {
      this.addRowMethod = addRowMethod;
      this.resultColumns = resultColumns;
      this.groupColumns = groupColumns;
      this.aggColumns = aggColumns;
      this.havingClause = havingClause;
      this.hashRows = new Hashtable((IEqualityComparer) this);
    }

    public int GetHashCode(object obj)
    {
      IColumn[] columnArray = (IColumn[]) obj;
      int num = 0;
      int index = 0;
      for (int count = this.groupColumns.Count; index < count; ++index)
      {
        IColumn column = columnArray[index];
        if (!column.IsNull)
          num = num << 1 ^ column.TrimmedValue.GetHashCode();
      }
      return num;
    }

    public int Compare(object x, object y)
    {
      IColumn[] columnArray1 = (IColumn[]) x;
      IColumn[] columnArray2 = (IColumn[]) y;
      int index = 0;
      for (int count = this.groupColumns.Count; index < count; ++index)
      {
        int num = columnArray1[index].Compare((IVistaDBColumn) columnArray2[index]);
        if (num != 0)
          return num;
      }
      return 0;
    }

    public bool Equals(object x, object y)
    {
      return this.Compare(x, y) == 0;
    }

    private void CreateAggregateRow(IColumn[] key)
    {
      object[] objArray = new object[this.aggColumns.Count];
      int index1 = 0;
      for (int count = this.aggColumns.Count; index1 < count; ++index1)
      {
        SelectStatement.AggregateExpression aggColumn = this.aggColumns[index1];
        object newVal = aggColumn.Expression == (Signature) null ? (object) null : ((IValue) aggColumn.Expression.Execute()).Value;
        objArray[index1] = (object) null;
        aggColumn.Function.CreateNewGroupAndSerialize(newVal, ref objArray[index1]);
      }
      Signature signature = this.groupColumns[0].Signature;
      int index2 = 0;
      for (int length = key.Length; index2 < length; ++index2)
      {
        IColumn column = signature.CreateColumn(key[index2].Type);
        ((IValue) column).Value = ((IValue) key[index2]).Value;
        key[index2] = column;
      }
      this.hashRows.Add((object) key, (object) objArray);
    }

    private void UpdateAggregateRow(object[] data)
    {
      int index = 0;
      for (int count = this.aggColumns.Count; index < count; ++index)
      {
        SelectStatement.AggregateExpression aggColumn = this.aggColumns[index];
        object newVal = aggColumn.Expression == (Signature) null ? (object) null : ((IValue) aggColumn.Expression.Execute()).Value;
        aggColumn.Function.AddRowToGroupAndSerialize(newVal, ref data[index]);
      }
    }

    private void CreateSchema()
    {
      if (this.addRowMethod != null)
        return;
      int index1 = 0;
      for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; index1 < visibleColumnCount; ++index1)
        this.patternRow.AppendColumn(this.database.CreateEmptyColumn(this.resultColumns[index1].DataType));
      int index2 = 0;
      for (int count = this.groupColumns.Count; index2 < count; ++index2)
        this.patternRow.AppendColumn(this.database.CreateEmptyColumn(this.groupColumns[index2].DataType));
      this.patternRow.InstantiateComparingMask();
    }

    private void InsertRowToResult()
    {
      this.curRow = this.patternRow.CopyInstance();
      int index = 0;
      for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; index < visibleColumnCount; ++index)
        this.curRow[index].Value = ((IValue) this.resultColumns[index].Signature.Execute()).Value;
      this.rows.Add(this.curRow);
    }

    public void AddRowToAggregateStream()
    {
      IColumn[] key = new IColumn[this.groupColumns.Count];
      int index = 0;
      for (int length = key.Length; index < length; ++index)
        key[index] = this.groupColumns[index].Signature.Execute();
      object hashRow = this.hashRows[(object) key];
      if (hashRow == null)
        this.CreateAggregateRow(key);
      else
        this.UpdateAggregateRow((object[]) hashRow);
    }

    public void FinishAggregateStream()
    {
      SourceRow sourceRow = new SourceRow();
      this.CreateSchema();
      try
      {
        int columnIndex = 0;
        for (int count = this.groupColumns.Count; columnIndex < count; ++columnIndex)
        {
          this.groupColumns[columnIndex].Signature.SwitchToTempTable(sourceRow, columnIndex);
          foreach (SelectStatement.ResultColumn resultColumn in (List<SelectStatement.ResultColumn>) this.resultColumns)
          {
            if (resultColumn.Signature.SignatureType != SignatureType.Column)
              resultColumn.Signature.SwitchToTempTable(sourceRow, columnIndex, this.groupColumns[columnIndex]);
          }
        }
        IDictionaryEnumerator enumerator = this.hashRows.GetEnumerator();
        enumerator.Reset();
        while (enumerator.MoveNext())
        {
          DictionaryEntry entry = enumerator.Entry;
          object[] objArray = (object[]) entry.Value;
          sourceRow.Columns = (IColumn[]) entry.Key;
          int index1 = 0;
          for (int count = this.aggColumns.Count; index1 < count; ++index1)
            this.aggColumns[index1].Function.FinishGroup(objArray[index1]);
          if (this.havingClause.Evaluate())
          {
            if (this.addRowMethod == null)
            {
              this.InsertRowToResult();
            }
            else
            {
              int index2 = 0;
              for (int visibleColumnCount = this.resultColumns.VisibleColumnCount; index2 < visibleColumnCount; ++index2)
                this.resultColumns[index2].Signature.Execute();
              int num = this.addRowMethod(DataRowType.ResultColumnList, (object) this.resultColumns, true) ? 1 : 0;
            }
          }
        }
        if (this.addRowMethod != null)
          return;
        this.rowCount = this.rows.Count;
        this.curRowIndex = this.rowCount - 1;
        this.eof = true;
      }
      finally
      {
        int index = 0;
        for (int count = this.groupColumns.Count; index < count; ++index)
          this.groupColumns[index].Signature.SwitchToTable();
      }
    }
  }
}
