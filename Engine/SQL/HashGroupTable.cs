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
            hashRows = new Hashtable((IEqualityComparer)this);
        }

        public int GetHashCode(object obj)
        {
            IColumn[] columnArray = (IColumn[])obj;
            int num = 0;
            int index = 0;
            for (int count = groupColumns.Count; index < count; ++index)
            {
                IColumn column = columnArray[index];
                if (!column.IsNull)
                    num = num << 1 ^ column.TrimmedValue.GetHashCode();
            }
            return num;
        }

        public int Compare(object x, object y)
        {
            IColumn[] columnArray1 = (IColumn[])x;
            IColumn[] columnArray2 = (IColumn[])y;
            int index = 0;
            for (int count = groupColumns.Count; index < count; ++index)
            {
                int num = columnArray1[index].Compare((IVistaDBColumn)columnArray2[index]);
                if (num != 0)
                    return num;
            }
            return 0;
        }

        public new bool Equals(object x, object y)
        {
            return Compare(x, y) == 0;
        }

        private void CreateAggregateRow(IColumn[] key)
        {
            object[] objArray = new object[aggColumns.Count];
            int index1 = 0;
            for (int count = aggColumns.Count; index1 < count; ++index1)
            {
                SelectStatement.AggregateExpression aggColumn = aggColumns[index1];
                object newVal = aggColumn.Expression == (Signature)null ? (object)null : ((IValue)aggColumn.Expression.Execute()).Value;
                objArray[index1] = (object)null;
                aggColumn.Function.CreateNewGroupAndSerialize(newVal, ref objArray[index1]);
            }
            Signature signature = groupColumns[0].Signature;
            int index2 = 0;
            for (int length = key.Length; index2 < length; ++index2)
            {
                IColumn column = signature.CreateColumn(key[index2].Type);
                ((IValue)column).Value = ((IValue)key[index2]).Value;
                key[index2] = column;
            }
            hashRows.Add((object)key, (object)objArray);
        }

        private void UpdateAggregateRow(object[] data)
        {
            int index = 0;
            for (int count = aggColumns.Count; index < count; ++index)
            {
                SelectStatement.AggregateExpression aggColumn = aggColumns[index];
                object newVal = aggColumn.Expression == (Signature)null ? (object)null : ((IValue)aggColumn.Expression.Execute()).Value;
                aggColumn.Function.AddRowToGroupAndSerialize(newVal, ref data[index]);
            }
        }

        private void CreateSchema()
        {
            if (addRowMethod != null)
                return;
            int index1 = 0;
            for (int visibleColumnCount = resultColumns.VisibleColumnCount; index1 < visibleColumnCount; ++index1)
                patternRow.AppendColumn(database.CreateEmptyColumn(resultColumns[index1].DataType));
            int index2 = 0;
            for (int count = groupColumns.Count; index2 < count; ++index2)
                patternRow.AppendColumn(database.CreateEmptyColumn(groupColumns[index2].DataType));
            patternRow.InstantiateComparingMask();
        }

        private void InsertRowToResult()
        {
            curRow = patternRow.CopyInstance();
            int index = 0;
            for (int visibleColumnCount = resultColumns.VisibleColumnCount; index < visibleColumnCount; ++index)
                curRow[index].Value = ((IValue)resultColumns[index].Signature.Execute()).Value;
            rows.Add(curRow);
        }

        public void AddRowToAggregateStream()
        {
            IColumn[] key = new IColumn[groupColumns.Count];
            int index = 0;
            for (int length = key.Length; index < length; ++index)
                key[index] = groupColumns[index].Signature.Execute();
            object hashRow = hashRows[(object)key];
            if (hashRow == null)
                CreateAggregateRow(key);
            else
                UpdateAggregateRow((object[])hashRow);
        }

        public void FinishAggregateStream()
        {
            SourceRow sourceRow = new SourceRow();
            CreateSchema();
            try
            {
                int columnIndex = 0;
                for (int count = groupColumns.Count; columnIndex < count; ++columnIndex)
                {
                    groupColumns[columnIndex].Signature.SwitchToTempTable(sourceRow, columnIndex);
                    foreach (SelectStatement.ResultColumn resultColumn in (List<SelectStatement.ResultColumn>)resultColumns)
                    {
                        if (resultColumn.Signature.SignatureType != SignatureType.Column)
                            resultColumn.Signature.SwitchToTempTable(sourceRow, columnIndex, groupColumns[columnIndex]);
                    }
                }
                IDictionaryEnumerator enumerator = hashRows.GetEnumerator();
                enumerator.Reset();
                while (enumerator.MoveNext())
                {
                    DictionaryEntry entry = enumerator.Entry;
                    object[] objArray = (object[])entry.Value;
                    sourceRow.Columns = (IColumn[])entry.Key;
                    int index1 = 0;
                    for (int count = aggColumns.Count; index1 < count; ++index1)
                        aggColumns[index1].Function.FinishGroup(objArray[index1]);
                    if (havingClause.Evaluate())
                    {
                        if (addRowMethod == null)
                        {
                            InsertRowToResult();
                        }
                        else
                        {
                            int index2 = 0;
                            for (int visibleColumnCount = resultColumns.VisibleColumnCount; index2 < visibleColumnCount; ++index2)
                                resultColumns[index2].Signature.Execute();
                            int num = addRowMethod(DataRowType.ResultColumnList, (object)resultColumns, true) ? 1 : 0;
                        }
                    }
                }
                if (addRowMethod != null)
                    return;
                rowCount = rows.Count;
                curRowIndex = rowCount - 1;
                eof = true;
            }
            finally
            {
                int index = 0;
                for (int count = groupColumns.Count; index < count; ++index)
                    groupColumns[index].Signature.SwitchToTable();
            }
        }
    }
}
