using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class StandardIndexEvalStack : EvalStack
  {
    private Row patternKey;
    private List<Row.Column> map;

    internal StandardIndexEvalStack(Connection connection, DataStorage activeStorage)
      : base(connection, activeStorage)
    {
    }

    internal override void Exec(Row contextRow, Row targetResult)
    {
      if (this.patternKey == null)
      {
        base.Exec(contextRow, targetResult);
        this.patternKey = this.EvaluatedRow.CopyInstance();
        this.map = this.EnumColumns();
      }
      else
      {
        this.rowResult = targetResult;
        targetResult.CopyMetaData(contextRow);
        if (targetResult.Count > 0)
        {
          for (int index = 0; index < this.patternKey.Count; ++index)
          {
            Row.Column srcColumn = contextRow[this.map[index].RowIndex];
            Row.Column column = targetResult[index];
            column.CreateFullCopy(srcColumn);
            column.Descending = this.patternKey[index].Descending;
          }
        }
        else
        {
          for (int index = 0; index < this.patternKey.Count; ++index)
          {
            Row.Column column = contextRow[this.map[index].RowIndex].Duplicate(false);
            column.Descending = this.patternKey[index].Descending;
            column.RowIndex = index;
            targetResult.AppendColumn((IColumn) column);
          }
        }
      }
    }
  }
}
