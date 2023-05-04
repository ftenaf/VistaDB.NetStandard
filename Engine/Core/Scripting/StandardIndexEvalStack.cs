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
      if (patternKey == null)
      {
        base.Exec(contextRow, targetResult);
        patternKey = EvaluatedRow.CopyInstance();
        map = EnumColumns();
      }
      else
      {
        rowResult = targetResult;
        targetResult.CopyMetaData(contextRow);
        if (targetResult.Count > 0)
        {
          for (int index = 0; index < patternKey.Count; ++index)
          {
            Row.Column srcColumn = contextRow[map[index].RowIndex];
            Row.Column column = targetResult[index];
            column.CreateFullCopy(srcColumn);
            column.Descending = patternKey[index].Descending;
          }
        }
        else
        {
          for (int index = 0; index < patternKey.Count; ++index)
          {
            Row.Column column = contextRow[map[index].RowIndex].Duplicate(false);
            column.Descending = patternKey[index].Descending;
            column.RowIndex = index;
            targetResult.AppendColumn((IColumn) column);
          }
        }
      }
    }
  }
}
