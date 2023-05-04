using System;
using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class FTSKeysFilter : Filter
  {
    private PassedFTSRows passedRows;

    internal FTSKeysFilter(uint maxRowId)
      : base(null, FilterType.Optimized, true, false, 1)
    {
      passedRows = new PassedFTSRows(maxRowId);
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      bool flag = !passedRows[row.RowId];
      if (flag)
        SetRowStatus(row, false);
      return flag;
    }

    protected override void OnSetRowStatus(Row row, bool valid)
    {
      passedRows[row.RowId] = false;
    }

    internal void Reset(uint maxRowId)
    {
      passedRows.Init(maxRowId);
    }

    private class PassedFTSRows
    {
      private byte[] statusArray;

      internal PassedFTSRows(uint maxRowId)
      {
        Init(maxRowId);
      }

      internal bool this[uint rowId]
      {
        get
        {
          uint num1 = rowId / 8U;
          byte num2 = (byte) (1U << (int) (rowId % 8U));
          return (byte)(statusArray[num1] & (uint)num2) == num2;
        }
        set
        {
          statusArray[rowId / 8U] |= (byte) (1U << (int) (rowId % 8U));
        }
      }

      internal void Init(uint maxRowId)
      {
        uint num = maxRowId / 8U + 1U;
        if (statusArray != null)
        {
          uint length = (uint) statusArray.Length;
          if (num <= length)
          {
            for (int index = 0; index < num; ++index)
              statusArray[index] = 0;
            return;
          }
        }
        statusArray = new byte[num];
      }
    }
  }
}
