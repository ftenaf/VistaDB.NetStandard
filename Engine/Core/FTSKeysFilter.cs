using System;
using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class FTSKeysFilter : Filter
  {
    private FTSKeysFilter.PassedFTSRows passedRows;

    internal FTSKeysFilter(uint maxRowId)
      : base((EvalStack) null, Filter.FilterType.Optimized, true, false, 1)
    {
      this.passedRows = new FTSKeysFilter.PassedFTSRows(maxRowId);
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      bool flag = !this.passedRows[row.RowId];
      if (flag)
        this.SetRowStatus(row, false);
      return flag;
    }

    protected override void OnSetRowStatus(Row row, bool valid)
    {
      this.passedRows[row.RowId] = false;
    }

    internal void Reset(uint maxRowId)
    {
      this.passedRows.Init(maxRowId);
    }

    private class PassedFTSRows
    {
      private byte[] statusArray;

      internal PassedFTSRows(uint maxRowId)
      {
        this.Init(maxRowId);
      }

      internal bool this[uint rowId]
      {
        get
        {
          uint num1 = rowId / 8U;
          byte num2 = (byte) (1U << (int) (rowId % 8U));
          return (int) (byte) ((uint) this.statusArray[num1] & (uint) num2) == (int) num2;
        }
        set
        {
          this.statusArray[rowId / 8U] |= (byte) (1U << (int) (rowId % 8U));
        }
      }

      internal void Init(uint maxRowId)
      {
        uint num = maxRowId / 8U + 1U;
        if (this.statusArray != null)
        {
          uint length = (uint) this.statusArray.Length;
          if (num <= length)
          {
            for (int index = 0; (long) index < (long) num; ++index)
              this.statusArray[index] = (byte) 0;
            return;
          }
        }
        this.statusArray = new byte[num];
      }
    }
  }
}
