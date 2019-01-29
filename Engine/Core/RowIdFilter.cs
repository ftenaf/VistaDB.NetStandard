using System;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class RowIdFilter : Filter, IOptimizedFilter
  {
    private RowIdFilter.ActiveRows activeRows;
    private long rowCount;

    private RowIdFilter(RowIdFilter filter)
      : base((EvalStack) null, Filter.FilterType.Optimized, true, false, 1)
    {
      this.activeRows = filter.activeRows.Clone();
      this.rowCount = filter.rowCount;
    }

    internal RowIdFilter(uint maxRowId)
      : base((EvalStack) null, Filter.FilterType.Optimized, true, false, 1)
    {
      this.activeRows = new RowIdFilter.ActiveRows(maxRowId);
    }

    protected RowIdFilter()
      : base((EvalStack) null, Filter.FilterType.Optimized, true, false, 1)
    {
      this.activeRows = (RowIdFilter.ActiveRows) null;
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      return this.activeRows.GetValidStatus(row.RowId);
    }

    protected override void OnSetRowStatus(Row row, bool valid)
    {
      if (valid)
        return;
      this.SetValidStatus(row, false);
    }

    protected virtual long OnConjunction(IOptimizedFilter filter)
    {
      if (filter.IsConstant())
        return this.activeRows.ConjunctionConstant(((ConstantRowIdFilter) filter).Constant);
      return this.activeRows.Conjunction(((RowIdFilter) filter).activeRows);
    }

    protected virtual long OnDisjunction(IOptimizedFilter filter)
    {
      if (filter.IsConstant())
        return this.activeRows.DisjunctionConstant(((ConstantRowIdFilter) filter).Constant);
      return this.activeRows.Disjunction(((RowIdFilter) filter).activeRows);
    }

    protected virtual long OnInvert(bool instant)
    {
      this.rowCount = this.activeRows.Invert(instant);
      return this.rowCount;
    }

    public void Conjunction(IOptimizedFilter filter)
    {
      this.rowCount = this.OnConjunction(filter);
    }

    public void Disjunction(IOptimizedFilter filter)
    {
      this.rowCount = this.OnDisjunction(filter);
    }

    public void Invert(bool instant)
    {
      this.rowCount = this.OnInvert(instant);
    }

    public virtual bool IsConstant()
    {
      return false;
    }

    public long RowCount
    {
      get
      {
        return this.rowCount;
      }
    }

    internal void SetValidStatus(Row CurrentRow, bool setTrueValue)
    {
      if (setTrueValue)
        ++this.rowCount;
      this.activeRows.SetValidStatus(CurrentRow.RowId, setTrueValue);
    }

    internal void PrepareAttachment()
    {
      this.activeRows.PrepareAttachment();
    }

    internal RowIdFilter Clone()
    {
      return new RowIdFilter(this);
    }

    private class ActiveRows
    {
      private static byte[,] andMatrix = RowIdFilter.ActiveRows.TriangularLogicTableFunction.AndLogic();
      private static byte[,] orMatrix = RowIdFilter.ActiveRows.TriangularLogicTableFunction.OrLogic();
      private static byte[] notMatrix = RowIdFilter.ActiveRows.TriangularLogicTableFunction.NotLogic();
      private static byte[] countMatrix = RowIdFilter.ActiveRows.TriangularLogicTableFunction.CountLogic();
      private byte[] statusArray;
      private bool inverted;

      private ActiveRows(RowIdFilter.ActiveRows activeRows)
      {
        this.statusArray = new byte[activeRows.statusArray.Length];
        Array.Copy((Array) activeRows.statusArray, (Array) this.statusArray, this.statusArray.Length);
        this.inverted = activeRows.inverted;
      }

      internal ActiveRows(uint maxRowId)
      {
        this.statusArray = new byte[(maxRowId / 4U + 1U)];
        byte num = 170;
        int index1 = 0;
        for (int length = this.statusArray.Length; index1 < length; ++index1)
          this.statusArray[index1] = num;
        this.SetValidStatus(0U, false);
        uint rowId = maxRowId + 1U;
        for (uint index2 = (uint) (this.statusArray.Length * 4); rowId < index2; ++rowId)
          this.SetValidStatus(rowId, false);
      }

      internal bool GetValidStatus(uint rowId)
      {
        uint num1 = rowId / 4U;
        byte num2 = (byte) (3U << (int) (2U * (rowId % 4U)));
        if ((long) num1 < (long) this.statusArray.Length)
          return (int) (byte) ((uint) this.statusArray[num1] & (uint) num2) == (int) num2;
        return false;
      }

      internal void SetValidStatus(uint rowId, bool setTrueValue)
      {
        uint num1 = rowId / 4U;
        byte num2 = (byte) (3U << (int) (2U * (rowId % 4U)));
        if ((long) num1 >= (long) this.statusArray.Length)
          return;
        if (setTrueValue)
          statusArray[num1] |= num2;
        else
          statusArray[num1] &= BitConverter.GetBytes(~num2)[0];
      }

      internal long Invert(bool instant)
      {
        this.inverted = !this.inverted;
        long num1 = 0;
        if (instant && this.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            this.statusArray[index] = num2;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num2];
          }
          this.inverted = false;
        }
        return num1;
      }

      internal long Disjunction(RowIdFilter.ActiveRows filter)
      {
        long num1 = 0;
        if (this.inverted && filter.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            byte num3 = RowIdFilter.ActiveRows.notMatrix[(int) filter.statusArray[index]];
            byte num4 = RowIdFilter.ActiveRows.orMatrix[(int) num2, (int) num3];
            this.statusArray[index] = num4;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num4];
          }
        }
        else if (this.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            byte status = filter.statusArray[index];
            byte num3 = RowIdFilter.ActiveRows.orMatrix[(int) num2, (int) status];
            this.statusArray[index] = num3;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num3];
          }
        }
        else if (filter.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte status = this.statusArray[index];
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) filter.statusArray[index]];
            byte num3 = RowIdFilter.ActiveRows.orMatrix[(int) status, (int) num2];
            this.statusArray[index] = num3;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num3];
          }
        }
        else
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte status1 = this.statusArray[index];
            byte status2 = filter.statusArray[index];
            byte num2 = RowIdFilter.ActiveRows.orMatrix[(int) status1, (int) status2];
            this.statusArray[index] = num2;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num2];
          }
        }
        this.inverted = false;
        filter.inverted = false;
        return num1;
      }

      internal long Conjunction(RowIdFilter.ActiveRows filter)
      {
        long num1 = 0;
        if (this.inverted && filter.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            byte num3 = RowIdFilter.ActiveRows.notMatrix[(int) filter.statusArray[index]];
            byte num4 = RowIdFilter.ActiveRows.andMatrix[(int) num2, (int) num3];
            this.statusArray[index] = num4;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num4];
          }
        }
        else if (this.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            byte status = filter.statusArray[index];
            byte num3 = RowIdFilter.ActiveRows.andMatrix[(int) num2, (int) status];
            this.statusArray[index] = num3;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num3];
          }
        }
        else if (filter.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte status = this.statusArray[index];
            byte num2 = RowIdFilter.ActiveRows.notMatrix[(int) filter.statusArray[index]];
            byte num3 = RowIdFilter.ActiveRows.andMatrix[(int) status, (int) num2];
            this.statusArray[index] = num3;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num3];
          }
        }
        else
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte status1 = this.statusArray[index];
            byte status2 = filter.statusArray[index];
            byte num2 = RowIdFilter.ActiveRows.andMatrix[(int) status1, (int) status2];
            this.statusArray[index] = num2;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num2];
          }
        }
        this.inverted = false;
        filter.inverted = false;
        return num1;
      }

      internal long DisjunctionConstant(Triangular.Value triangular)
      {
        long num1 = 0;
        byte num2 = (byte) (triangular | (Triangular.Value) ((int) triangular << 2) | (Triangular.Value) ((int) triangular << 4) | (Triangular.Value) ((int) triangular << 6));
        if (this.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num3 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            byte num4 = RowIdFilter.ActiveRows.orMatrix[(int) num3, (int) num2];
            this.statusArray[index] = num4;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num4];
          }
        }
        else
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte status = this.statusArray[index];
            byte num3 = RowIdFilter.ActiveRows.orMatrix[(int) status, (int) num2];
            this.statusArray[index] = num3;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num3];
          }
        }
        this.inverted = false;
        return num1;
      }

      internal long ConjunctionConstant(Triangular.Value triangular)
      {
        long num1 = 0;
        byte num2 = (byte) (triangular | (Triangular.Value) ((int) triangular << 2) | (Triangular.Value) ((int) triangular << 4) | (Triangular.Value) ((int) triangular << 6));
        if (this.inverted)
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte num3 = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
            byte num4 = RowIdFilter.ActiveRows.orMatrix[(int) num3, (int) num2];
            this.statusArray[index] = num4;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num4];
          }
        }
        else
        {
          int index = 0;
          for (int length = this.statusArray.Length; index < length; ++index)
          {
            byte status = this.statusArray[index];
            byte num3 = RowIdFilter.ActiveRows.andMatrix[(int) status, (int) num2];
            this.statusArray[index] = num3;
            num1 += (long) RowIdFilter.ActiveRows.countMatrix[(int) num3];
          }
        }
        this.inverted = false;
        return num1;
      }

      internal void PrepareAttachment()
      {
        if (!this.inverted)
          return;
        int index = 0;
        for (int length = this.statusArray.Length; index < length; ++index)
          this.statusArray[index] = RowIdFilter.ActiveRows.notMatrix[(int) this.statusArray[index]];
      }

      internal RowIdFilter.ActiveRows Clone()
      {
        return new RowIdFilter.ActiveRows(this);
      }

      private class TriangularLogicTableFunction
      {
        private static Triangular.Value[] values = new Triangular.Value[3]{ Triangular.Value.Null, Triangular.Value.True, Triangular.Value.False };

        private static byte[] Indexes
        {
          get
          {
            byte[] numArray = new byte[RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length * RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length * RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length * RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length];
            int num = 0;
            for (int index1 = 0; index1 < RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length; ++index1)
            {
              for (int index2 = 0; index2 < RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length; ++index2)
              {
                for (int index3 = 0; index3 < RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length; ++index3)
                {
                  for (int index4 = 0; index4 < RowIdFilter.ActiveRows.TriangularLogicTableFunction.values.Length; ++index4)
                    numArray[num++] = (byte) (RowIdFilter.ActiveRows.TriangularLogicTableFunction.values[index1] | (Triangular.Value) ((int) RowIdFilter.ActiveRows.TriangularLogicTableFunction.values[index2] << 2) | (Triangular.Value) ((int) RowIdFilter.ActiveRows.TriangularLogicTableFunction.values[index3] << 4) | (Triangular.Value) ((int) RowIdFilter.ActiveRows.TriangularLogicTableFunction.values[index4] << 6));
                }
              }
            }
            return numArray;
          }
        }

        private static void GetCompositValues(byte v, out Triangular.Value a, out Triangular.Value b, out Triangular.Value c, out Triangular.Value d)
        {
          a = (Triangular.Value) ((uint) v & 3U);
          b = (Triangular.Value) ((uint) (byte) ((uint) v >> 2) & 3U);
          c = (Triangular.Value) ((uint) (byte) ((uint) v >> 4) & 3U);
          d = (Triangular.Value) ((uint) (byte) ((uint) v >> 6) & 3U);
        }

        private static byte NotByte(byte v)
        {
          Triangular.Value a;
          Triangular.Value b;
          Triangular.Value c;
          Triangular.Value d;
          RowIdFilter.ActiveRows.TriangularLogicTableFunction.GetCompositValues(v, out a, out b, out c, out d);
          return (byte) (Triangular.Not(a) | (Triangular.Value) ((int) Triangular.Not(b) << 2) | (Triangular.Value) ((int) Triangular.Not(c) << 4) | (Triangular.Value) ((int) Triangular.Not(d) << 6));
        }

        private static byte AndByte(byte v1, byte v2)
        {
          Triangular.Value a1;
          Triangular.Value b1;
          Triangular.Value c1;
          Triangular.Value d1;
          RowIdFilter.ActiveRows.TriangularLogicTableFunction.GetCompositValues(v1, out a1, out b1, out c1, out d1);
          Triangular.Value a2;
          Triangular.Value b2;
          Triangular.Value c2;
          Triangular.Value d2;
          RowIdFilter.ActiveRows.TriangularLogicTableFunction.GetCompositValues(v2, out a2, out b2, out c2, out d2);
          return (byte) (Triangular.And(a1, a2) | (Triangular.Value) ((int) Triangular.And(b1, b2) << 2) | (Triangular.Value) ((int) Triangular.And(c1, c2) << 4) | (Triangular.Value) ((int) Triangular.And(d1, d2) << 6));
        }

        private static byte OrByte(byte v1, byte v2)
        {
          Triangular.Value a1;
          Triangular.Value b1;
          Triangular.Value c1;
          Triangular.Value d1;
          RowIdFilter.ActiveRows.TriangularLogicTableFunction.GetCompositValues(v1, out a1, out b1, out c1, out d1);
          Triangular.Value a2;
          Triangular.Value b2;
          Triangular.Value c2;
          Triangular.Value d2;
          RowIdFilter.ActiveRows.TriangularLogicTableFunction.GetCompositValues(v2, out a2, out b2, out c2, out d2);
          return (byte) (Triangular.Or(a1, a2) | (Triangular.Value) ((int) Triangular.Or(b1, b2) << 2) | (Triangular.Value) ((int) Triangular.Or(c1, c2) << 4) | (Triangular.Value) ((int) Triangular.Or(d1, d2) << 6));
        }

        private static byte CountByte(byte v)
        {
          byte num1 = 0;
          byte num2 = 3;
          for (int index = 0; index < 4; ++index)
          {
            if ((int) (byte) ((uint) v & (uint) num2) == (int) num2)
              ++num1;
            v >>= 2;
          }
          return num1;
        }

        internal static byte[] NotLogic()
        {
          byte[] numArray = new byte[256];
          foreach (byte index in RowIdFilter.ActiveRows.TriangularLogicTableFunction.Indexes)
            numArray[(int) index] = RowIdFilter.ActiveRows.TriangularLogicTableFunction.NotByte(index);
          return numArray;
        }

        internal static byte[,] AndLogic()
        {
          byte[,] numArray = new byte[256, 256];
          byte[] indexes = RowIdFilter.ActiveRows.TriangularLogicTableFunction.Indexes;
          for (int index1 = 0; index1 < indexes.Length; ++index1)
          {
            for (int index2 = 0; index2 < indexes.Length; ++index2)
            {
              byte v1 = indexes[index1];
              byte v2 = indexes[index2];
              numArray[(int) v1, (int) v2] = RowIdFilter.ActiveRows.TriangularLogicTableFunction.AndByte(v1, v2);
            }
          }
          return numArray;
        }

        internal static byte[,] OrLogic()
        {
          byte[,] numArray = new byte[256, 256];
          byte[] indexes = RowIdFilter.ActiveRows.TriangularLogicTableFunction.Indexes;
          for (int index1 = 0; index1 < indexes.Length; ++index1)
          {
            for (int index2 = 0; index2 < indexes.Length; ++index2)
            {
              byte v1 = indexes[index1];
              byte v2 = indexes[index2];
              numArray[(int) v1, (int) v2] = RowIdFilter.ActiveRows.TriangularLogicTableFunction.OrByte(v1, v2);
            }
          }
          return numArray;
        }

        internal static byte[] CountLogic()
        {
          byte[] numArray = new byte[256];
          foreach (byte index in RowIdFilter.ActiveRows.TriangularLogicTableFunction.Indexes)
            numArray[(int) index] = RowIdFilter.ActiveRows.TriangularLogicTableFunction.CountByte(index);
          return numArray;
        }
      }
    }
  }
}
