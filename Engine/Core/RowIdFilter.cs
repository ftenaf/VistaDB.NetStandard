using System;
using VistaDB.Engine.Core.Scripting;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class RowIdFilter : Filter, IOptimizedFilter
  {
    private ActiveRows activeRows;
    private long rowCount;

    private RowIdFilter(RowIdFilter filter)
      : base(null, FilterType.Optimized, true, false, 1)
    {
      activeRows = filter.activeRows.Clone();
      rowCount = filter.rowCount;
    }

    internal RowIdFilter(uint maxRowId)
      : base(null, FilterType.Optimized, true, false, 1)
    {
      activeRows = new ActiveRows(maxRowId);
    }

    protected RowIdFilter()
      : base(null, FilterType.Optimized, true, false, 1)
    {
      activeRows = null;
    }

    protected override bool OnGetValidRowStatus(Row row)
    {
      return activeRows.GetValidStatus(row.RowId);
    }

    protected override void OnSetRowStatus(Row row, bool valid)
    {
      if (valid)
        return;
      SetValidStatus(row, false);
    }

    protected virtual long OnConjunction(IOptimizedFilter filter)
    {
      if (filter.IsConstant())
        return activeRows.ConjunctionConstant(((ConstantRowIdFilter) filter).Constant);
      return activeRows.Conjunction(((RowIdFilter) filter).activeRows);
    }

    protected virtual long OnDisjunction(IOptimizedFilter filter)
    {
      if (filter.IsConstant())
        return activeRows.DisjunctionConstant(((ConstantRowIdFilter) filter).Constant);
      return activeRows.Disjunction(((RowIdFilter) filter).activeRows);
    }

    protected virtual long OnInvert(bool instant)
    {
      rowCount = activeRows.Invert(instant);
      return rowCount;
    }

    public void Conjunction(IOptimizedFilter filter)
    {
      rowCount = OnConjunction(filter);
    }

    public void Disjunction(IOptimizedFilter filter)
    {
      rowCount = OnDisjunction(filter);
    }

    public void Invert(bool instant)
    {
      rowCount = OnInvert(instant);
    }

    public virtual bool IsConstant()
    {
      return false;
    }

    public long RowCount
    {
      get
      {
        return rowCount;
      }
    }

    internal void SetValidStatus(Row CurrentRow, bool setTrueValue)
    {
      if (setTrueValue)
        ++rowCount;
      activeRows.SetValidStatus(CurrentRow.RowId, setTrueValue);
    }

    internal void PrepareAttachment()
    {
      activeRows.PrepareAttachment();
    }

    internal RowIdFilter Clone()
    {
      return new RowIdFilter(this);
    }

    private class ActiveRows
    {
      private static byte[,] andMatrix = TriangularLogicTableFunction.AndLogic();
      private static byte[,] orMatrix = TriangularLogicTableFunction.OrLogic();
      private static byte[] notMatrix = TriangularLogicTableFunction.NotLogic();
      private static byte[] countMatrix = TriangularLogicTableFunction.CountLogic();
      private byte[] statusArray;
      private bool inverted;

      private ActiveRows(ActiveRows activeRows)
      {
        statusArray = new byte[activeRows.statusArray.Length];
        Array.Copy(activeRows.statusArray, statusArray, statusArray.Length);
        inverted = activeRows.inverted;
      }

      internal ActiveRows(uint maxRowId)
      {
        statusArray = new byte[(maxRowId / 4U + 1U)];
        byte num = 170;
        int index1 = 0;
        for (int length = statusArray.Length; index1 < length; ++index1)
          statusArray[index1] = num;
        SetValidStatus(0U, false);
        uint rowId = maxRowId + 1U;
        for (uint index2 = (uint) (statusArray.Length * 4); rowId < index2; ++rowId)
          SetValidStatus(rowId, false);
      }

      internal bool GetValidStatus(uint rowId)
      {
        uint num1 = rowId / 4U;
        byte num2 = (byte) (3U << (int) (2U * (rowId % 4U)));
        if (num1 < statusArray.Length)
          return (byte)(statusArray[num1] & (uint)num2) == num2;
        return false;
      }

      internal void SetValidStatus(uint rowId, bool setTrueValue)
      {
        uint num1 = rowId / 4U;
        byte num2 = (byte) (3U << (int) (2U * (rowId % 4U)));
        if (num1 >= statusArray.Length)
          return;
        if (setTrueValue)
          statusArray[num1] |= num2;
        else
          statusArray[num1] &= BitConverter.GetBytes(~num2)[0];
      }

      internal long Invert(bool instant)
      {
        inverted = !inverted;
        long num1 = 0;
        if (instant && inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num2 = notMatrix[statusArray[index]];
            statusArray[index] = num2;
            num1 += countMatrix[num2];
          }
          inverted = false;
        }
        return num1;
      }

      internal long Disjunction(ActiveRows filter)
      {
        long num1 = 0;
        if (inverted && filter.inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num2 = notMatrix[statusArray[index]];
            byte num3 = notMatrix[filter.statusArray[index]];
            byte num4 = orMatrix[num2, num3];
            statusArray[index] = num4;
            num1 += countMatrix[num4];
          }
        }
        else if (inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num2 = notMatrix[statusArray[index]];
            byte status = filter.statusArray[index];
            byte num3 = orMatrix[num2, status];
            statusArray[index] = num3;
            num1 += countMatrix[num3];
          }
        }
        else if (filter.inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte status = statusArray[index];
            byte num2 = notMatrix[filter.statusArray[index]];
            byte num3 = orMatrix[status, num2];
            statusArray[index] = num3;
            num1 += countMatrix[num3];
          }
        }
        else
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte status1 = statusArray[index];
            byte status2 = filter.statusArray[index];
            byte num2 = orMatrix[status1, status2];
            statusArray[index] = num2;
            num1 += countMatrix[num2];
          }
        }
        inverted = false;
        filter.inverted = false;
        return num1;
      }

      internal long Conjunction(ActiveRows filter)
      {
        long num1 = 0;
        if (inverted && filter.inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num2 = notMatrix[statusArray[index]];
            byte num3 = notMatrix[filter.statusArray[index]];
            byte num4 = andMatrix[num2, num3];
            statusArray[index] = num4;
            num1 += countMatrix[num4];
          }
        }
        else if (inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num2 = notMatrix[statusArray[index]];
            byte status = filter.statusArray[index];
            byte num3 = andMatrix[num2, status];
            statusArray[index] = num3;
            num1 += countMatrix[num3];
          }
        }
        else if (filter.inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte status = statusArray[index];
            byte num2 = notMatrix[filter.statusArray[index]];
            byte num3 = andMatrix[status, num2];
            statusArray[index] = num3;
            num1 += countMatrix[num3];
          }
        }
        else
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte status1 = statusArray[index];
            byte status2 = filter.statusArray[index];
            byte num2 = andMatrix[status1, status2];
            statusArray[index] = num2;
            num1 += countMatrix[num2];
          }
        }
        inverted = false;
        filter.inverted = false;
        return num1;
      }

      internal long DisjunctionConstant(Triangular.Value triangular)
      {
        long num1 = 0;
        byte num2 = (byte) (triangular | (Triangular.Value) ((int) triangular << 2) | (Triangular.Value) ((int) triangular << 4) | (Triangular.Value) ((int) triangular << 6));
        if (inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num3 = notMatrix[statusArray[index]];
            byte num4 = orMatrix[num3, num2];
            statusArray[index] = num4;
            num1 += countMatrix[num4];
          }
        }
        else
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte status = statusArray[index];
            byte num3 = orMatrix[status, num2];
            statusArray[index] = num3;
            num1 += countMatrix[num3];
          }
        }
        inverted = false;
        return num1;
      }

      internal long ConjunctionConstant(Triangular.Value triangular)
      {
        long num1 = 0;
        byte num2 = (byte) (triangular | (Triangular.Value) ((int) triangular << 2) | (Triangular.Value) ((int) triangular << 4) | (Triangular.Value) ((int) triangular << 6));
        if (inverted)
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte num3 = notMatrix[statusArray[index]];
            byte num4 = orMatrix[num3, num2];
            statusArray[index] = num4;
            num1 += countMatrix[num4];
          }
        }
        else
        {
          int index = 0;
          for (int length = statusArray.Length; index < length; ++index)
          {
            byte status = statusArray[index];
            byte num3 = andMatrix[status, num2];
            statusArray[index] = num3;
            num1 += countMatrix[num3];
          }
        }
        inverted = false;
        return num1;
      }

      internal void PrepareAttachment()
      {
        if (!inverted)
          return;
        int index = 0;
        for (int length = statusArray.Length; index < length; ++index)
          statusArray[index] = notMatrix[statusArray[index]];
      }

      internal ActiveRows Clone()
      {
        return new ActiveRows(this);
      }

      private class TriangularLogicTableFunction
      {
        private static Triangular.Value[] values = new Triangular.Value[3]{ Triangular.Value.Null, Triangular.Value.True, Triangular.Value.False };

        private static byte[] Indexes
        {
          get
          {
            byte[] numArray = new byte[values.Length * values.Length * values.Length * values.Length];
            int num = 0;
            for (int index1 = 0; index1 < values.Length; ++index1)
            {
              for (int index2 = 0; index2 < values.Length; ++index2)
              {
                for (int index3 = 0; index3 < values.Length; ++index3)
                {
                  for (int index4 = 0; index4 < values.Length; ++index4)
                    numArray[num++] = (byte) (values[index1] | (Triangular.Value) ((int)values[index2] << 2) | (Triangular.Value) ((int)values[index3] << 4) | (Triangular.Value) ((int)values[index4] << 6));
                }
              }
            }
            return numArray;
          }
        }

        private static void GetCompositValues(byte v, out Triangular.Value a, out Triangular.Value b, out Triangular.Value c, out Triangular.Value d)
        {
          a = (Triangular.Value) (v & 3U);
          b = (Triangular.Value) ((byte)((uint)v >> 2) & 3U);
          c = (Triangular.Value) ((byte)((uint)v >> 4) & 3U);
          d = (Triangular.Value) ((byte)((uint)v >> 6) & 3U);
        }

        private static byte NotByte(byte v)
        {
          Triangular.Value a;
          Triangular.Value b;
          Triangular.Value c;
          Triangular.Value d;
                    GetCompositValues(v, out a, out b, out c, out d);
          return (byte) (Triangular.Not(a) | (Triangular.Value) ((int) Triangular.Not(b) << 2) | (Triangular.Value) ((int) Triangular.Not(c) << 4) | (Triangular.Value) ((int) Triangular.Not(d) << 6));
        }

        private static byte AndByte(byte v1, byte v2)
        {
          Triangular.Value a1;
          Triangular.Value b1;
          Triangular.Value c1;
          Triangular.Value d1;
                    GetCompositValues(v1, out a1, out b1, out c1, out d1);
          Triangular.Value a2;
          Triangular.Value b2;
          Triangular.Value c2;
          Triangular.Value d2;
                    GetCompositValues(v2, out a2, out b2, out c2, out d2);
          return (byte) (Triangular.And(a1, a2) | (Triangular.Value) ((int) Triangular.And(b1, b2) << 2) | (Triangular.Value) ((int) Triangular.And(c1, c2) << 4) | (Triangular.Value) ((int) Triangular.And(d1, d2) << 6));
        }

        private static byte OrByte(byte v1, byte v2)
        {
          Triangular.Value a1;
          Triangular.Value b1;
          Triangular.Value c1;
          Triangular.Value d1;
                    GetCompositValues(v1, out a1, out b1, out c1, out d1);
          Triangular.Value a2;
          Triangular.Value b2;
          Triangular.Value c2;
          Triangular.Value d2;
                    GetCompositValues(v2, out a2, out b2, out c2, out d2);
          return (byte) (Triangular.Or(a1, a2) | (Triangular.Value) ((int) Triangular.Or(b1, b2) << 2) | (Triangular.Value) ((int) Triangular.Or(c1, c2) << 4) | (Triangular.Value) ((int) Triangular.Or(d1, d2) << 6));
        }

        private static byte CountByte(byte v)
        {
          byte num1 = 0;
          byte num2 = 3;
          for (int index = 0; index < 4; ++index)
          {
            if ((byte)(v & (uint)num2) == num2)
              ++num1;
            v >>= 2;
          }
          return num1;
        }

        internal static byte[] NotLogic()
        {
          byte[] numArray = new byte[256];
          foreach (byte index in Indexes)
            numArray[index] = NotByte(index);
          return numArray;
        }

        internal static byte[,] AndLogic()
        {
          byte[,] numArray = new byte[256, 256];
          byte[] indexes = Indexes;
          for (int index1 = 0; index1 < indexes.Length; ++index1)
          {
            for (int index2 = 0; index2 < indexes.Length; ++index2)
            {
              byte v1 = indexes[index1];
              byte v2 = indexes[index2];
              numArray[v1, v2] = AndByte(v1, v2);
            }
          }
          return numArray;
        }

        internal static byte[,] OrLogic()
        {
          byte[,] numArray = new byte[256, 256];
          byte[] indexes = Indexes;
          for (int index1 = 0; index1 < indexes.Length; ++index1)
          {
            for (int index2 = 0; index2 < indexes.Length; ++index2)
            {
              byte v1 = indexes[index1];
              byte v2 = indexes[index2];
              numArray[v1, v2] = OrByte(v1, v2);
            }
          }
          return numArray;
        }

        internal static byte[] CountLogic()
        {
          byte[] numArray = new byte[256];
          foreach (byte index in Indexes)
            numArray[index] = CountByte(index);
          return numArray;
        }
      }
    }
  }
}
