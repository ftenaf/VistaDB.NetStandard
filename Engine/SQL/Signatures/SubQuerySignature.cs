using System;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SubQuerySignature : Signature, IValueList
  {
    private IQueryResult table;
    private SelectStatement statement;
    private IColumn tempValue;

    internal static bool IsSubQuery(string token)
    {
      return string.Compare("SELECT", token, StringComparison.OrdinalIgnoreCase) == 0;
    }

    internal static Signature CreateSignature(SQLParser parser)
    {
      return new SubQuerySignature(parser);
    }

    private SubQuerySignature(SQLParser parser)
      : base(parser)
    {
      Statement parent = parser.Parent;
      statement = new SelectStatement(parser.Parent.Connection, parent, parser, 0L);
      parser.Parent = parent;
      signatureType = SignatureType.Expression;
      dataType = VistaDBType.Unknown;
      optimizable = false;
      table = null;
      tempValue = null;
    }

    public override SignatureType OnPrepare()
    {
      dataType = statement.PrepareQuery();
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        table = statement.ExecuteNonLiveQuery();
        object obj;
        try
        {
          table.FirstRow();
          obj = !table.EndOfTable ? table.GetValue(0, VistaDBType.Unknown) : null;
        }
        finally
        {
          table.Close();
          table = null;
        }
                result.Value = obj;
      }
      return result;
    }

    public override void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      if (statement == null)
        return;
      statement.SwitchToTemporaryTable(sourceRow, columnIndex, resultColumn);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature is SubQuerySignature)
        return statement.IsEquals(((SubQuerySignature) signature).statement);
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      statement.SetChanged();
    }

    public override void ClearChanged()
    {
      statement.ClearChanged();
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public bool IsResultPresent()
    {
      table = statement.ExecuteNonLiveQuery();
      statement.ClearChanged();
      bool flag;
      try
      {
        flag = table.RowCount > 0L;
      }
      finally
      {
        table.Close();
        table = null;
      }
      if (result == null)
        result = CreateColumn(VistaDBType.Bit);
            result.Value = flag;
      return flag;
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      return statement.GetIsChanged();
    }

    public override int ColumnCount
    {
      get
      {
        return 1;
      }
    }

    public bool IsValuePresent(IColumn val)
    {
      return IsValuePresent(val, false, CompareOperation.Equal);
    }

    public bool IsValuePresent(IColumn val, bool all, CompareOperation op)
    {
      bool flag = false;
      if (table == null || GetIsChanged())
      {
        if (table != null)
          table.Close();
        table = statement.ExecuteNonLiveQuery();
        statement.ClearChanged();
      }
      if (result == null)
        result = CreateColumn(dataType);
      if (tempValue == null)
        tempValue = CreateColumn(val.Type);
      table.FirstRow();
      if (table.EndOfTable)
        return false;
      while (!table.EndOfTable)
      {
                result.Value = table.GetValue(0, VistaDBType.Unknown);
        Convert(result, tempValue);
        if (Utils.IsCharacterDataType(val.Type) && !tempValue.IsNull)
                    tempValue.Value = ((string)tempValue.Value).TrimEnd();
        int num = val.Compare(tempValue);
        switch (op)
        {
          case CompareOperation.Equal:
            flag = num == 0;
            break;
          case CompareOperation.NotEqual:
            flag = num != 0;
            break;
          case CompareOperation.Greater:
            flag = num > 0;
            break;
          case CompareOperation.GreaterOrEqual:
            flag = num >= 0;
            break;
          case CompareOperation.Less:
            flag = num < 0;
            break;
          case CompareOperation.LessOrEqual:
            flag = num <= 0;
            break;
        }
        if (all && !flag)
          return false;
        if (!all && flag)
          return true;
        table.NextRow();
      }
      return all;
    }
  }
}
