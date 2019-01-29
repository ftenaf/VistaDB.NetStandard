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
      return (Signature) new SubQuerySignature(parser);
    }

    private SubQuerySignature(SQLParser parser)
      : base(parser)
    {
      Statement parent = parser.Parent;
      this.statement = new SelectStatement(parser.Parent.Connection, parent, parser, 0L);
      parser.Parent = parent;
      this.signatureType = SignatureType.Expression;
      this.dataType = VistaDBType.Unknown;
      this.optimizable = false;
      this.table = (IQueryResult) null;
      this.tempValue = (IColumn) null;
    }

    public override SignatureType OnPrepare()
    {
      this.dataType = this.statement.PrepareQuery();
      return this.signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        this.table = this.statement.ExecuteNonLiveQuery();
        object obj;
        try
        {
          this.table.FirstRow();
          obj = !this.table.EndOfTable ? this.table.GetValue(0, VistaDBType.Unknown) : (object) null;
        }
        finally
        {
          this.table.Close();
          this.table = (IQueryResult) null;
        }
        ((IValue) this.result).Value = obj;
      }
      return this.result;
    }

    public override void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      if (this.statement == null)
        return;
      this.statement.SwitchToTemporaryTable(sourceRow, columnIndex, resultColumn);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature is SubQuerySignature)
        return this.statement.IsEquals(((SubQuerySignature) signature).statement);
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      this.statement.SetChanged();
    }

    public override void ClearChanged()
    {
      this.statement.ClearChanged();
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public bool IsResultPresent()
    {
      this.table = this.statement.ExecuteNonLiveQuery();
      this.statement.ClearChanged();
      bool flag;
      try
      {
        flag = this.table.RowCount > 0L;
      }
      finally
      {
        this.table.Close();
        this.table = (IQueryResult) null;
      }
      if (this.result == null)
        this.result = this.CreateColumn(VistaDBType.Bit);
      ((IValue) this.result).Value = (object) flag;
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
      return this.statement.GetIsChanged();
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
      return this.IsValuePresent(val, false, CompareOperation.Equal);
    }

    public bool IsValuePresent(IColumn val, bool all, CompareOperation op)
    {
      bool flag = false;
      if (this.table == null || this.GetIsChanged())
      {
        if (this.table != null)
          this.table.Close();
        this.table = this.statement.ExecuteNonLiveQuery();
        this.statement.ClearChanged();
      }
      if (this.result == null)
        this.result = this.CreateColumn(this.dataType);
      if (this.tempValue == null)
        this.tempValue = this.CreateColumn(val.Type);
      this.table.FirstRow();
      if (this.table.EndOfTable)
        return false;
      while (!this.table.EndOfTable)
      {
        ((IValue) this.result).Value = this.table.GetValue(0, VistaDBType.Unknown);
        this.Convert((IValue) this.result, (IValue) this.tempValue);
        if (Utils.IsCharacterDataType(val.Type) && !this.tempValue.IsNull)
          ((IValue) this.tempValue).Value = (object) ((string) ((IValue) this.tempValue).Value).TrimEnd();
        int num = val.Compare((IVistaDBColumn) this.tempValue);
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
        this.table.NextRow();
      }
      return all;
    }
  }
}
