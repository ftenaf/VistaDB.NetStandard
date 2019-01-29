using System.Collections.Generic;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class SystemVariable : Signature
  {
    protected SystemVariable(SQLParser parser)
      : base(parser)
    {
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    protected override bool IsEquals(Signature signature)
    {
      return this.GetType() == signature.GetType();
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
    }

    public override void ClearChanged()
    {
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }

    public override int ColumnCount
    {
      get
      {
        return 0;
      }
    }
  }
}
